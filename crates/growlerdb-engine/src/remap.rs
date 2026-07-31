//! The background **compaction re-map** driver (the `coordinates` strategy): turn locator
//! staleness from a per-read tax into a bounded background cost.
//!
//! Iceberg compaction (`rewrite_data_files`, a `replace` snapshot) moves rows into new data
//! files, so every location slot into a rewritten file goes stale at once. The driver polls
//! the table's current plan (one catalog REST call per tick) and, when interned files
//! **disappear** from the live set:
//!
//! 1. marks them **dead** (hydration then skips the doomed point read for the pass-2 fallback), and
//! 2. **re-maps**: column-projects the key columns + row positions of the plan's *added* files,
//!    bulk-patching each key's location slot with its new `(file, position)`.
//!
//! ## Why every interleaving is safe
//!
//! Slot patches are idempotent last-wins 12-byte writes serialized by the writer lock (held per
//! chunk, released between). The shard-side guard (`Shard::remap_locations`) only patches a slot
//! that **still points at a dead file**, so one ingest or a lazy refresh already re-pointed is
//! never clobbered. Keys with no live doc are skipped; hydration's verify-and-fallback remains
//! the correctness safety net for any residual window — the re-map only changes *where the cost lands*.
//!
//! Files carrying delete files are **not** re-mapped (ingest positions are delete-shifted, the
//! key scan reads physical positions) — their slots heal via the lazy path.

use std::collections::HashSet;
use std::sync::Arc;

use growlerdb_core::{CompositeKey, RowLocator};
use growlerdb_index::{RemapStats, Shard};
use growlerdb_source::{read_file_key_rows, FileIO, IcebergReader};

use crate::error::EngineError;

/// The poller's memory between ticks: the snapshot it last diffed at and that
/// snapshot's live data-file set (so `added` is a plan-to-plan diff, not a guess).
#[derive(Debug, Default)]
pub struct RemapState {
    last_snapshot: Option<i64>,
    prev_files: Option<HashSet<String>>,
}

/// What one re-map pass did — the numbers behind `growlerdb_locator_remap_events_total`
/// / `growlerdb_locator_remapped_rows_total` and the tick's log line.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemapOutcome {
    /// The snapshot the pass diffed against.
    pub snapshot_id: i64,
    /// Interned live files that disappeared from the plan (newly marked dead), summed
    /// across shards.
    pub files_marked_dead: u64,
    /// Added files whose key columns were scanned.
    pub files_scanned: usize,
    /// Added files skipped because they carry delete files (left to the lazy path).
    pub files_skipped_deletes: usize,
    /// Rewritten rows read from the added files' key columns.
    pub rows_read: usize,
    /// Slot-patch stats summed across shards.
    pub stats: RemapStats,
}

/// The per-shard re-map **entry point**: mark `disappeared` files dead (the live-file bitmap),
/// then bulk-patch the slots of `moved` rows (`key → new (file, position)`). Returns `(files
/// newly marked dead, patch stats)`. Split from [`remap_tick`] so a rewrite diff can be fed
/// directly (the regression tests do this).
pub fn remap_shard(
    shard: &Shard,
    disappeared: &[String],
    moved: &[(CompositeKey, RowLocator)],
) -> Result<(u64, RemapStats), EngineError> {
    // Dead flags first: hydration then stops issuing doomed point reads, and the flags are the
    // guard remap_locations checks (only dead-pointing slots are healed).
    let marked = shard.mark_files_dead(disappeared)?;
    let stats = shard.remap_locations(moved)?;
    Ok((marked, stats))
}

/// One poll of the re-map loop: fetch the table's current plan, diff its live data-file set
/// against the shards' interned files and — when interned files disappeared (a rewrite) — mark
/// them dead and re-map from the plan's added files.
///
/// Returns `Ok(None)` when nothing happened (snapshot unchanged, or a pure append). `key_fields`
/// is the index's composite key, used to project the added files' rows. Multiple `shards` (a
/// windowed index's hot windows) share one plan fetch and key scan; each skips keys it doesn't
/// hold. The first tick after boot has no previous plan, so `added` falls back to "plan files
/// not interned by any shard" — a safe superset (foreign keys skipped, already-live slots not patched).
pub async fn remap_tick(
    reader: &IcebergReader,
    table: &str,
    key_fields: (&[String], &[String]),
    shards: &[Arc<Shard>],
    state: &mut RemapState,
) -> Result<Option<RemapOutcome>, EngineError> {
    let plan = reader.current_plan(table).await?;
    if state.last_snapshot == Some(plan.snapshot_id) {
        return Ok(None); // no new snapshot → nothing can have changed
    }
    let current: HashSet<String> = plan
        .tasks
        .iter()
        .map(|t| t.data_file_path.clone())
        .collect();

    // Interned, still-live files that vanished from the live set — per shard, since each shard
    // interns only its own rows' files.
    let disappeared_per_shard: Vec<Vec<String>> = shards
        .iter()
        .map(|s| {
            s.interned_live_files()
                .into_iter()
                .filter(|f| !current.contains(f))
                .collect()
        })
        .collect();
    if disappeared_per_shard.iter().all(Vec::is_empty) {
        // Pure append (or unrelated change). Commit the observation so the next replace diffs
        // against THIS plan.
        state.last_snapshot = Some(plan.snapshot_id);
        state.prev_files = Some(current);
        return Ok(None);
    }

    // The rewrite's *added* files: in the current plan but not the previous (first tick: not
    // interned anywhere — see the doc comment). State commits only at the end, so a failed
    // scan/patch is retried next tick rather than skipping the snapshot.
    let baseline: HashSet<String> = match &state.prev_files {
        Some(files) => files.clone(),
        None => shards
            .iter()
            .flat_map(|s| s.interned_live_files())
            .collect(),
    };
    let mut outcome = RemapOutcome {
        snapshot_id: plan.snapshot_id,
        ..Default::default()
    };
    let mut moved: Vec<(CompositeKey, RowLocator)> = Vec::new();
    for task in plan.tasks.iter() {
        if baseline.contains(&task.data_file_path) {
            continue;
        }
        if !task.deletes.is_empty() {
            // Delete-bearing files have delete-shifted ingest positions but the key scan reads
            // physical ones — don't write a mismatch; the lazy refresh heals these.
            outcome.files_skipped_deletes += 1;
            continue;
        }
        moved.extend(scan_added_file(&plan.file_io, &task.data_file_path, key_fields).await?);
        outcome.files_scanned += 1;
    }
    outcome.rows_read = moved.len();

    for (shard, disappeared) in shards.iter().zip(&disappeared_per_shard) {
        let (marked, stats) = remap_shard(shard, disappeared, &moved)?;
        outcome.files_marked_dead += marked;
        outcome.stats.remapped += stats.remapped;
        outcome.stats.skipped_no_live_doc += stats.skipped_no_live_doc;
        outcome.stats.skipped_already_live += stats.skipped_already_live;
    }
    state.last_snapshot = Some(plan.snapshot_id);
    state.prev_files = Some(current);
    Ok(Some(outcome))
}

/// Column-project one added file's key columns → `(key, locator)` rows for the patch.
async fn scan_added_file(
    file_io: &FileIO,
    path: &str,
    (partition_fields, identifier_fields): (&[String], &[String]),
) -> Result<Vec<(CompositeKey, RowLocator)>, EngineError> {
    let rows = read_file_key_rows(file_io, path, partition_fields, identifier_fields).await?;
    Ok(rows
        .into_iter()
        .map(|(key, row_position)| {
            (
                key,
                RowLocator {
                    iceberg_file: path.to_string(),
                    row_position,
                },
            )
        })
        .collect())
}
