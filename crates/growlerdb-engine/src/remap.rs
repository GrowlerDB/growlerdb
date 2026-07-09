//! The background **compaction re-map** driver (task-184 slice 3, [D30] `coordinates`
//! strategy): turn locator staleness from a per-read tax into a bounded background cost.
//!
//! Iceberg compaction (`rewrite_data_files` — a `replace` snapshot) moves rows into new
//! data files, so every location slot pointing into a rewritten file goes stale at once.
//! Before this driver the only healing was the lazy per-key verify-and-refresh at
//! hydration (`growlerdb_stale_locators_total` ≈ 1 per hydrated key under churn). The
//! driver polls the table's **current plan** (via the reader's snapshot-pinned plan
//! cache — one catalog REST call per tick, manifest reads only on snapshot advance;
//! observing table metadata is read-only and imposes nothing on the source) and, when
//! interned files **disappear** from the live file set:
//!
//! 1. marks the disappeared files **dead** (the live-file bitmap — hydration then skips
//!    the doomed point read and goes straight to the pass-2 fallback), and
//! 2. **re-maps**: column-projects only the key columns + row positions of the plan's
//!    *added* files, and bulk-patches each key's location slot with its new
//!    `(file, position)` — batched and key-sorted (term-dictionary locality; spike
//!    ~1M key-sorted lookups/s warm), fsynced per chunk under the shard's
//!    writer-lock contract.
//!
//! ## Why every interleaving is safe
//!
//! Ingest may intern new files and commit upserts, and hydration may lazily refresh
//! slots, while a re-map runs. Slot patches are idempotent last-wins 12-byte writes,
//! serialized by the writer lock (held per chunk, released between chunks — the re-map
//! never blocks ingest or hydration for its full duration), and the shard-side patch
//! guard (`Shard::remap_locations`) only patches a slot that **still points at a dead
//! file** — so a slot that ingest or a lazy refresh already re-pointed at a live file
//! is never clobbered with the (older) rewritten row. Keys with no live doc (deleted,
//! or not yet ingested) are skipped; if ingest lands them later, its commit writes the
//! fresh location anyway. For any residual window, hydration's verify-and-fallback
//! remains the correctness safety net — the re-map only changes *where the cost lands*.
//!
//! Files that carry delete files are **not** re-mapped (ingest records delete-shifted
//! positions for them; the key scan reads physical positions) — their slots heal via
//! the lazy path. Freshly-compacted files are delete-free, so this is the rare case.
//!
//! [D30]: ../../../okf/system/decisions/d30-layered-locator.md

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

/// The per-shard re-map **entry point**: mark `disappeared` files dead (the live-file
/// bitmap), then bulk-patch the slots of `moved` rows (`key → new (file, position)`,
/// read from the replace snapshot's added files). Returns `(files newly marked dead,
/// patch stats)`. Split out from [`remap_tick`] so a rewrite can be fed to it directly
/// — the regression tests simulate compaction by rewriting fixture files and calling
/// this with the diff.
pub fn remap_shard(
    shard: &Shard,
    disappeared: &[String],
    moved: &[(CompositeKey, RowLocator)],
) -> Result<(u64, RemapStats), EngineError> {
    // Dead flags first: from this instant hydration stops issuing doomed point reads
    // into the rewritten files, and the flags are the patch guard remap_locations
    // checks (only dead-pointing slots are healed).
    let marked = shard.mark_files_dead(disappeared)?;
    let stats = shard.remap_locations(moved)?;
    Ok((marked, stats))
}

/// One poll of the re-map loop: fetch the table's current plan, diff its live data-file
/// set against the shards' interned live files, and — when interned files disappeared
/// (a `replace`/rewrite) — mark them dead and re-map from the plan's added files.
///
/// Returns `Ok(None)` when nothing happened (snapshot unchanged, or a pure append —
/// files only added, nothing interned disappeared). `key_fields` is the index's
/// composite key (`(partition fields, identifier fields)`), used to project + encode
/// the added files' rows. Multiple `shards` (a windowed index's hot windows) share one
/// plan fetch and one key scan; each shard skips the keys it doesn't hold (no live
/// doc). The first tick after boot has no previous plan, so `added` falls back to
/// "plan files not interned by any shard" — a superset that is safe (foreign keys are
/// skipped; already-live slots aren't patched).
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

    // Which interned, still-live files vanished from the live set — per shard, since
    // each shard interns only the files its own rows came from.
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
        // Pure append (or unrelated change) — the lazy path owes nothing. Commit the
        // observation so the next replace diffs against THIS plan.
        state.last_snapshot = Some(plan.snapshot_id);
        state.prev_files = Some(current);
        return Ok(None);
    }

    // The rewrite's *added* files: in the current plan but not the previous one (first
    // tick: not interned anywhere — see the doc comment). State commits only at the
    // end — a failed scan/patch leaves it untouched, so the next tick retries the
    // same diff instead of skipping the snapshot.
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
            // Ingest positions for delete-bearing files are delete-shifted; the key
            // scan reads physical positions — don't write a mismatch, let the lazy
            // verify-and-refresh heal these.
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
