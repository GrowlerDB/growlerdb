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

/// The poller's memory between ticks: the snapshot it last diffed, so an unchanged snapshot is a
/// cheap no-op. The set of files still needing a heal is re-derived each tick from the shards'
/// **persisted** interned/dead bitmap — never from in-memory state — so a poller restart mid-heal
/// still finishes the heal (see [`remap_tick`]).
#[derive(Debug, Default)]
pub struct RemapState {
    last_snapshot: Option<i64>,
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
/// Returns `Ok(None)` when nothing happened (snapshot unchanged, a pure append, or an already-
/// finished heal). `key_fields` is the index's composite key, used to project the replacement
/// files' rows. Multiple `shards` (a windowed index's hot windows) share one plan fetch and key
/// scan; each skips keys it doesn't hold.
///
/// **Restart-safe by design.** The files still needing a heal are re-derived each tick from the
/// shards' *persisted* interned/dead bitmap — the plan's files that no shard already holds a live
/// slot into — not from in-memory prev-tick state. `mark_files_dead` is persisted but the heal that
/// follows is not, so gating the heal on this-tick `disappeared` (interned-live files that just
/// vanished) alone stranded a half-done heal forever: after a restart the dead files are excluded
/// from `interned_live_files()`, so `disappeared` reads empty and the heal never ran again —
/// hydration stayed on the slow pass-2 fallback. Deriving the work from the persisted bitmap lets
/// the next tick (even in a fresh process) finish the heal.
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
    // interns only its own rows' files. These are newly rewritten, so mark them dead this tick.
    let disappeared_per_shard: Vec<Vec<String>> = shards
        .iter()
        .map(|s| {
            s.interned_live_files()
                .into_iter()
                .filter(|f| !current.contains(f))
                .collect()
        })
        .collect();
    let any_disappeared = disappeared_per_shard.iter().any(|d| !d.is_empty());
    // Dead files present ⇒ a rewrite happened at some point whose heal may be unfinished (e.g. this
    // process restarted mid-heal). With no dead files and nothing disappeared this tick it's a pure
    // append / unrelated change — the early-out that keeps steady-state ingest off the scan path.
    let any_dead = shards.iter().any(|s| s.dead_file_count() > 0);
    if !any_disappeared && !any_dead {
        state.last_snapshot = Some(plan.snapshot_id);
        return Ok(None);
    }

    // A shard already interns its live/original files, so those and any already-healed replacement
    // are skipped below; only a plan file no shard has a live slot into is a heal candidate. Union
    // across shards mirrors the "each skips keys it doesn't hold" fan-out of the patch loop.
    let interned_live: HashSet<String> = shards
        .iter()
        .flat_map(|s| s.interned_live_files())
        .collect();

    let mut outcome = RemapOutcome {
        snapshot_id: plan.snapshot_id,
        ..Default::default()
    };
    // Mark the disappeared files dead FIRST (per shard), before any scan: hydration then skips the
    // doomed pass-1 point reads for the heal window, and the dead flag is `remap_locations`' guard.
    for (shard, disappeared) in shards.iter().zip(&disappeared_per_shard) {
        outcome.files_marked_dead += shard.mark_files_dead(disappeared)?;
    }
    // Stream the replacement files one at a time, patching each file's slots immediately rather than
    // accumulating the whole rewritten table into one Vec: a whole-table compaction moves *every*
    // row, so batching was O(table) memory (OOM at scale) and healed nothing until the last file was
    // read. Per-file streaming bounds memory to one file's rows and heals progressively. Patches are
    // idempotent last-wins and dead-file-guarded, so a crash mid-stream just re-heals next tick (an
    // already-re-pointed slot is skipped as `already_live`).
    for task in plan.tasks.iter() {
        if interned_live.contains(&task.data_file_path) {
            continue; // a shard already holds a live slot into this file — nothing to heal here
        }
        if !task.deletes.is_empty() {
            // Delete-bearing files have delete-shifted ingest positions but the key scan reads
            // physical ones — don't write a mismatch; the lazy refresh heals these.
            outcome.files_skipped_deletes += 1;
            continue;
        }
        let moved = scan_added_file(&plan.file_io, &task.data_file_path, key_fields).await?;
        outcome.rows_read += moved.len();
        outcome.files_scanned += 1;
        for shard in shards.iter() {
            let stats = shard.remap_locations(&moved)?;
            outcome.stats.remapped += stats.remapped;
            outcome.stats.skipped_no_live_doc += stats.skipped_no_live_doc;
            outcome.stats.skipped_already_live += stats.skipped_already_live;
        }
    }
    state.last_snapshot = Some(plan.snapshot_id);
    // Nothing marked and nothing scanned ⇒ a snapshot advance with no heal work (e.g. an append
    // after the table has ever been compacted): report it as a no-op, not a re-map event.
    if outcome.files_marked_dead == 0 && outcome.files_scanned == 0 {
        return Ok(None);
    }
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
