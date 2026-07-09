---
type: Concept
title: Locators & segments
description: Immutable Tantivy segments plus a layered locator — key terms, a locator-ID fast field, and a dense location array — kept crash-consistent and healed in the background when Iceberg compaction rewrites files.
tags: [system, storage, segments, locators, crash-consistency]
timestamp: 2026-07-04T14:22:00
---

# Locators & segments

- **Segments** are immutable Tantivy index files. New documents go into a new segment; deletes/updates
  are handled per-generation (live-docs / tombstones), and [compaction](/product/functional/index-management/compact.md)
  merges segments to bound their count and reclaim space.
- **Locators** map each document key to its source-row location (`iceberg_file`, `row_position`) so
  hydration, update, and delete can find it. Since [D30](/system/decisions/d30-layered-locator.md)
  the locator is **layered** by mutability rather than stored as one keyed table.

## The three layers (D30)

- **Identity** — key → document: the Tantivy key-term dictionary, already paid for by search.
- **Reference** — document → locator ID: `_locid`, an immutable u64 fast field written at ingest.
  Updates *reuse* the live doc's ID (a pre-commit key-term lookup) so the location array stays
  O(live keys), not O(all versions).
- **Location** — locator ID → location: `location.arr`, a dense array file beside `aux.redb` with
  fixed **12 B** entries (`u32` interned file ID + `u64` row position; the locator ID *is* the slot
  index). File paths are interned once in the `files` table in `aux.redb`. Slots are **patched in
  place** when a row moves (upsert reuse; locator refresh after an Iceberg rewrite); deletes just
  leave a slot unreachable until store compaction.

A locate resolves key term → live doc → `_locid` → array slot → interned path. The array is tiny
(~12 B/row vs ~54 B/row keyed in redb, measured), so a parked cold window keeps it **local** beside
`aux.redb` while the segment bulk is served read-through from object storage. This layered design
is the **only** shard layout; `aux.redb` holds just meta (checkpoint, zone-map, lineage), batch
idempotency, and the interned file table (+ its dead-file set) — there is no per-key table.

## Location strategies (per index)

The reference + location layers are governed by the index definition's **`location_strategy`**
option ([D30](/system/decisions/d30-layered-locator.md); see
[create](/product/functional/index-management/create.md)). Changing it on an existing index is
reindex-only (non-cached field values aren't stored, so the layers can't be rebuilt in place).
Auto-detection from table inspection is deferred until the `row_id` strategy exists; today the
choice is explicit. Key verification and the predicate fallback stay on under every strategy — a
strategy changes cost, never correctness.

- **`COORDINATES`** (default, described above) — per-row location data, fast point reads on any
  table; staleness under source compaction is healed by the re-map below.
- **`PREDICATE`** — **store-less**: the write path stores *no* location data — no `location.arr`
  entries (the file exists, empty; backups carry it at 0 bytes), no file interns, and the `_locid`
  fast field is **kept in the schema but never populated** (one schema shape across strategies —
  no field-ordinal divergence for segments, backup, reindex, or cold-open tooling; an absent u64
  fast value bitpacks to ~nothing). Hydration skips locators entirely: every present key goes to
  the source with no locator, i.e. straight to the partition/stats-**pruned key-equality scan**
  (the same machinery as the `COORDINATES` fallback, promoted to primary). Missing keys are still
  a local `NotFound` before any catalog connect (a key-term presence probe). There is nothing to
  refresh or re-map — the re-map loop is not spawned, `refresh_locators` is a no-op, and a
  predicate hydration never increments `growlerdb_stale_locators_total` (it isn't a refresh).
  Everything else — the live-key set below, reconcile, backup — works unchanged. **Honest
  scope**: effective only where the key correlates with layout (partitioned on key fields, or
  clustered/sorted by the key — including temporal keys, which build real pruning predicates); on
  an unclustered high-cardinality key, stats can't prune and hydration degrades to broad scans.
  Create warns about exactly this; layout is documented and warned, not detected.
- **`row_id`** (future, [D30](/system/decisions/d30-layered-locator.md)) — Iceberg v3 row
  lineage; gated on ecosystem support.

**Duplicate-PK detection** lives on the shared key-scan path (both the `COORDINATES` fallback and
the `PREDICATE` primary read): a second distinct source row matching an already-matched key
increments `growlerdb_duplicate_pks_total` and logs a rate-limited warning naming the key. The
result stays deterministic — per key, the row with the **highest `(file, position)`** among the
scanned rows wins (the scan's early exit bounds detection to what it read). A nonzero rate means
the source table is not unique on the composite key — fix the source.

## Compaction re-map & the live-file bitmap

Iceberg compaction (`rewrite_data_files`, a `replace` snapshot) moves rows into new data files, so
every slot pointing into a rewritten file goes stale at once. Staleness is a **bounded background
cost**, not a per-read tax:

- **Live-file bitmap.** Interned files that disappeared from the live table are flagged **dead** —
  a small parallel `dead_files` key-set table in `aux.redb` (the `files` rows stay immutable),
  mirrored in memory and consulted at locator resolution: a locator into a dead file skips the
  doomed parquet point read and goes **straight to the pass-2 fallback** (whose result refreshes
  the slot). Dead flags are permanent tombstones — interned ids are never reused.
- **Background re-map.** Each node polls the source table's current plan on an interval
  (`--remap-interval-secs`, default 45 s; served by the snapshot-pinned plan cache, so the
  steady-state poll is one catalog call — observing metadata imposes nothing on the source) and
  diffs the live file set against the shard's interned live files. On a rewrite it marks the
  disappeared files dead, **column-projects only the key columns + positions** of the plan's added
  files, and bulk-patches each key's slot — batched and key-sorted (term-dictionary locality,
  ~1M lookups/s warm measured), fsynced per chunk under the writer-lock contract, the lock released
  between chunks so ingest and hydration never wait on a full re-map. Delete-bearing added files
  are skipped (their ingest positions are delete-shifted) and heal lazily.
- **Interleaving safety.** Slot patches are idempotent last-wins writes serialized by the writer
  lock, and the re-map patches a slot **only while it still points at a dead file** — so ingest
  upserts and lazy refreshes (which write fresher locations) are never clobbered with the older
  rewritten row; keys with no live doc are skipped. Verify-and-fallback remains the correctness
  net for every residual window.
- **Observability**: `growlerdb_locator_remap_events_total` / `growlerdb_locator_remapped_rows_total`
  (counters) and `growlerdb_locator_dead_files` (gauge); with the re-map on,
  `growlerdb_stale_locators_total` stays ≈ 0 under source compaction (regression-tested).

## The live-key set

Drift repair, `key_count`, and partition reconciliation need the exact set of **live** keys. It is
enumerated from the index itself: the composite-key encoding is partition-first and
length-prefixed, so a partition's keys form one contiguous raw-bytes prefix range of the `_keyenc`
term dictionary — and each term counts only if it has a live doc (postings + alive bitset).
Per-term liveness matters because the store defers merges: a deleted-but-unmerged doc's key term
stays in the dictionary until compaction, so raw term enumeration would over-report.

## Crash consistency

Commit ordering extends the original two-phase contract: the **location array is fsynced first**,
then the durable **Tantivy commit** lands, then the **redb txn** advances the checkpoint (+ batch
idempotency, new file interns). A crash between array fsync and Tantivy commit leaves only
unreachable orphan slots; between Tantivy commit and redb txn, the connector replays the batch
idempotently — so the index, its locators, and the checkpoint always agree on restart. This
underpins the [durability](/product/non-functional/durability.md) guarantee.

## Notes

Segments are the unit of everything (build, merge, backup, cold-bundle); backups also carry
`location.arr` ([backup format](/system/storage/backup-format.md) 1 — the layered format). Part of
the [index store](/system/storage/index-store.md).
