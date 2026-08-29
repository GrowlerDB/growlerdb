---
type: Concept
title: Segments & aux store
description: Immutable Tantivy segments plus a slim redb aux store (meta, checkpoint, batch idempotency). The key-term dictionary finds live docs; sort/fast fields give hydration its prune hints. No stored per-row location — hydration is a store-less pruned scan (D54).
tags: [system, storage, segments, crash-consistency]
timestamp: 2026-08-28T00:00:00
---

# Segments & aux store

A shard is **immutable Tantivy segments** plus a **slim redb aux store**. There is **no stored
per-row source location** — hydration re-finds a row by a store-less, stats-pruned key scan
([D54](/system/decisions/d54-store-less-hydration.md)), so the aux store carries only what the write
and ingest paths need, not a per-key map.

- **Segments** are immutable Tantivy index files. New documents go into a new segment; deletes/updates
  are handled per-generation (live-docs / tombstones), and [compaction](/product/functional/index-management/compact.md)
  merges segments to bound their count and reclaim space.
- **Aux store (`aux.redb`)** holds meta (checkpoint, zone-map, lineage) and batch idempotency —
  no per-key table. It is tiny, so a parked cold window keeps it **local** while the segment bulk
  is served read-through from object storage.

## Finding a live doc, and the prune hints for its row

- **Key-term dictionary** (identity) — a hit's composite key is stored as the same compact `enc(key)`
  bytes the delete term uses, in the `_keyenc` term dictionary; that dictionary maps a key to its
  live doc, and is how a key is confirmed **present** (a local `NotFound` before any catalog connect)
  and how the [live-key set](#the-live-key-set) is enumerated. One key encoding, computed once per doc.
- **Sort / fast fields** (prune hints) — the columnar `fast` fields a hit already carries (e.g. `ts`)
  are what hydration AND-s onto its Iceberg key-scan predicate so row-group min/max stats prune to the
  wanted row groups ([hydration](/product/functional/hydration.md)). They are ordinary query fields;
  hydration just reuses the stored value as a prune hint. No dedicated location field exists.

## The live-key set

Drift repair, `key_count`, and partition reconciliation need the exact set of **live** keys. It is
enumerated from the index itself: the composite-key encoding is partition-first and
length-prefixed, so a partition's keys form one contiguous raw-bytes prefix range of the `_keyenc`
term dictionary — and each term counts only if it has a live doc (postings + alive bitset).
Per-term liveness matters because the store defers merges: a deleted-but-unmerged doc's key term
stays in the dictionary until compaction, so raw term enumeration would over-report.

## Crash consistency

A two-phase commit contract: the durable **Tantivy commit** lands first, then the **redb txn**
advances the checkpoint (+ batch idempotency). A crash after the Tantivy commit but before the redb
txn replays the batch idempotently (the connector re-sends; the delete-then-add-by-key path is
idempotent), so the index and the checkpoint always agree on restart. This underpins the
[durability](/product/non-functional/durability.md) guarantee.

## Notes

Segments are the unit of everything (build, merge, backup, cold-bundle); a backup carries the
segments + `aux.redb` ([backup format](/system/storage/backup-format.md)). Part of the
[index store](/system/storage/index-store.md).
