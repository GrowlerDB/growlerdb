---
type: Decision
title: 'D54. Store-less hydration — the pruned key scan is the only path'
description: Hydration keeps no stored per-row location; a key re-finds its source row by a key-equality scan pruned by partition/sort-key column stats, byte-budget-bounded and key-verified. Supersedes the layered locator (D30) and the original locator (D13).
tags: [decision, adr, hydration, storage]
timestamp: 2026-08-28T00:00:00
---

# D54. Store-less hydration — the pruned key scan is the only path

**Decision.** GrowlerDB stores **no per-row source location** and offers **no hydration strategy
choice**. A search returns keys; the engine re-finds each row by a **key-equality scan against
Iceberg**, pruned to the matching row groups by the row's own stored partition/sort-key value
(row-group min/max stats — e.g. a hit's `ts`), bounded by a byte budget, and **key-verified** before
return. There is no `location_strategy` option, no `LocationStrategy` enum, no `RowLocator`, no
`_locid` fast field, no dense location array (`location.arr`), no live-file bitmap, no background
compaction re-map, and the connector no longer emits `(file, position)` locators.

**Why.** The stored-locator design ([D30](/system/decisions/d30-layered-locator.md)) bought a targeted
point read at the cost of an O(total rows) hot location array *and* a whole-index staleness event on
every Iceberg compaction: `rewrite_data_files` moves every row, so every stored locator goes stale at
once. Healing that needed a background re-map that reads O(table) key columns to re-point the array —
a maintenance loop whose cost tracks the source table, not the query load, and which the scale runs
never demonstrated converging (stale-rate rose ~1 per hydrated hit; the re-map metrics were absent at
0.5.0). Store-less hydration has **nothing to heal**: a compaction changes only which row groups a
key's stored sort-key value points the scan at, and the next lookup prunes to them for free. On a
sort-clustered table (`WRITE ORDERED BY ts`) a scattered top-k reads ~one row group per hit, so the
point-read cost is recovered without carrying the array or the re-map. This also removes the
O(rows) hot-NVMe floor that cold-tiering could not bound.

**Consequences.** One hydration path, one schema shape, no strategy flag on create/alter and no
strategy-change reindex reason. The redb aux store shrinks to meta/checkpoint/batch-idempotency +
the interned-file table's role disappears; crash consistency is the plain two-phase contract
(durable Tantivy commit, then the redb checkpoint advance) with no location array to fsync first.
Every fetched row is still verified against the requested key, and a genuine duplicate PK is still
detected loudly (`growlerdb_duplicate_pks_total`). **One real limitation:** a **large, unclustered,
unpartitioned** table gives the scan no stats to prune on, so a lookup degrades to a broad scan
(bounded by the same byte budget) — compact the source with a sort or partition to restore fast
fetch. This is stated at create, not auto-detected. The interim Trino variant lane
([D49](/system/decisions/d49-variant-iceberg-rust-routing.md)) was already store-less (Trino re-finds
by key), so it is unaffected.

**Status.** Accepted. Supersedes **[D13](/system/decisions/d13-locator.md)** (locator vs
PK-clustering) and **[D30](/system/decisions/d30-layered-locator.md)** (the layered locator and its
compaction re-map), both retired. The stored-locator apparatus is fully removed from the engine and
connector. Iceberg v3 row-lineage (`row_id`) remains a possible future *optimization* over this same
store-less contract, tracked under [D28](/system/decisions/d28-iceberg-v3.md), not a stored locator.
