---
type: Concept
title: Index store
description: The local index store — Tantivy segments on NVMe + the layered locator (location array + slim redb aux store), durably backed up.
tags: [system, storage, tantivy, redb]
resource: /crates/growlerdb-index
timestamp: 2026-07-04T14:22:00
---

# Index store

The **local, purpose-built** store the [node](/system/runtime/components/node.md) searches — Tantivy
inverted-index [segments](/system/storage/locators-segments.md) on local NVMe plus the **layered
locator** (a dense location array + a slim redb aux store), kept crash-consistent, and durably
[backed up](/system/storage/backup-format.md) to object storage. Local-first is what delivers
search-engine latency instead of object-storage-scan latency.

## Structure

- **Segments** — immutable Tantivy segments; the unit of build, merge, backup, and query. They also
  carry the locator's identity + reference layers (key terms and the `_locid` fast field, D30).
  A hit's composite key is stored as the same compact `enc(key)` bytes the delete term uses —
  one format, computed once per doc (task-212). The doc store is **zstd**-compressed (task-212):
  lz4 only match-copies, so high-entropy stored values (hex/UUID keys, random-ish cached fields)
  pass through nearly uncompressed — zstd entropy-codes them (~40% store cut measured on hex
  keys). The compressor persists per index in `meta.json`.
- **Location array** — `location.arr`, the dense locator ID → (interned file, row position) store
  [hydration resolves through](/system/storage/locators-segments.md); fsynced before the Tantivy
  commit, patched in place on moves, always local (even for cold windows).
- **Aux store (redb)** — meta (checkpoint, zone-map, lineage), batch idempotency, and the interned
  file table. No per-key state: locators live in the layers above, and the live-key set is
  enumerated from the index (D30).
- **Pluggable directory** — a read-through object-storage `ObjectDirectory` + byte-bounded range cache
  serves [cold windows](/system/storage/cold-bundles.md) directly from object storage.

## Notes

Implemented in `growlerdb-index`. Because the store is derived, it can be dropped and rebuilt from
Iceberg (or restored from backup) — recovery is bounded by rebuild time, never data loss.
