---
type: Concept
title: Index store
description: The local index store — Tantivy segments on NVMe + a slim redb aux store, durably backed up.
tags: [system, storage, tantivy, redb]
resource: /crates/growlerdb-index
timestamp: 2026-07-04T14:22:00
---

# Index store

The **local, purpose-built** store the [node](/system/runtime/components/node.md) searches — Tantivy
inverted-index [segments](/system/storage/locators-segments.md) on local NVMe plus a **slim redb aux
store**, kept crash-consistent, and durably [backed up](/system/storage/backup-format.md) to object
storage. Local-first is what delivers search-engine latency instead of object-storage-scan latency.

## Structure

- **Segments** — immutable Tantivy segments; the unit of build, merge, backup, and query. They carry
  the key-term dictionary (`_keyenc`) and the `fast` fields hydration reuses as prune hints
  ([D54](/system/decisions/d54-store-less-hydration.md)). A hit's composite key is stored as the same
  compact `enc(key)` bytes the delete term uses — one format, computed once per doc. The doc store is
  **zstd**-compressed: lz4 only match-copies, so high-entropy stored values (hex/UUID keys,
  random-ish cached fields) pass through nearly uncompressed — zstd entropy-codes them (~40% store cut
  measured on hex keys). The compressor persists per index in `meta.json`.
- **Aux store (redb)** — meta (checkpoint, zone-map, lineage) and batch idempotency. No per-key state
  and no stored location: hydration is a store-less pruned key scan, and the live-key set is
  enumerated from the index ([D54](/system/decisions/d54-store-less-hydration.md)).
- **Pluggable directory** — a read-through object-storage `ObjectDirectory` + byte-bounded range cache
  serves [cold windows](/system/storage/cold-bundles.md) directly from object storage.

## Notes

Implemented in `growlerdb-index`. Because the store is derived, it can be dropped and rebuilt from
Iceberg (or restored from backup) — recovery is bounded by rebuild time, never data loss.
