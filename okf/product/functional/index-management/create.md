---
type: Feature
title: Create index
description: Define and build an index over an Iceberg table — fields, key, cached/fast flags, windowing, location strategy.
tags: [feature, index, create]
timestamp: 2026-07-04T14:22:00
---

# Create index

Define an index over an Iceberg table and build it. The definition chooses:

- **Source** — the Iceberg table (+ scan mode: append or changelog).
- **Key** — the composite key (partition fields + identifier fields) for routing and hydration.
- **Mapping** — which fields to index (all vs an explicit list), types/analyzers, and per-field
  [`cached`](/system/storage/data-model.md) (returned with hits) / **`fast`** (sortable/filterable) /
  **`indexed`** (the inverted index; defaults **off** for fast numeric/date/IP fields, which the
  columnar path serves — task-215) / TEXT-only **`record`** + **`fieldnorms`** (posting detail and
  BM25 norms — task-216; see [data model](/system/storage/data-model.md)) flags; optionally a
  declared **timestamp** field and [windowing](/product/functional/windowing-time.md).
- **Sharding** — shard count + routing.
- **Location strategy** (`location_strategy`, [D30](/system/decisions/d30-layered-locator.md)) —
  how [hydration](/product/functional/hydration.md) locates a key's source row: `COORDINATES`
  (default; per-row location data, fast point reads on any table) or `PREDICATE` (store-less;
  re-find by a pruned key scan — effective only on key-correlated layouts, and create returns a
  warning saying so). Changing it later is reindex-only; auto-detection is deferred until the
  `row_id` strategy exists.

## Behavior

Create validates the definition against the source schema, registers the index in the
[control plane](/system/runtime/components/control-plane.md), and a [node](/system/runtime/components/node.md)
builds it. Non-fatal resolution **warnings** (e.g. the `PREDICATE` honest-scope note, or an
equality-delete column outside the key) come back in the create response and are logged. The
console offers create-from-introspection (describe the source, pick fields).

## Notes

REST `POST /v1/index...` / CLI `growlerdb index`. The definition is versioned with the index.
