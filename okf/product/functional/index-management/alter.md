---
type: Feature
title: Alter index
description: Change an index definition in place where safe; guide changes that require a reindex.
tags: [feature, index, alter, schema-evolution]
timestamp: 2026-07-04T14:22:00
---

# Alter index

Evolve an existing index. `POST /v1/index:alter` **dry-runs a plan** or **applies** a definition
change. A multi-shard index is applied by the [control plane](/system/runtime/components/control-plane.md)
(the gateway forwards it); a single embedded node applies its own.

- **In-place** — metadata-only changes apply without a rebuild: an index **rename**, a **`sensitive`**
  flip, and a **`max_bytes`** redeclaration. Nothing stored or indexed differs.
- **Reindex-requiring** — everything else needs a rebuild: **adding or removing** a mapped field, any
  field **type/analyzer/`record`/`fieldnorms`/`fast`/`indexed`/`cached`/vector/variant** change, and
  **key/`source`/`shard_count`** changes. A segment's Tantivy schema is fixed at
  build time, so these can't be applied to existing segments. (Adding new keys *within* a VARIANT field
  is not a definition change — those flatten in place, no reindex.)

**Apply is durable.** The control plane commits the new **registry definition** as a compare-and-swap
on its version, so the change survives restart — the registry is the source of truth, not a node's
local copy. *When* that commit lands depends on whether a rebuild is needed:

- An **in-place-only** apply commits the definition immediately (nodes reload it via `GetIndex`).
- A **reindex-requiring** apply over the control plane defers the commit to the **cutover**. It runs a
  coordinated [reindex](/product/functional/index-management/reindex.md) **from the new definition**
  across every shard (or window) — the definition travels to the nodes as the reindex payload, not via
  the registry — and commits the new definition **atomically with the generation bump** in the reindex's
  final phase, only after every shard has promoted. So a rebuild that fails (disk, timeout, cancel, a
  node crash) leaves the registry on the **old** definition, matching the untouched on-disk shards:
  never a registry that advertises a schema the segments don't have, and never a node that reboots
  mid-rebuild into a `SchemaChanged` against a definition it can't satisfy. The response reports
  `applied`, `reindex_triggered`, and the new `generation`.

A single embedded node instead **guides**: it applies only the in-place changes and reports the reindex
reasons for you to run a reindex.

## Boot-time definition reload

A node that boots after a durable alter **loads the registry's definition from the control plane**
rather than rebuilding from a stale local `index.json` (or one re-derived from the source). `GetIndex`
carries the authoritative resolved definition (`definition_json`, tracked by `definition_version`), and
in cluster mode the boot build step (`growlerdb index --control-plane` / `--define-only
--control-plane`) uses it — so the on-disk index opens/builds at the schema its reindexed segments were
built with, instead of hitting `SchemaChanged` against the altered on-disk index. `serve` /
`serve-pool` then read that CP-authoritative `index.json`. On first boot (the index isn't registered
yet) it falls back to the local / re-derived definition and registers it. This closes cross-restart
durability for a durable alter across the sharded and placement-pool boot paths.

## Notes

Schema-evolution rules live in the [data model](/system/storage/data-model.md).
