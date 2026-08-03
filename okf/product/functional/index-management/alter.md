---
type: Feature
title: Alter index
description: Change an index definition in place where safe; guide changes that require a reindex.
tags: [feature, index, alter, schema-evolution]
timestamp: 2026-07-04T14:22:00
---

# Alter index

Evolve an existing index. `POST /v1/index:alter` supports a **dry-run plan** or an in-place **apply**:

- **In-place** — metadata-only changes apply without a rebuild: an index **rename**, a **`sensitive`**
  flip, and a **`max_bytes`** redeclaration. Nothing stored or indexed differs.
- **Reindex-requiring** — everything else is *guided*, not silently applied: **adding or removing** a
  mapped field, any field **type/analyzer/`record`/`fieldnorms`/`fast`/`indexed`/`cached`/vector/variant**
  change, and **key/`source`/`shard_count`/`location_strategy`** changes. A segment's Tantivy schema is
  fixed at build time, so these can't be applied to existing segments; the plan lists the reasons and
  the operator runs a [reindex](/product/functional/index-management/reindex.md). (Adding new keys
  *within* a VARIANT field is not a definition change — those flatten in place, no reindex.)

## Notes

Single-shard today (multi-shard alter returns Unimplemented). Schema-evolution rules live in the
[data model](/system/storage/data-model.md).
