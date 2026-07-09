---
type: Feature
title: Alter index
description: Change an index definition in place where safe; guide changes that require a reindex.
tags: [feature, index, alter, schema-evolution]
timestamp: 2026-07-04T14:22:00
---

# Alter index

Evolve an existing index. `POST /v1/index:alter` supports a **dry-run plan** or an in-place **apply**:

- **In-place** — additive/flag changes (e.g. a new source field auto-indexed, add a cached/fast flag
  to new segments) apply without a rebuild.
- **Reindex-requiring** — changes that alter how existing data is indexed are *guided*, not silently
  applied; the operator runs a [reindex](/product/functional/index-management/reindex.md).

## Notes

Single-shard today (multi-shard alter returns Unimplemented). Schema-evolution rules live in the
[data model](/system/storage/data-model.md).
