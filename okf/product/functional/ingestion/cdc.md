---
type: Feature
title: Changelog / CDC ingestion
description: Propagate inserts, updates, and deletes from the Iceberg changelog into the index.
tags: [feature, ingestion, cdc, changelog]
timestamp: 2026-07-04T14:22:00
---

# Changelog / CDC ingestion

For sources that **mutate** (updates/deletes, not just appends), the connector reads the Iceberg
**changelog** so edits and deletes to source rows propagate into the index — keeping the derived index
faithful to the authoritative table.

## Behavior

- Insert/update/delete operations from the changelog are applied to the index.
- A [source-lineage guard](/quality/known-limitations/index.md) detects a dropped-and-recreated source
  (table-uuid mismatch) and serves **degraded** (`SOURCE_RECREATED`) rather than serving orphaned
  rows, until a [reindex](/product/functional/index-management/reindex.md) recovers.

## Notes

Append-mostly sources use the [append fast path](/product/functional/ingestion/streaming.md) instead.
Mutating CDC tables stay hash-sharded + hot (windowing is for append-mostly data).
