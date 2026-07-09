---
type: Concept
title: Catalog metadata
description: How GrowlerDB uses Iceberg catalog metadata — table UUID, snapshots, schema.
tags: [system, storage, catalog, iceberg, metadata]
timestamp: 2026-07-04T14:22:00
---

# Catalog metadata

GrowlerDB reads several pieces of Iceberg catalog metadata (through the
[catalog](/system/runtime/dependencies/iceberg-catalog/polaris.md)):

- **Table UUID** — recorded at index build; on mismatch the index knows its source was
  dropped-and-recreated and serves **degraded** (`SOURCE_RECREATED`) rather than serving orphaned
  rows (the lineage guard).
- **Snapshots** — the ingestion position; the index's committed snapshot advances toward the source
  head, and lag is snapshots-behind.
- **Source schema** — drives index-definition validation and field mapping at
  [create](/product/functional/index-management/create.md) time.
- **Data files** — resolved for [hydration](/product/functional/hydration.md) reads.

## Notes

A persistent catalog [metastore](/system/runtime/dependencies/metastore/postgres.md) is required so
this metadata survives a catalog restart.
