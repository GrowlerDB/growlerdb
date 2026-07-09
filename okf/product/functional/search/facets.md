---
type: Feature
title: Facets
description: Aggregated value counts per field to filter and drill into a result set.
tags: [feature, search, facets, aggregation]
timestamp: 2026-07-04T14:22:00
---

# Facets

Faceted aggregation over a query — value counts per **fast field** (e.g. `status`, `error_code`) so a
user can see the distribution and filter down. Served by `POST /v1/facets` (and the gRPC search path).

## Behavior

- Returns, per requested field, the top values and their document counts for the current query.
- Selecting a facet value adds a score-neutral **filter** clause to the query (ANDed in).
- The console renders facet groups in the search results rail; groups are collapsible.

## Notes

Facet fields must be [fast fields](/system/storage/data-model.md). Counts are computed cross-shard and
merged. Aggregation accuracy/claims are covered under [query execution](/system/query-execution.md).
