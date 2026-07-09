---
type: Feature
title: Query syntax (Lucene / KQL)
description: Choose the query string grammar per request — Lucene (default) or KQL.
tags: [feature, search, syntax, kql, lucene]
timestamp: 2026-07-04T14:22:00
---

# Query syntax (Lucene / KQL)

The [query](/product/functional/search/query.md) string can be written in two grammars, selected per
request:

- **Lucene** (default) — uppercase `AND` / `OR` / `NOT`.
- **KQL** — lowercase `and` / `or` / `not` (Kibana Query Language), via `"syntax": "kql"` (REST) /
  `SearchRequest.syntax` (gRPC).

Both parse into the same [AST](/product/functional/search/query.md); the console's search box has a
Lucene/KQL toggle.

## Notes

Only the search path honors the selector today; aggregate/export paths stay Lucene.
