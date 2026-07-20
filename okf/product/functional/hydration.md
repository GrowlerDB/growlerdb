---
type: Feature
title: Hydration
description: Resolve search coordinates to the full authoritative Iceberg rows, governed — via a per-index location strategy (coordinates or predicate); standalone (keys:get) or inline with the search.
tags: [feature, hydration, keys, retrieval]
timestamp: 2026-07-20T00:00:00
---

# Hydration

Search returns [coordinates](/glossary.md) (the composite key), not documents. **Hydration** resolves
those coordinates to the **full authoritative rows** via `POST /v1/keys:get` (gRPC `Lookup`) — a fast
point lookup against Iceberg, governed by the catalog so a user only retrieves what they may read.

## Three retrieval paths

- **Cached display fields (no hydration).** If the result columns are marked
  [`cached`](/system/storage/data-model.md), their values return **with the hit**, so a results page
  renders without any Iceberg round trip.
- **Full hydration.** For the authoritative record (including large/uncached fields), fetch by key —
  typically on row-open.
- **Inline hydration (one call).** A search (lexical, semantic, or hybrid) with `hydrate: true`
  returns each hit's authoritative row **with the search response** (`hit.row`, projected by
  `hydrate_columns`) — the search → keys:get round trip collapsed for callers that want documents,
  not coordinates (SDK/agent retrieval, the [OpenSearch adapter](/product/interfaces/opensearch-adapter.md)'s
  `_source`, the [MCP `search` tool](/product/interfaces/mcp-server.md)). The gateway orchestrates it
  through the **same governed GetByKey path** (never a new one) under the query's single admission
  permit; only the returned page hydrates (a page above the hydration batch maximum is rejected up
  front), and a row that fails to resolve degrades **per hit** (`hit.hydrate_error`) — never the
  search. Cached fields stay the no-round-trip default; inline hydration is the explicit opt-in.

## How a key finds its row: the location strategy

How the lookup reaches the source row is a per-index choice
(`location_strategy` in the [definition](/product/functional/index-management/create.md);
[D30](/system/decisions/d30-layered-locator.md)):

- **`COORDINATES`** (default) — the index keeps each row's `(file, position)`
  ([locators](/system/storage/locators-segments.md)), so hydration is a targeted parquet point
  read. Works well on **any** table; costs ~13–15 B/row of index and background healing when the
  source compacts. Choose it unless you know better.
- **`PREDICATE`** — the index keeps **no location data**; hydration re-finds the row by a
  key-equality scan pruned by partition values + column stats (temporal keys included). Zero
  location bytes and nothing to heal — but **only worth it when the key correlates with the table
  layout** (partitioned on key fields, or clustered/sorted by the key). On an unclustered
  high-cardinality key the scan can't prune and lookups degrade to broad scans — create warns
  about this; the layout is not auto-detected.

Under either strategy every fetched row is **verified** against the requested key (a phantom row is
never returned), and a genuine duplicate key in the source is detected loudly
(`growlerdb_duplicate_pks_total`; deterministic highest-`(file, position)` winner).

## Notes

Hydration is the "fetch-by-key from the lake" half of the [thesis](/overview.md) (find-by-text in the
index, fetch-by-key from Iceberg). Point-lookup performance is a
[system](/system/query-execution.md) concern; access control is enforced here at retrieval.
