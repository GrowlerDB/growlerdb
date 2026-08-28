---
type: Feature
title: Hydration
description: Resolve search coordinates to the full authoritative Iceberg rows, governed — store-less, by a key-equality scan pruned to the row's own partition/sort-key stats; standalone (keys:get) or inline with the search.
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

## How a key finds its row: the store-less pruned scan

The index keeps **no per-row location** — no stored `(file, position)`, no choice to make. Hydration
re-finds each row by a **key-equality scan against Iceberg**, and the trick that keeps it a point
read is pruning: it AND-s the hit's **own stored partition/sort-key value** (a `fast` field, e.g.
`ts`) onto the scan predicate, so Iceberg's **row-group** min/max stats skip straight to the row
groups that can hold the row. On a **sort-clustered** table (`ts` under a Spark `WRITE ORDERED BY`)
a scattered top-k over a hash-routed high-cardinality key (`request_id`) still reads only ~one row
group per hit. The scan is **byte-budget-bounded** so no single lookup can turn into a whole-snapshot
scan, and every fetched row is **verified** against the requested key (a phantom row is never
returned). A genuine duplicate key in the source is detected loudly (`growlerdb_duplicate_pks_total`;
deterministic winner among the scanned rows).

Because there is no stored location, there is **nothing to stale and nothing to heal**: a source
compaction (`rewrite_data_files`) only changes which row groups the stored sort-key value points the
scan at, and the next lookup prunes to them for free — no re-map, no staleness class, and no O(rows)
location floor to carry ([D54](/system/decisions/d54-store-less-hydration.md)).

**Honest scope.** The pruning needs a layout the row's own stored values can point the scan at — a
sorted or partitioned table. On a **large, unclustered, unpartitioned** table the stats can't prune
and a lookup degrades to a broad scan (bounded by the same byte budget); the fix is to compact the
source with a sort or partition. This is stated at create, not auto-detected.

## Variant tables: the interim Trino lane

For an index over an Iceberg v3 [variant](/product/functional/index-management/variant.md) table,
hydration takes a **separate lane** ([D48](/system/decisions/d48-variant-delivery.md),
[D49](/system/decisions/d49-variant-iceberg-rust-routing.md)): released iceberg-rust cannot parse a
v3 variant schema (it fails in `load_table`, which fronts even the direct-parquet point read), so a
per-index fork (`ResolvedIndex::has_variant_field`) routes the read through **Trino** — a
key-predicated point `SELECT` returning the variant column as JSON — while a non-variant index keeps
the native path **completely untouched**. Trino unreachable degrades loudly
([D45](/system/decisions/d45-degraded-vs-error.md)), never a silent miss. The seam returns the same
result shape as the native reader, so when iceberg-rust ships variant the native path takes over
without touching callers, leaving Trino as the permanent slow lane for delete-bearing files and the
unprunable-scan fallback.

## Notes

Hydration is the "fetch-by-key from the lake" half of the [thesis](/overview.md) (find-by-text in the
index, fetch-by-key from Iceberg). Point-lookup performance is a
[system](/system/query-execution.md) concern; access control is enforced here at retrieval.
