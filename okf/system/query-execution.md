---
type: Concept
title: Query execution
description: How a query runs — planning, pruning, scoring, cursors, and cost guards.
tags: [system, query, execution, pruning, hydration]
timestamp: 2026-07-04T14:22:00
---

# Query execution

How a [query](/product/functional/search/query.md) executes against the index.

- **Planning** — the query AST is validated against the schema and lowered to Tantivy queries;
  field-type rules are checked at execution.
- **Pruning** — a time-bounded query prunes **shards and windows** it can prove won't match, using
  per-window event-time zone-maps, so most of the corpus is never scanned. This is the main reason
  search is fast at scale.
- **Scoring** — BM25 with block-max WAND for efficient top-K.
- **Paging & PIT** — offset paging (page-fetch capped), keyset `search_after` cursors over fast fields
  with the composite-key tiebreaker, and point-in-time snapshots for consistent scroll/export.
- **Cost guards** — a leading wildcard is cost-guarded, Node page fetches are capped, and segment
  reads are bounds-clamped, so a pathological query can't run away.
- **Cross-shard merge** — the [gateway](/system/distribution.md) merges shard top-K, dedupes, and
  reports a true cross-shard `total`. A shard that fails or times out is dropped and the page is
  flagged `partial`; when **every** shard fails, the gateway distinguishes cause by the shards'
  own error codes. Because each shard runs the same query against the same schema, a bad query
  (e.g. a CIDR on a non-IP field, a sort on a non-`fast` field) fails them all with the same
  **client error**, which the gateway surfaces verbatim as a 4xx — so the caller learns *why*
  rather than seeing an opaque, retryable 500. If any failure is server-side/transient (or shards
  vanished without a status), the total failure stays a retryable `unavailable`.
- **Hydration point reads** — resolving a [hydration](/product/functional/hydration.md) locator's
  `(file, row position)` is a **targeted parquet read**: one footer-metadata read per data file,
  row-group scoping to the group(s) holding the requested positions, and a row selection to the
  exact rows — cost is bounded by the touched row groups, not the file size (rather than the file
  streaming from row 0 up to the requested position). Requests coalesce per file; key verification
  and the predicate fallback are unchanged; files carrying delete files keep the delete-applying
  streaming read. Foundation for every
  [D30](/system/decisions/d30-layered-locator.md) location strategy.
- **Hydration planning reuse** — the lookup service holds one **shared, lazily-connected** catalog
  client instead of rebuilding it per RPC (a source failure invalidates it; the next request
  reconnects), and pass 1's unpredicated current-snapshot plan is served from a **snapshot-pinned
  plan cache** (a small per-table LRU): each hydrate makes one catalog call to learn the current
  snapshot id, reuses the cached file-scan plan while it's unchanged, and replans (replacing the
  entry) when it advances — so steady-state lookups skip the per-batch manifest-list/manifest reads
  that would otherwise dominate p99. The predicate fallback (pass 2) is per-request and uncached.
  Hit/miss are observable as `growlerdb_plan_cache_hits_total` / `_misses_total`
  ([D30](/system/decisions/d30-layered-locator.md) foundations).

## Notes

The consistency bound (eventually consistent with Iceberg) is described under
[consistency](/product/non-functional/consistency.md).
