---
type: Glossary
title: Glossary
description: GrowlerDB terminology — the domain vocabulary used across this knowledge base.
tags: [glossary, terminology]
timestamp: 2026-07-04T14:22:00
---

# Glossary

Core GrowlerDB terms. Concepts elsewhere in this bundle link here for definitions.

- **GrowlerDB** — a *growler* is a small chunk of ice calved off an iceberg, floating free; it fits a
  search index derived from Apache **Iceberg**.
- **Derived index** — the full-text index GrowlerDB maintains. Secondary to Iceberg, rebuildable from
  it, never authoritative.
- **Primary key / composite key** — the Iceberg key a document is indexed under: partition fields +
  identifier fields. The bridge from a search hit back to the authoritative row.
- **Coordinates** — the composite key returned with a search hit; the handle used to hydrate the row.
- **Hydration** — resolving a hit's coordinates to the full authoritative record via a point lookup
  against Iceberg (`keys:get`).
- **Cached (display) fields** — field values stored *with* the hit so a results page renders without
  hydration (the `_source`-equivalent).
- **Fast fields** — columnar-stored fields that are sortable/filterable/aggregatable.
- **Segment** — an immutable Tantivy index segment; the unit of the local index store.
- **Locator** — the record (in redb) mapping a document key to its position, kept crash-consistent
  with the Tantivy commit.
- **Shard** — a horizontal partition of an index served by one node; queries scatter-gather across
  shards and merge top-K.
- **Ordinal** — a shard's index (0..N-1); a node serves one or more ordinals.
- **Bucket** — a virtual routing unit (fixed count) mapped to shards, enabling online elasticity
  (resharding) without rehashing every key.
- **Window / windowing** — time-based partitioning of an index by ingest time; old windows are
  immutable and parkable, queries prune by event-time zone-maps.
- **Cold tier / cold window** — an old window served read-through directly from object storage (via a
  range-cached object directory) instead of local disk.
- **Hot cache** — the warm structural bytes kept locally for a cold window to speed cold reads.
- **Replica** — a read-only copy of a shard kept in sync by segment shipping (or re-index from source).
- **Control plane** — the cluster registry (indexes, shards, routing, tokens, roles); serves gRPC.
- **Gateway** — the public Engine API; routes/scatter-gathers to nodes and merges results.
- **Node** — builds and serves an index (or a shard/window of one); exposes search + Write gRPC.
- **Connector** — the Spark job that reads an Iceberg changelog and streams updates into a node.
- **Checkpoint** — the persisted ingestion position enabling exactly-once resume.
- **Exactly-once** — the ingestion guarantee: no committed-data loss and no duplicates across restarts.
- **Snapshot** — an Iceberg table snapshot; ingestion advances the index's committed snapshot toward
  the source head.
- **Catalog** — the Iceberg REST catalog (Polaris) that governs tables and hydration.
- **PIT (point-in-time)** — a pinned view for consistent pagination/export across a result set.
- **Scatter-gather** — the gateway querying all target shards in parallel and merging their top-K.
- **Alias / ILM** — a named pointer to one or more indexes enabling atomic reindex-and-swap and
  index-lifecycle retention.
