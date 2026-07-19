---
type: Concept
title: Distribution
description: Sharding, virtual-bucket routing, scatter-gather, and online elasticity.
tags: [system, distribution, sharding, routing]
timestamp: 2026-07-04T14:22:00
---

# Distribution

How an index is partitioned across nodes and how queries fan out.

## Sharding & routing

- An index has N **shards**; a document routes to a shard by its
  [composite key](/system/storage/data-model.md).
- A virtual-bucket layer (`NUM_BUCKETS = 1024`) maps `key → bucket → shard`. Every ordinal index is
  **Bucketed**: a balanced `bucket_owners` map is adopted at its first served-index registration
  (byte-compatible with the old `fnv % shards` when the bucket count divides evenly), enabling
  **online resharding** by moving buckets, not rehashing every key. The map lives in the
  [control-plane](/system/runtime/components/control-plane.md) registry and, once present, is the
  **sole source of truth for the routed shard count** — the registered/assigned count deliberately
  is not: during a grow the new build targets register *before* the cutover, so the assigned count
  runs ahead of the routed count. A **Legacy** placement (`fnv % shards`) survives only for an index
  no node has announced yet.

## Scatter-gather

The [gateway](/system/runtime/components/gateway.md) fans a query to the target shards, merges their
top-K, and **dedupes by composite key** (safe while a bucket is briefly on two shards mid-reshard) —
with an honest `partial` flag and per-shard deadlines.

## Elasticity

Growth-only resharding (build new shards → commit the map at cutover → trim old, no missing/duplicate
read window) and skew relief (move a hot shard's bucket to a cold one). The grow flow is:
register the new shards with the new total (`serve --shards N+k --shard-ordinal K` — this does
**not** change live routing), then `ApplyReshard`, which plans from the **stored map's** shard count,
builds the new shards filtered from source, and commits the new map. The cutover is a
**compare-and-swap** against the map the op planned from: two concurrent placement ops (a reshard
and a bucket move) can't last-write-wins revert each other — the loser gets a loud
`PLACEMENT_CONFLICT` (`FAILED_PRECONDITION`) and re-plans. The gateway **hot-reloads** the map so a
reshard is online end-to-end.

## Notes

Routing logic is mirrored in the connector (`ShardRouter`) with golden-vector parity. Core in
`growlerdb-core::routing`.
