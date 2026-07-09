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
- A virtual-bucket layer (`NUM_BUCKETS = 1024`) maps `key → bucket → shard`. A **Bucketed** placement
  (a stored `bucket_owners` map) enables **online resharding** by moving buckets, not rehashing every
  key; a **Legacy** placement (`fnv % shards`) is the default and is byte-compatible when the bucket
  count divides evenly. The map lives in the
  [control-plane](/system/runtime/components/control-plane.md) registry.

## Scatter-gather

The [gateway](/system/runtime/components/gateway.md) fans a query to the target shards, merges their
top-K, and **dedupes by composite key** (safe while a bucket is briefly on two shards mid-reshard) —
with an honest `partial` flag and per-shard deadlines.

## Elasticity

Growth-only resharding (build new shards → commit the map at cutover → trim old, no missing/duplicate
read window) and skew relief (move a hot shard's bucket to a cold one). The gateway **hot-reloads** the
map so a reshard is online end-to-end.

## Notes

Routing logic is mirrored in the connector (`ShardRouter`) with golden-vector parity. Core in
`growlerdb-core::routing`.
