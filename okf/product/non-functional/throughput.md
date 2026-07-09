---
type: Requirement
title: Ingest throughput & freshness
description: Aggregate ingest rate and steady-state index lag targets.
tags: [nfr, throughput, ingestion, freshness]
timestamp: 2026-07-04T14:22:00
---

# Ingest throughput & freshness

- **Throughput:** ≥ 250k docs/s aggregate (log-class), ~10–50k/s per indexer, scaling out with
  indexers / source partitions.
- **Freshness (lag):** steady-state < 30 s for changelog ingestion, < 10 s for the append fast path;
  sub-second would need the deferred hot tier. Lag is a monitored
  [SLI](/product/functional/observability.md).

**Status.** v1 **design targets**, not yet benchmarked — validated by
[scalability/benchmarking](/quality/scalability.md); the ingestion guarantees themselves are
[exactly-once](/product/functional/ingestion/checkpoints-exactly-once.md). The "scaling out with
indexers" mechanism now exists: the shard-group connector set
([D32](/system/decisions/d32-parallel-ingest.md), task-196).
