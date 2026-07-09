---
type: Requirement
title: Scale & cost
description: The scale envelope and the object-storage cost model for long retention.
tags: [nfr, scale, cost, retention]
timestamp: 2026-07-04T14:22:00
---

# Scale & cost

- **Scale envelope:** tens of millions+ of documents per index and long retention (1 year+), driven
  by the [IoT telemetry](/product/use-cases/iot-telemetry.md) lead use case; scales out via shards,
  windows, and source partitions. Per-node index size is capacity-bounded by local disk; the highest
  scales lean on the [cold, object-storage-served backend](/product/functional/cold-tiering.md).
- **Cost:** object-storage economics for retention — no always-on hot replicas of cold data; a local
  hot window + object-storage backup. Index size is a small fraction of the source table.

**Status.** v1 **design targets**, not yet benchmarked — validated by
[scalability/benchmarking](/quality/scalability.md) (a scale run on real hardware).
