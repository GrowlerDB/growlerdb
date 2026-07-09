---
type: Requirement
title: Latency
description: Warm query and hydration latency targets.
tags: [nfr, latency, performance]
timestamp: 2026-07-04T14:22:00
---

# Latency

- **Query (warm):** p50 < 20 ms, p99 < 100 ms for boolean/term queries, top-K ≤ 100, over a pruned
  window — on local NVMe with partition/window pruning. Cold-start (object-storage-served windows) is
  slower by design.
- **Hydration:** a full-row fetch adds tens of ms (an Iceberg read) **only when requested**; a results
  page rendered from [cached fields](/product/functional/hydration.md) needs **no** hydration.

**Status.** A v1 **design target**, not yet benchmarked — validated by
[scalability/benchmarking](/quality/scalability.md).
