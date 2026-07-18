---
type: Process
title: Scalability & benchmarking
description: How the scale envelope and performance targets are measured and regression-gated.
tags: [quality, scalability, benchmark, performance]
timestamp: 2026-07-04T14:22:00
---

# Scalability & benchmarking

How GrowlerDB validates its [performance and scale](/product/non-functional/scale.md) targets — which
are design targets until measured.

## Method

- A **benchmark harness** (`bench/`) measures the realistic paths: filter/count latency, and **top-K
  documents** in three variants — coordinates-only, cached display fields, and full hydration — so the
  report reflects honest end-to-end "give me the events" latency, not just filter/count.
- A **scale run** on real hardware (a Hetzner k3s cluster, provisioned by
  [IaC](/system/deployment/iac.md)) measures ingest throughput and query p50/p95/p99 at QPS over tens
  of millions of events, warm and cold — see the [scale test plan](/quality/scale-test-plan.md) for
  the workload, duration, cluster, and run-duration cost model.
- **Published numbers** feed the release; a **CI regression gate** guards against regressions. The
  harness's current directional report (GrowlerDB vs Elasticsearch vs Trino on 1M rows) is published as
  a public **Performance** page on the docs site; the formal at-scale numbers are the pre-1.0 gate.

## Notes

The proper at-scale assessment is the gate before a confident 1.0 performance claim. Numbers are
GrowlerDB's own — no competitor benchmarks in the OKF.
