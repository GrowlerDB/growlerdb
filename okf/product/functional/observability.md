---
type: Feature
title: Observability
description: SLI dashboards and alerts users see — search/ingest health, lag, shards, cold-cache.
tags: [feature, observability, metrics, dashboards]
timestamp: 2026-07-04T14:22:00
---

# Observability

The user-facing view of system health: **SLI dashboards + alerts** in the
[console](/product/interfaces/ui.md) Observability screen (and Grafana). It is organised so it
*answers* the product questions (["does GrowlerDB keep up with Iceberg?", "…match Iceberg?",
"index:source size ratio?"](/quality/scale-test-plan.md)) rather than listing raw metrics.

## What you see

- A persistent **Alerts** strip (critical/warning severity rows, evaluated server-side) above
  **sub-tabs** that group the signals:
  - **Search** — query rate, error rate, latency (p50/95/99), hydrate rate/latency, stale/drift, cold-cache hit.
  - **Runtime** — processes up, and (with the cluster metrics stack) API request/error/status/latency and per-node CPU/mem/disk.
  - **Data** — GrowlerDB size, segments, index-size-by-component, Iceberg-match; the index:source overlay.
  - **Ingestion** — the *Iceberg-append-vs-GrowlerDB-index* overlay, throughput, lag, and a per-index → per-shard drill-down (the old standalone Ingestion screen, folded in).
  - **Source** — source size, [source-health](/system/source-health.md) (small-file / snapshot signals), commit rate.
  - **Access** — sign-in / failure / session / logout signals.
- Each card is a clean value + sparkline; **hover** reads the value at a point, a **ⓘ** gives
  self-serve help, and an **expand** control opens a full detail chart (axes, legend, tooltip). A few
  "hero" overlay charts show relationships a sparkline can't.
- A runtime Grafana deep-link (served on `/v1/config`, hidden when unset) for deep dashboards.

The **Runtime** resource panels (busiest-node CPU / memory / fullest-disk) read from `node-exporter`
in the cluster metrics stack. The local `just stack` bundles it, and the k8s observability bundle
(or a cluster's `kube-prometheus-stack`) provides it in production; where it isn't running, those
cards show a **"needs the metrics stack"** state rather than a misleading 0.

## Notes

The instrumentation behind these views (OpenTelemetry, metric definitions) is a
[system concern](/system/observability.md); using monitoring to *maintain* quality is covered under
[quality](/quality/reliability.md).
