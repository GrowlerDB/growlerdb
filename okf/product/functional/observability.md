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

- An **index-scope selector** ("All indexes" by default) that governs the index-dimensioned tabs
  (**Search / Data / Ingestion / Source**): every metric there is per-index at the source, so picking
  an index reflows the cards and hero charts to it and filters the ingestion drill-down. **Runtime**
  (per-node CPU/mem/disk, `up`, route-level API RED metrics) and **Access** (logins) have no `{index}`
  dimension and stay cluster/node-wide — a note says so when an index is selected. The choice persists
  across reloads; the selector is hidden when no control plane is fronted.
- **Fleet-first** default view (all indexes): the aggregate surfaces the outlier rather than making you
  hunt for it. The index-keyed "collapse" cards — ingest lag, ingest throughput, source rows,
  small-file size — carry a **"worst / top: `index`"** annotation, and a card's expand modal shows a
  per-index **top-N breakdown** (top 6 + "other") whose rows **click to scope the whole screen** to
  that index. The ingestion hero renders the per-index index-rate **stacked by index** with the
  Iceberg-append total overlaid — so one index falling behind shows as its own band. (The size/skew
  cards read a per-shard `index="<name> s<n>"` gauge, which the selector still scopes but which has no
  clean per-index top-N, so they carry no breakdown annotation.)
- A persistent **Alerts** strip (critical/warning severity rows, evaluated server-side) above
  **sub-tabs** that group the signals:
  - **Search** — query rate, error rate, latency (p50/95/99), hydrate rate/latency, stale/drift, cold-cache hit.
  - **Runtime** — processes up, and (with the cluster metrics stack) API request/error/status/latency and per-node CPU/mem/disk.
  - **Data** — GrowlerDB size, segments, index-size-by-component, Iceberg-match; the index:source overlay.
  - **Ingestion** — the *Iceberg-append-vs-GrowlerDB-index* overlay, throughput, lag, and a per-index → per-shard drill-down (the old standalone Ingestion screen, folded in). A shard that has built and caught up to the source snapshot reports **`in_sync`** even when it holds **zero rows** (a sparse shard in a multi-shard index, or a currently-empty source records the snapshot it caught up to), so a legitimately-empty shard shows green, not a grey `uninitialized`. On the **[HA placement pool](/system/decisions/d52-placement-pool.md)**, each served shard is probed for its committed checkpoint by index selector (a multi-index pool node needs the index to route the probe), so a healthy pool reports **`in_sync`** / `shards_up`, not a spurious `unreachable`; the bundled Prometheus therefore scrapes the **pool** nodes, not the pre-pool per-index node names.
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
