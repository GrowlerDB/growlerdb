---
type: Concept
title: Observability (instrumentation)
description: OpenTelemetry instrumentation and the SLIs behind the observability feature.
tags: [system, observability, otel, sli]
resource: /crates/growlerdb-telemetry
timestamp: 2026-07-04T14:22:00
---

# Observability (instrumentation)

The instrumentation behind the [observability](/product/functional/observability.md) feature — the
`growlerdb-telemetry` crate exports **OpenTelemetry** traces, metrics, and logs via OTLP to the
[LGTM stack](/system/runtime/dependencies/lgtm.md).

## SLIs

Metrics defined as SLIs include: query latency; **ingest lag** (`growlerdb_ingest_lag_ms{index}`) and
**shard availability** (`growlerdb_shards_up/total{index}`); **cold-cache hit rate**; and
**segments/compactions**. Gauges are kept fresh by a background sampler in the control plane so
dashboards don't depend on console polling.

**Topology sync:** a gateway hot-reload that rejects a malformed shard map keeps the old, servable
routing and records it through the background-loop SLIs
(`growlerdb_background_failures_total{loop="topology-swap"}` plus the
`growlerdb_background_last_success_timestamp{loop="topology-swap"}` gauge). A node stuck on a stale
routing table is then alertable ("topology hasn't synced in N minutes") rather than silently healthy
behind a plain `/healthz` 200.

**Write-path latency + backpressure:** alongside the ingest **throughput** counter
(`growlerdb_ingested_docs_total{index}`) and the lag gauge, the node emits
`growlerdb_write_duration_seconds{index}` — a true histogram of the per-commit
stage+commit wall-clock, on both the ordinal and windowed write paths — and
`growlerdb_write_queue_depth{index}`, the in-flight-commit gauge (admission is refuse-not-queue, so it
pins at the ceiling under backpressure). Together they **localize an ingest ceiling to the commit
path**: a rising write p95 while node CPU stays flat means the bottleneck is the write, not query or
node compute — the exact signal needed to explain a ~6.5k
docs/s ceiling. `growlerdb_write_phase_duration_seconds{phase}` then splits a commit into
`apply` / `location_sync` / `tantivy_commit` / `redb` so the cost is attributable to a phase — all
O(batch), which is how p99 ~9.5s commits trace to **large source snapshots**
(the connector cuts only at snapshot boundaries, so a 300k-row generator append commits whole).
The node now **chunks the commit** ([ingestion](/product/functional/ingestion/index.md)):
`commit_staged` applies a batch larger than `GROWLERDB_WRITE_COMMIT_CHUNK` (~25k default) as
several bounded Tantivy commits, each made durable and searchable in turn, while advancing the source
checkpoint exactly once at the end — so per-commit `apply`/`tantivy_commit` latency stays bounded and
early docs are queryable mid-batch (freshness) regardless of source snapshot size, without changing the
exactly-once checkpoint contract. The Spark connector's own counters (`growlerdb_connector_*` — rows read, write-retries
by gRPC code, per-shard acks) are Prometheus-scraped too, so the same ceiling
is visible from the producer side (retries climbing = the node shedding load).

The same sampler also emits **[source-health](/system/source-health.md)** gauges
(`growlerdb_source_*{index}`) — data-file count, mean file size, delete files, snapshots —
read from source snapshot metadata to *diagnose* a source table that wants Iceberg maintenance (the
remedy stays the user's, outside GrowlerDB).

**REST RED metrics** (`growlerdb_http_requests_total{route,status}` + `_http_request_duration_seconds{route}`)
come from a single axum middleware over the merged `/v1/*` router, labelled by the matched
route *template* (bounded cardinality; unmatched paths bucket as `<unmatched>`). They drive the
console's Runtime "API" panels (request rate, 5xx/4xx rate, p95 latency) and the Search
"query status codes" panel (`route="/v1/search"`).

**Index size attribution:** `growlerdb_index_bytes{index}` is a shard's
full on-disk footprint, and `growlerdb_index_bytes_component{index,component}` splits it by structure
— `term` / `postings` / `positions` / `fieldnorms` (together the inverted index), `fast`
(fast-field cache), `store` (doc store), `locator` (hydration lookup: `location.arr` + `aux.redb`),
`other` (metadata/deletes). The components **sum to the total exactly**, so the index:source ratio
is attributable to the structure that drives it and storage changes are verifiable against the
exact file kind they target.
`growlerdb_index_deleted_docs{index}` is the **delete debt** a size sample must be read
against: superseded docs stay on disk until compaction merges them away, so a between-merges sample
overstates the steady-state footprint — the scale bench records debt + `growlerdb_segments_live`
alongside every size snapshot. All emitted on the compaction-loop tick.

**Convergence:** `growlerdb_index_docs{index}` — a shard's live doc count, emitted on the
compaction-loop tick alongside `growlerdb_index_bytes`. `sum(growlerdb_index_docs)` is GrowlerDB's own
count of what it holds; against `growlerdb_source_records` it drives the "source rows −
index docs → 0" convergence graph natively (no scale-test exporter). The *authoritative* dup-safe
assertion still compares to the source's DISTINCT-id count (Trino) at drain — raw `total-records` is
fooled by duplicate PKs, which collapse last-write-wins in the index.

**Node resource metrics** (CPU/mem/disk) are **not emitted by GrowlerDB** — they come from
`node-exporter` in the cluster metrics stack (bundled in the compose `stack` profile; the k8s
observability bundle / `kube-prometheus-stack` in production). The console's Runtime resource cards
query the standard `node_*` series and fall back to a "needs the metrics stack" state when absent.

## Notes

Alert rules are evaluated server-side (LGTM/Prometheus) and surfaced as severity rows in the console.
Deploy-specific config (the Grafana link) is served at runtime on `/v1/config`.
