---
type: Test Suite
title: Scale test plan (Hetzner)
description: The repeatable, time-boxed scale/performance run on a Hetzner k3s cluster — what is measured, the pluggable dataset/harness (http_logs by default), how long it runs, the cluster shape, the run-duration cost model, and the IaC that makes it repeatable.
tags: [quality, scale, performance, benchmark, hetzner, iac]
timestamp: 2026-07-04T14:22:00
---

# Scale test plan (Hetzner)

The concrete plan for the at-scale run that validates GrowlerDB's
[scale & performance targets](/product/non-functional/scale.md) on real hardware — the gate before a
confident 1.0 performance claim. It realizes the scale run described in
[scalability & benchmarking](/quality/scalability.md), using the
[benchmark harness](/quality/tests/performance-bench.md) at scale. **Designed to be repeatable**: a
parameterized, teardown-able run that can be re-executed for regression, not a one-off.

## What is tested

Driven by the [IoT-telemetry](/product/use-cases/iot-telemetry.md) lead use case's shape — tens of
millions+ of events over long retention. The primary corpus is the standard **`http_logs`** dataset
(see [Dataset & harness](#dataset--harness-pluggable) below); a fixed dataset keeps results comparable
across runs. Measured (GrowlerDB's own paths — no competitor benchmarks live here):

- **Ingest throughput** — events/s through the connector into shard primaries; snapshot advance + lag.
- **Query latency** — p50/p95/p99 at a target QPS, fanned out across shards via the gateway.
- **Top-K documents** in three variants — coordinates-only, [cached fields](/product/functional/search/index.md),
  and full [hydration](/product/functional/hydration.md) — the honest end-to-end "give me the events"
  path, not just filter/count.
- **Cold tier** — [parked-partition](/product/functional/cold-tiering.md) revive latency and
  hydration throughput; **warm vs cold cache**.
- **Maintenance overhead** — compaction cost under steady ingest.

## Questions the run answers

Each maps to metrics/graphs (dashboard: `deploy/k8s/observability/grafana.yaml`):

1. **Does GrowlerDB keep up with Iceberg ingest?** ingest-rate (`deriv(growlerdb_source_records)`) vs
   index-rate (`rate(growlerdb_ingested_docs_total)`), overlaid.
2. **Lag Iceberg→GrowlerDB?** `max(growlerdb_source_records) - sum(growlerdb_index_docs)` (rows) +
   `growlerdb_ingest_lag_ms` (time).
3. **Does GrowlerDB match Iceberg?** convergence — at drain the index's live doc count
   (`sum(growlerdb_index_docs)`) must equal the source's **DISTINCT-id** count (Trino, dup-safe — raw
   `total-records` is fooled by duplicate PKs, which collapse last-write-wins in the index), asserted
   by `bench/scale/convergence_check.py`, plus a sample of real ids each resolving to exactly one doc
   that hydrates. The k8s drain gate `deploy/k8s/streaming/convergence-gate.sh` is the count-only,
   compaction-racing sibling.
4. **Index:source size ratio,** stacked into **inverted-index / locator / fast-cache** (the size-attribution
   breakdown) vs `growlerdb_source_bytes`.
5. **Query performance at each storage milestone** (below).
6. **GrowlerDB vs Iceberg-alone** (Trino over the same table) at each milestone.
7. **Compute required at each ingest + storage scale** (CPU/mem/disk/net per node).
8. **Stability** — error rate + restarts.

## Staged scale protocol (step-ups + milestones)

Run in **steps**, capturing the full metric + graph set at each. On the current Hetzner cluster we
cover the low/mid scales directly and **extrapolate** the top:

- **Ingest step-ups:** **1k → 10k records/s** are reachable here (generator `BATCH`/`SLEEP_S`, connector);
  **100k/s** is **modeled** (connector parallelism now exists — the shard-group set — but
  needs a bigger cluster to drive it; see below). At each step
  record keep-up, lag, and resources. **Drive a rate with a bounded `BATCH` + short `SLEEP_S`, not a
  huge `BATCH`:** the generator's `BATCH` is one Iceberg snapshot = the connector's commit size (it
  cuts only at snapshot boundaries), and commit latency is ~O(snapshot) — a 300k `BATCH` self-inflicts
  ~9.5s p99 commits, ~10–30k stays sub-2s (the connector-side split is chunked commit).
- **Storage milestones:** grow the source to **1 GB → 10 GB → 100 GB** (fits the ~960 GB cluster disk);
  **1 TB** is **modeled**. At each milestone **freeze ingest, run the query load, and capture**:
  query p50/p95/p99, hydration latency, index:source ratio (+ breakdown), resource utilisation, and
  the **GrowlerDB-vs-Trino** comparison — into a results table + dashboard snapshots.
- **Extrapolation:** fit the reachable points (query-latency vs data-size; resources vs ingest-rate;
  index:source ratio) and project **1 TB / 100k rec/s** with explicit assumptions + ±ranges.
  Clearly label modeled vs measured.

**Cluster ceiling (honest scoping):** the interim cluster (4× `cpx42` shared, ~960 GB total disk)
can't hold **1 TB** and likely can't sustain **100k rec/s** — those are modeled. The full envelope
needs the Hetzner **dedicated-core limit raised** + more nodes/disk.

## Metrics & observability

Every component serves Prometheus `/metrics` (control-plane `:9101`, node `:9102`, gateway `:9103`).
A **headless test cluster has no Prometheus** — deploy one (scraping those ports) and set the chart's
`gateway.prometheusUrl` so the console's `/v1/stats/*` SLI panels have a backend (otherwise
Observability/Ingestion pages error with an HTML/JSON parse failure).

Coverage vs what a scale test needs:

- **Have (engine-native):** query latency (`growlerdb_query_duration_seconds` histogram → p50/95/99),
  indexing throughput (`rate(growlerdb_ingested_docs_total)`), live segments, Tantivy compactions,
  doc count.
- **Now engine-native (was the scale-test exporter's job):** the source side is emitted by GrowlerDB
  itself — **source size** (`growlerdb_source_records`/`_bytes`), **data-file count**
  (`growlerdb_source_data_files`), **index size** per shard + component breakdown
  (`growlerdb_index_bytes`), the **hydration-latency histogram**
  (`growlerdb_hydration_duration_seconds`), the **ingestion-lag gauge** (`growlerdb_ingest_lag_ms`),
  and the **index doc count** (`growlerdb_index_docs`) — so the source→index lag graph is
  `max(growlerdb_source_records) - sum(growlerdb_index_docs)` from native telemetry, no exporter. The
  dashboards (compose + k8s) and the staged harness read these.
- **Still exporter-only (`deploy/k8s/streaming/metrics-exporter.yaml`):** **Iceberg-compaction
  events** (`gdb_iceberg_last_compaction_timestamp_seconds`, `gdb_iceberg_compactions_total`) — those
  come from the maintenance job, not GrowlerDB. The distinct-id convergence count is Trino-only (too
  expensive to scrape every 15s), computed at drain by the convergence check.

**Iceberg maintenance cadence:** the CronJob default (`deploy/k8s/streaming/maintenance.yaml`) is
hourly — a *production* cadence. For a bounded test, run it **every ~10 min** (`*/10 * * * *`) so the
compaction↔hydration relationship is visible within the run window; each `replace` snapshot marks a
compaction to overlay on the hydration chart. **Compaction is a prerequisite for measuring hydrated
queries, not an afterthought:** streaming appends one data-file per commit, so an unmaintained source
accumulates thousands of tiny files (Run 7: **2769 files at 1 GB**) and hydration pays the small-file
tax — compacting to ~128 MB files cut a top-20 hydrated `_search` **~2×** (30s→16s). Two open caveats
the runs must watch (do **not** assume they hold): the maintenance CronJob is currently hardcoded to
the non-windowed `growlerdb.http_logs` table, so a **windowed** run gets no compaction unless you run
one targeting `…_windowed` ([[TASK-340]]); and the post-compaction locator heal does **not** yet
demonstrably persist — Run 7 saw `growlerdb_stale_locators_total` **rise ~1 per hydrated hit** with
topk latency flat across passes (re-refresh on every read), and the `growlerdb_locator_remap_*`
metrics an earlier draft cited are **absent in 0.5.0** ([[TASK-339]]). So a run asserts hydration p99
across compactions as a *measurement to report*, not an invariant known to hold.

## Operational prerequisites (learned bringing it up live)

- **Images (must be built from the code under test):** `release.yml` builds the signed,
  multi-arch **server** image (`ghcr.io/growlerdb/growlerdb`) only on a *release*, so its `latest`
  **lags merged main** — a post-merge / pre-release scale run that deploys `latest` silently runs stale
  code (this exact trap cost a windowed validation run). The **`scale-images` workflow** now builds a
  `growlerdb:dev` (+ commit-SHA) server image from merged main **alongside** `growlerdb-connector:dev`
  (Spark connector + Iceberg maintenance) and `growlerdb-seed:dev` (generator) — trigger it
  (workflow_dispatch) or let the on-push build run, then deploy with `IMAGE_TAG=dev` (or the commit
  SHA), **never `latest`**. `scale-up.sh` warns on `latest` and prints the deployed binary's
  `--version` (the `:dev` build stamps `GROWLERDB_VERSION=dev-<sha>`) so a stale image can't hide.
- **Hetzner cluster:** see [IaC](/system/deployment/iac.md) + `deploy/iac/README.md` — dedicated-vCPU
  quota (may force shared `cpx`), private-NIC wait, `--node-ip`/`--flannel-iface` (VXLAN MTU), and the
  public-IP TLS SAN are all handled in the cloud-init now.

## Dataset & harness (pluggable)

**The harness (`bench/scale/`) is dataset-agnostic — swapping datasets is a config change, not a
rewrite.** A dataset (a `workloads/<name>/` directory) is defined by exactly three things:

1. **Index schema / field mapping** — the GrowlerDB index definition for the corpus.
2. **Corpus** — a downloaded public dataset or a generator that emits the documents.
3. **Query mix** — the operations run against it (search shapes, filters, top-K), with a schedule.

Anything satisfying that contract plugs in; the driver and [IaC](#iac--repeatability) take the dataset
as a **parameter** (like node/shard count). We adopt the **OpenSearch Benchmark "workload" format** as
that contract, so GrowlerDB is driven through its [OpenSearch-compatible adapter](/product/interfaces/opensearch-adapter.md)
and the OSB-provided workloads come nearly for free.

- **Default: `http_logs`** — the OSB HTTP-access-log workload (~247M events, log/event shaped, matching
  the lead use case). Unpartitioned + hash-routed by `id` — the **non-temporal design boundary** (no
  cold tier, no query pruning; the genuinely weak case per
  [scale-ceilings](/quality/known-limitations/scale-ceilings.md)).
- **`http_logs_windowed`** — the same corpus, Iceberg-partitioned on an identity `day` column and the
  index **daily-windowed** on `ts`: the **temporal sweet spot** (windowed sharding, cold-tier
  park/revive, event-time query pruning, per-window routing). Its streaming generator advances a
  **synthetic timeline** (`LOGS_PER_DAY` events/day, not wall-clock), so day-windows form continuously
  as the run proceeds. **Now runnable on-cluster** via the control-plane-driven windowed node topology
  ([D33](/system/decisions/d33-windowed-topology.md)): `WORKLOAD=http_logs_windowed
  deploy/k8s/scale-up.sh` deploys empty windowed nodes, and the connector streams each row to its
  window's CP-assigned owner. Windowed nodes start **truly empty** — the build runs `growlerdb index
  --define-only` (writes `index.json`, builds zero windows), so windows are created only by streamed
  writes and placed across the pool, never batch-built-and-replicated on every node; and
  `scale-up.sh` deploys the connector **before** the windowed gateway-ready wait, since that gateway
  isn't `/readyz` until the connector has created ≥1 window. Running both variants captures
  the two ends of the temporal/non-temporal fork in one run — the honest GA story.
- **Drop-in alternatives** — e.g. **Wikipedia** (the Lucene/Tantivy-family full-text corpus) or
  **MS MARCO** follow the same three-part contract; adding one is authoring a workload module, not
  changing the driver or IaC.
- **Synthetic fallback** — a seeded, deterministic generator conforming to the same contract, for
  shapes the public workloads don't cover (specific telemetry field mixes, time-series cardinality).

Because the dataset is fixed/seeded and selected by parameter, runs stay **repeatable and
regression-comparable** across datasets.

## Duration (how long the run takes)

A **bounded ~1–3 day** run, phased so each phase's numbers are isolated, then torn down:

1. **Provision** — IaC `apply`, cluster + deps up (~15–30 min).
2. **Backfill / index build** — build the corpus's segments (~8–16 h at the modeled scale).
3. **Steady-ingest soak** — a delta stream held for several hours/overnight to exercise compaction,
   checkpoint lag, and stability.
4. **Query load** — ramp to target QPS; measure latency **warm**, then flush caches and measure **cold**.
5. **Cold-tier park/revive** ([TASK-229](/system/decisions/d39-automatic-cold-tiering.md)) — on a
   **windowed** run, cold-tiering is on by default (`scale-up.sh` sets `coldTier.enabled` +
   `parkIntervalSecs`; the windowed index def keeps the 3 most-recent windows hot). The synthetic
   corpus ages windows continuously, so older ones **auto-park** to the in-cluster `growlerdb-backups`
   bucket and serve read-through; sustained query traffic **auto-revives** a re-heated window. Drive +
   measure it with `python bench/scale/coldtier_validate.py` (polls `GET /v1/cold`; asserts auto-park,
   cold read-through correctness, and auto-revive; records revive latency + cold-cache SLIs to
   `coldtier_results.json`). Disable with `COLD_TIER=false`.
6. **Teardown** — capture first (`just capture`), then IaC `destroy` back to baseline (~minutes); the
   run cost is recorded.

## How it runs on Hetzner

- **Cluster:** a Hetzner **k3s** cluster; GrowlerDB deployed via the sharded
  [Helm chart](/system/deployment/helm-k8s.md). The whole pipeline is brought up in one command by
  **`deploy/k8s/scale-up.sh`** against **`values-scale.yaml`** (the in-cluster scale variant: static
  Prometheus, no ingress/OIDC, `_search` adapter on, deps credentials wired — 6 shards, one primary
  pod each). It enforces the deploy ordering (deps → source table → shards → connector sized to the
  shard count → observability) that otherwise silently mis-deploys.
- **Searchers / NVMe:** the index store wants **local NVMe**. This run uses **hourly cloud
  (dedicated-vCPU)** nodes with local-NVMe disk — the cheapest, cleanest fit for a time-boxed run
  within the budget below. Dedicated (AX-line) servers with multi-TB Gen4 NVMe are the path for a
  future, larger, production-representative run at a higher budget — **out of scope here**.
- **Object store + catalog:** Hetzner **Object Storage** (S3-compatible) holds the Iceberg lake, the
  cold tier, and index backups; a self-run Polaris REST catalog and the connector run in-cluster.
- **Egress:** effectively unmetered (dedicated) / large included allowance (cloud), so the
  hydration-chatty read path and the backfill don't incur per-GB egress — a material fit for this
  workload.

## Cost & budget (for the run duration)

**Budget: hard cap of US $200 per run** — the cost of the run itself, distinct from the production
monthly model. The corpus and cluster are sized to fit inside it; egress ≈ free on Hetzner.

Pinned configuration (Hetzner Cloud, current rates):

| Item | Config | Cost |
|---|---|---|
| Compute | **4× CCX33** (8 vCPU / 32 GB / 240 GB local NVMe; ~€0.222/h) — hosts the 6 shard pods + gateway + control-plane + connector + Polaris | €0.89/h → ~€43 (48 h) / ~€107 (5 days) |
| Object storage | Hetzner Object Storage, source + backup within the ~1 TB base tier | ~€5–15 |
| Egress | 30 TB included per CCX33 | ~€0 |
| **Run total** | a ~2–3 day time-boxed run, reruns included | **≈ €55–120 (~$65–140)** |

Comfortably under the $200 cap with margin for iteration. Staying under the cap **bounds the corpus**
to what fits ≤4 cloud-node local NVMe (tens to low-hundreds of millions of events) and the run to a
few days. A much larger, dedicated-NVMe run (billions of events, multi-TB index, monthly-billed
AX servers) would exceed $200 and is a **separate, higher-budget effort**. Note: reconfirm
dedicated-vCPU cloud rates at provisioning.

## IaC & repeatability

Provisioning is [Infrastructure as Code](/system/deployment/iac.md) — Terraform + the hcloud provider
in **`deploy/iac/`**, with the harness in **`bench/scale/`** — and repeatability is the point:

- **Parameterized** — node count/type, shard count, **dataset** (default `http_logs`), and corpus
  size — so a run scales up/down or switches dataset by changing inputs, and `apply`/`destroy` are
  repeatable.
- **Fixed / seeded dataset** — a pinned public workload or seeded generator so successive runs are
  comparable and can be **regression-gated**, whichever dataset is selected.
- **Cost-guarded teardown** — `destroy` returns the account to baseline; the run cost is recorded and
  kept within the **$200 cap** (the time-box and node count are the levers).
- **Committed to the repo** — the whole run is reproducible from version control; **secrets (the
  hcloud token) stay out of git**.

Published numbers feed [release readiness](/quality/release-readiness.md); a CI regression gate guards
against drift.

## Capturing results (for the analysis)

At the end of the run — **before `terraform destroy`** — capture the metrics/logs, because Prometheus
+ Loki are **in-cluster and ephemeral**: an un-captured post-mortem dies with the cluster. This is
**automated** by `bench/scale/capture.py` (`just capture "<purpose>"`), so it isn't a manual
screenshot ritual racing teardown.

**What it collects**, into a timestamped run directory:

- **Metric time-series** — Prometheus `query_range` over the run window for the graph set: doc-count
  growth (`growlerdb_source_records` vs `growlerdb_index_docs`); **query latency** p50/p95/p99
  (`growlerdb_query_duration_seconds`) + **hydration latency** (`growlerdb_hydration_duration_seconds`);
  **throughput** into Iceberg (`deriv(growlerdb_source_records)`) and GrowlerDB
  (`rate(growlerdb_ingested_docs_total)`); the **write-path trio** (`growlerdb_write_duration_seconds`,
  `growlerdb_write_queue_depth`, connector retries) that localizes an ingest ceiling; **source→index
  lag** (`growlerdb_ingest_lag_ms`); **index bytes** (`growlerdb_index_bytes`); the locator-heal
  signals (`growlerdb_stale_locators_total` ≈ flat, `growlerdb_locator_remapped_rows_total`); and the
  **cold-tier cache** hit-ratio. Dumped as JSON (diff-able, re-plottable) rather than one-off images.
- **Logs** — the connector / hot node / gateway streams from **Loki** (`LOKI_URL`), to correlate a
  latency spike with the log line.
- **Raw numbers + cost** — the harness `results.json` (per-query p50/95/99 + QPS) and the recorded
  **run cost** (`--cost`).
- **Screenshots** — *optional* dashboard images (`--screenshots`, Grafana render API), **bounded** by
  count + a size budget. They can grow large without much value, so they are **off by default and
  never committed**.

**Durable record vs heavy artifacts.** The per-run directory (`bench/scale/runs/<run>/` — metric/log
dumps, `audit.json`, optional screenshots) is **gitignored**; archive/upload it if a run matters. The
**committed** record is `bench/scale/RUNLOG.md` — a bounded, append-only ledger with **one compact row
per run** (start time, duration, **purpose**, run **parameters**, a result summary), so the history of
*what was validated, when, why, and with what config* stays in git without bloat.

**Teardown is gated on capture:** run `just capture …` and confirm the ledger row + run dir before
`terraform destroy`.

**Staged results artifact** (the [staged protocol](#staged-scale-protocol-step-ups--milestones)):
`bench/scale/staged_run.py` drives the ingest step-ups + storage milestones and writes `results.json`
(each milestone's snapshot, query load, convergence, and the Trino comparison); pass it to `capture.py`
(`--results`) to fold it into the run dir, and `bench/scale/results_table.py results.json` renders the
**milestone×metric table** + the **1 TB / 100k-rec/s extrapolation** (measured vs modeled). Check the
table into the write-up.
