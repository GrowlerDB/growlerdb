# GrowlerDB vs OpenSearch — at-scale comparison benchmark plan

Operational plan and fairness charter for the pre-1.0 head-to-head benchmark. Companion to
[`okf/quality/scale-test-plan.md`](../../okf/quality/scale-test-plan.md) (the GrowlerDB-only scale
suite) and the [`RUNLOG.md`](RUNLOG.md) ledger. Results publish to a new `docs/benchmarks.md` page.

## Objective

Credible, published data on three axes:

1. **Query latency by query type** — GrowlerDB vs OpenSearch vs the Trino/Iceberg scan baseline.
2. **Ingest** — throughput and end-to-end freshness at 100 GB, with the **OpenSearch Data Prepper
   Iceberg CDC** path as the primary comparison.
3. **Query-throughput scaling** — concurrency/QPS sweeps to saturation.

Landing these closes the GA "formal at-scale benchmark suite" item and replaces the current
`docs/performance.md` directional numbers where measured ones now exist.

Scope this round is **lexical + autocomplete only**. Vector/kNN and hybrid/RRF are explicitly
deferred to a later round (they need embeddings at scale and a kNN harness on both engines).

## Systems under test (versions pinned in the report)

| System | Role | Ingest path | Notes |
|---|---|---|---|
| GrowlerDB | subject | streaming changelog connector | non-windowed `http_logs` for parity |
| OpenSearch + Data Prepper 2.15 | primary competitor | Iceberg CDC source (snapshot-poll, **CoW-only**, experimental) | `_bulk` fallback if CDC underperforms, **labeled as such** |
| Trino 483 | Iceberg-scan baseline | reads the same table | already wired in `compare_trino.py`; re-baseline from 470 |

Elasticsearch and Quickwit were considered and **cut this round** on cost/scope.

## Dataset

- **Corpus:** OSB `http_logs`, scaled to **100 GB uncompressed** (~750–800M events). Raw-uncompressed
  basis, matching the OKF convention (index:source ratios reported against uncompressed size, not
  compressed parquet).
- **Table:** **Copy-on-Write** Iceberg (mandatory — OpenSearch Data Prepper CDC rejects
  Merge-on-Read at startup), **non-windowed**, hash-routed by `request_id`.
- **Why non-windowed for the head-to-head:** apples-to-apples (a single evolving logs index is what
  OpenSearch would run; no GrowlerDB-only windowing/cold-tier that OpenSearch can't match);
  conservative for GrowlerDB (no partition-pruning tailwind); and the maintenance CronJob is
  hardcoded to `growlerdb.http_logs` (TASK-340), so a non-windowed run gets **correct compaction**
  while a windowed one currently gets none.
- **GrowlerDB-only windowed addendum:** a separate, clearly-labeled feature demo of windowed
  sub-linear top-K + cold-tier park/revive, where there is no OpenSearch equivalent to compare
  against. Not a head-to-head row.

## Query types (this round)

`match_all`, `term`, `phrase` (match_phrase), `boolean` (bool must/should/filter), `range`,
**prefix/autocomplete**, and top-K returning documents in three modes: coordinates-only, cached
fields, and full retrieval (GrowlerDB hydrate-from-Iceberg vs OpenSearch `_source`).

**Autocomplete parity:** fix ONE OpenSearch approach (e.g. `search_as_you_type` / edge-ngram or a
`match_bool_prefix` field) against GrowlerDB `/v1/suggest` (`suggest_prefix`/`suggest_fuzzy`), and
disclose it — field-type choice materially changes results, so it must be pinned, not tuned per run.

## Metrics

- Per-type latency **p50/p95/p99**, both client-timed and engine-internal (subtract the client→DC
  RTT floor as prior runs did).
- **QPS-vs-latency saturation curves** (open-loop arrival rate; see fairness charter).
- **Ingest throughput** — docs/s, backfill vs steady-state measured **separately**.
- **End-to-end freshness** — sentinel-row lag distribution (p50/p99), one clock (below).
- **Storage footprint** + index:source ratio (raw basis) per system.
- **Resource use** under load and ingest, on **all** components incl. the Data Prepper fleet.
- **Convergence** — index live-doc count == source `COUNT(DISTINCT request_id)` (Trino, dup-safe).

## Fairness charter

The load-bearing rules that make the numbers defensible. Every place parity is impossible gets
documented in the published report.

1. **Same source.** Identical CoW Iceberg table, catalog, snapshot history, and commit cadence for
   all systems.
2. **Equal total budget.** Same aggregate CPU/RAM/disk given to each system; each partitions it as
   it likes; report per-unit-work, not per-node. Data Prepper capacity counts **against** OpenSearch's
   budget, not free on the side.
3. **Config parity.** Same analyzers/tokenizers per field, same shard/replica counts, matching field
   types. `refresh_interval` is **fixed and disclosed** (raising it lifts bulk throughput but worsens
   freshness — a disclosed trade, not a per-run tuning knob).
4. **Freshness on one clock.** End-to-end lag = wall-clock from source Iceberg commit until a query
   returns that row, measured identically by sentinel rows. OpenSearch's lag legitimately stacks
   snapshot cadence + `polling_interval` + `refresh_interval`; GrowlerDB's is its streaming commit
   path. Never compare OpenSearch internal-refresh-only against GrowlerDB end-to-end.
5. **`_source` vs hydrate is a first-class result, not hidden.** OpenSearch stores a full `_source`
   copy (more storage, no Iceberg round-trip on fetch); GrowlerDB stores keys + index and hydrates.
   Measure **both** index-only (IDs/scores) and full-document top-K, and report **storage footprint**
   alongside latency. Do not silently disable `_source` for "parity"; if disabled, disclose it as a
   changed operating mode.
6. **Open-loop load.** Drive QPS at a fixed arrival rate (k6/vegeta) to avoid coordinated omission.
   If OpenSearch Benchmark is used for OS-native metrics, set a **non-zero `target-throughput`** and
   read `latency` (its `target-throughput: 0` mode hides tail latency); report both `latency` and
   `service_time`.
7. **Warmup + cache handling** identical across systems; enough iterations for stable p99; pin JVM
   heap/GC for the OpenSearch/Trino side.
8. **Pinned, disclosed versions.** OpenSearch + Data Prepper (experimental status noted), Trino 483,
   GrowlerDB image SHA.

## Hardware & sizing

- **6× ccx43** (16 dedicated vCPU / 64 GB / 360 GB local NVMe each) in **nbg1** = **96 dedicated
  cores** (matches the requested quota exactly). $0.5216/hr/node ≈ **$75/day** cluster.
- Probe (2026-08-24): 1× ccx43 provisioned + destroyed cleanly, no quota error → dedicated quota is
  ≥16 cores. The full 96-core headroom is confirmed at `terraform apply` (fails fast, no real spend).
- Index data (GrowlerDB Tantivy segments, OpenSearch shards) on **local NVMe**; MinIO/Iceberg on a
  cheap Hetzner volume. Est. footprint at 100 GB raw: Iceberg parquet ~10–30 GB, GrowlerDB index
  ~36–47 GB, **OpenSearch index (with `_source`) ~100–150 GB**, staging/backups ~50 GB — fits in the
  ~2,160 GB total local NVMe with headroom.

## Phases

- **Phase 0 — pre-flight:** DONE (quota probe, sizing, cost, dataset/hardware decisions).
- **Phase 1 — harness build-out (local/CI, lexical only):** neutral open-loop query driver across
  GrowlerDB + OpenSearch + Trino; OpenSearch + Data Prepper k8s manifests under an equal-budget
  profile (`deploy/k8s/competitors/`); sentinel-row freshness harness; expanded lexical `queries.json`
  incl. the autocomplete shape; concurrency-sweep runner; competitor metrics folded into
  `capture.py`. Smoke on Compose at small scale before any cloud run.
- **Phase 2 — Run A @ 100 GB:** provision → ingest all systems → convergence → query-type latency
  matrix → QPS sweeps → storage → capture + RUNLOG row + `scale-results.md` update.
- **Phase 4 — analysis & docs:** new `docs/benchmarks.md` next to Performance (nav_order ~10, bump
  the tail); update `comparison.md` (measured replaces directional), `ga-criteria.md`, `roadmap.md`,
  and OKF `scale-results.md`/`scale-test-plan.md`. Fairness charter summarized on the page. Honest
  labeling throughout (measured vs modeled, experimental competitor versions, every caveat).

## Cost & timeline

- One multi-day run this round. Cluster $75/day; big run ~1.5–2 days ≈ $110–150; volumes + debug +
  smoke ≈ $30–50. **Program ≈ $150–200 cloud.**
- Engineering: harness ~4–7 working days, then the run, then docs. **~1–2 weeks to published
  numbers** — so an imminent HN post should keep the "directional" framing and land these as a
  follow-up rather than rush.

## Risks & design-arounds

- **OpenSearch Data Prepper Iceberg CDC is experimental and CoW-only.** Fallback: labeled `_bulk`
  load so the ingest comparison still lands. Pin the exact Data Prepper/OpenSearch versions.
- **Compaction CronJob (TASK-340)** targets the non-windowed table — a pro here (non-windowed run
  compacts correctly); the windowed addendum would need a fix first.
- **Locator-heal persistence (TASK-339)** not demonstrably persistent post-compaction — report
  hydration-across-compaction as a measurement, not an assumed invariant.
- **Trino baseline** moves 470 → 483 this round; re-baseline and note it.
- **Coordinated omission** — see fairness charter #6.
