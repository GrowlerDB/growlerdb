# GrowlerDB vs OpenSearch — at-scale comparison benchmark plan

Operational plan and fairness charter for the pre-1.0 head-to-head benchmark. Companion to
[`okf/quality/scale-test-plan.md`](../../okf/quality/scale-test-plan.md) (the GrowlerDB-only scale
suite) and the [`RUNLOG.md`](RUNLOG.md) ledger. Results publish to a new `docs/benchmarks.md` page.

## Objective

Credible, published data on three axes:

1. **Ingest (full initial sync)** — first-class, measured symmetrically: each engine performs a **cold
   full sync of the identical, settled Iceberg table** and we measure **wall-clock time, sustained
   throughput (docs/s), and resource use**. GrowlerDB's streaming changelog connector vs the
   **OpenSearch Data Prepper Iceberg CDC** path. Ingest is as important as query. See the **Ingest
   methodology** section — the load-bearing rule is that generation FINISHES and SETTLES first, then
   each engine syncs the *same static table* (never overlap generation with either engine's ingest,
   which is what an earlier harness did — GrowlerDB streamed during generation while only OpenSearch got
   a clean cold load, so the two were not comparable).
2. **Query latency by query type** — GrowlerDB vs OpenSearch vs the Trino/Iceberg scan baseline.
3. **Query-throughput scaling** — concurrency/QPS sweeps to saturation.
4. **End-to-end freshness** — sentinel-row lag, reported **both under sustained ingest load and at rest**
   (they differ a lot: GrowlerDB's at-rest lag is seconds, but under an ingest burst above the
   connector's throughput ceiling it accumulates — see the ingest-ceiling note in Risks).

Landing these closes the GA "formal at-scale benchmark suite" item and replaces the current
`docs/performance.md` directional numbers where measured ones now exist.

Scope this round is **lexical + autocomplete only**. Vector/kNN and hybrid/RRF are explicitly
deferred to a later round (they need embeddings at scale and a kNN harness on both engines).

## Systems under test (versions pinned in the report)

| System | Role | Ingest path | Notes |
|---|---|---|---|
| GrowlerDB | subject | streaming changelog connector | non-windowed `http_logs` for parity |
| OpenSearch + Data Prepper 2.15 | primary comparison | Iceberg CDC source (snapshot-poll, **CoW-only**, experimental) | `_bulk` fallback if CDC underperforms, **labeled as such** |
| Trino 483 | Iceberg-scan baseline | reads the same table | already wired in `compare_trino.py`; re-baseline from 470 |

Elasticsearch and Quickwit were considered and **cut this round** on cost/scope.

## Dataset

- **Corpus:** a **generated `http_logs`** corpus at **~50 GB uncompressed** (~120–140M rows). No
  permissively-licensed real dataset fit all of log-shaped + commercial-use + 30–100 GB, so the corpus
  is synthetic — but modeled on real web traffic (Zipf path/IP/user popularity, ~87% 200, lognormal
  sizes/latency, diurnal/weekly timestamps, path-conditioned status/method/size). The generation
  methodology + a distribution-validation report are documented and open in
  [`synthetic-corpus.md`](synthetic-corpus.md); the generator is `workloads/http_logs/corpus.py`.
  Seeded → reproducible; parallel generator pods (distinct `BENCH_SEED`) shard to 50 GB.
- **Table:** **Copy-on-Write** Iceberg (mandatory — OpenSearch Data Prepper CDC rejects
  Merge-on-Read at startup), **non-windowed**, hash-routed by `request_id` (the generated primary key).
- **Why non-windowed for the head-to-head:** apples-to-apples (a single evolving logs index is what
  OpenSearch would run; no GrowlerDB-only windowing/cold-tier that OpenSearch can't match);
  conservative for GrowlerDB (no partition-pruning tailwind); and the maintenance CronJob is
  hardcoded to `growlerdb.http_logs` (TASK-340), so a non-windowed run gets **correct compaction**
  while a windowed one currently gets none.
- **GrowlerDB-only windowed addendum:** a separate, clearly-labeled feature demo of windowed
  sub-linear top-K + cold-tier park/revive, where there is no OpenSearch equivalent to compare
  against. Not a head-to-head row.

## Query types (this round)

`match_all`, `term`, **exact-id point lookup** (`trace_id`, the searchable X-Request-ID — the
request-correlation lookup real log search leans on; measured both as a per-type GrowlerDB-vs-OpenSearch
row in the open-loop driver, its value resolved live per engine since seeds vary per pod, and as a pair
against Iceberg's `trace_id` bloom in `compare_trino.py`), `phrase` (match_phrase), `boolean` (bool
must/should/filter), `range`,
**prefix/autocomplete**, and top-K returning documents in three modes: coordinates-only, cached
fields, and full retrieval (GrowlerDB hydrate-from-Iceberg vs OpenSearch `_source`).

**Autocomplete parity (resolved):** whole-value prefix typeahead on `user_id`, each system on its
intended path — GrowlerDB's native `POST /v1/suggest` (a bounded live term-dictionary scan; the
OpenSearch compat adapter has no suggest route) vs an OpenSearch dedicated `completion` FST field.
This is a real architectural asymmetry (GrowlerDB reuses the index; OpenSearch builds an extra
structure), disclosed and reported with the completion field's added storage — like `_source`
-vs-hydrate. The harness hits two different endpoints per system, also disclosed. See
`deploy/k8s/comparison/README.md` and the `autocomplete_user_id` query.

## Metrics

- Per-type latency **p50/p95/p99**, both client-timed and engine-internal (subtract the client→DC
  RTT floor as prior runs did).
- **QPS-vs-latency saturation curves** (open-loop arrival rate; see fairness charter).
- **Ingest — cold full-sync time + sustained throughput (docs/s)** per engine, measured on the same
  settled table (see **Ingest methodology**). Report backfill (this cold sync) and steady-state
  **separately**. GrowlerDB's connector and OpenSearch's Data Prepper are measured the same way.
- **End-to-end freshness** — sentinel-row lag distribution (p50/p99), one clock (below); reported
  **under sustained ingest AND at rest** (they differ materially — see Risks).
- **Storage footprint** + index:source ratio (raw basis) per system.
- **Resource use** under load and ingest, on **all** components incl. the Data Prepper fleet and the
  GrowlerDB connector/nodes during their cold sync.
- **Convergence** — index live-doc count == source `COUNT(DISTINCT request_id)` == source `COUNT(*)`,
  which holds **only if the corpus is dup-free** (see the dup-request_id note in Risks). GrowlerDB
  indexes one doc per source row (doc_count tracks `COUNT(*)`); OpenSearch dedups by `_id`=request_id
  (tracks `COUNT(DISTINCT)`). If the corpus has duplicate request_ids the two engines index different
  counts and convergence can't close — so a dup-free corpus is a prerequisite, not a nice-to-have.

## Ingest methodology (cold full-sync, symmetric)

Ingest is a first-class result, measured identically for both engines against the **same static
table**:

1. **Generate → finish → settle.** Fill the Iceberg table to the target size, stop the generator,
   and let the table settle (final snapshot committed, no in-flight writes). The corpus must be
   **dup-free** (see Risks) so both engines can converge exactly. Optionally run maintenance
   (compaction) once here so both engines read the identical, compacted layout.
2. **Cold full-sync to GrowlerDB — measured.** Bring GrowlerDB up against the settled table from
   empty and measure **wall-clock to converged** (doc_count == `COUNT(*)`), **sustained docs/s**
   (`growlerdb_ingested_docs_total` rate, sampled *during* the sync — not after), peak `ingest_lag_ms`,
   and node/connector resource use. Convergence gate as today.
3. **Cold full-sync to OpenSearch — measured.** Same table, same measurements: Data Prepper CDC from
   empty → `_count` converged; throughput, Data Prepper fleet resource use.
4. **Then queries** (per engine, on the fully-synced index): the query matrix, QPS sweep, and
   freshness, exactly as before.

The load-bearing change vs the earlier harness: **neither engine's ingest overlaps generation.**
Previously GrowlerDB's streaming connector was deployed by `scale-up` and ingested *concurrently with
generation*, so its "convergence time" measured only the tail catch-up, while OpenSearch (brought up
after generation) got a clean cold load — the two ingest numbers were not comparable. Now both start
from the same settled table. Orchestration change: `compare_run` must hold the GrowlerDB connector/node
build until generation settles, then time the cold sync as its own measured phase (mirroring the
OpenSearch phase), before the query matrix.

## Fairness charter

The load-bearing rules that make the numbers defensible. Every place parity is impossible gets
documented in the published report.

1. **Same source.** Identical CoW Iceberg table, catalog, snapshot history, and commit cadence for
   all systems.
2. **Equal total budget, run sequentially.** Each system is benchmarked on the **identical full
   cluster**, one at a time (bring up → ingest → query + QPS matrix → capture → tear down → next),
   not concurrently on shared nodes (which lets one starve the other). This is the cleanest reading of
   "equal budget" at the cost of ingesting 100 GB twice. Data Prepper capacity counts **against**
   OpenSearch's budget, not free on the side. See `deploy/k8s/comparison/README.md`.
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
  cheap Hetzner volume. Est. footprint at ~50 GB raw: Iceberg parquet ~10–25 GB, GrowlerDB index
  ~18–24 GB, **OpenSearch index (with `_source`) ~50–75 GB**, staging/backups ~25 GB — fits the
  ~2,160 GB total local NVMe with headroom. The searchable near-unique `trace_id` adds a large keyword
  term dictionary to **both** indexes (≈ one entry/row) — a disclosed cost, reported alongside latency.

## Phases

- **Phase 0 — pre-flight:** DONE (quota probe, sizing, cost, dataset/hardware decisions).
- **Phase 1 — harness build-out (local/CI, lexical only):** DONE (synthetic schema). Neutral
  open-loop driver (`compare_query.py`), OpenSearch + Data Prepper manifests (`deploy/k8s/comparison/`),
  sentinel-row freshness harness (`compare_freshness.py`), `queries.comparison.json`, concurrency
  sweep, `capture.py` fold-in. Both engines' query paths + the Data Prepper CDC pipeline were
  smoke-verified locally (Polaris + MinIO + OpenSearch, real Iceberg table).
- **Phase 1.5 — generator realism:** DONE. Enhanced `corpus.py` with realistic distributions (Zipf
  path/IP/user, ~87% 200, lognormal sizes/latency, diurnal/weekly time, path-conditioned fields),
  seeded/reproducible; added the methodology doc [`synthetic-corpus.md`](synthetic-corpus.md) and the
  `corpus_stats.py` validation report. Schema/queries unchanged from Phase 1 (the generated corpus
  keeps the `http_logs` shape everything was built against, now 18 fields with the searchable
  `trace_id`).
- **Phase 2 — Run A @ ~50 GB `http_logs`:** orchestrated by `compare_run.py`, sequential per the
  **Ingest methodology**: **generate once → settle (dup-free) → GrowlerDB cold full-sync (MEASURED) →
  query matrix → transition → OpenSearch cold full-sync (MEASURED) → query matrix → finalize.** The
  cold-sync of each engine from the *settled* table is its own timed, throughput-measured phase — not
  a side effect of generation. Load/convergence/freshness drivers run as **in-cluster Jobs** (rendered
  from `deploy/k8s/comparison/driver-job.template.yaml`), never over `kubectl port-forward` — the
  shake-out proved a port-forwarded driver measures the tunnel, not the engine. **10 GB shakedown
  first** (`--scale shakedown`), then `--scale full`. Corpus + result artifacts persist to a Hetzner
  Object Storage bucket. Output: RUNLOG row + `scale-results.md`. See `deploy/k8s/comparison/README.md`.
  **Harness changes still required for this phase order** (as of the 2026-08-27 shakedowns, not yet
  implemented): (a) hold the GrowlerDB connector/node build until generation settles, then time the
  cold sync as a measured phase; (b) sample GrowlerDB `index_rate_dps`/`ingest_lag_ms` *during* the
  sync (capture currently scrapes post-query, reading ~idle); (c) guarantee a **dup-free corpus** (see
  Risks) so convergence closes exactly.
- **Phase 4 — analysis & docs:** new `docs/benchmarks.md` next to Performance (nav_order ~10, bump
  the tail); update `comparison.md` (measured replaces directional), `ga-criteria.md`, `roadmap.md`,
  and OKF `scale-results.md`/`scale-test-plan.md`. Fairness charter summarized on the page. Label
  measured vs modeled and pin the comparison-system versions.

## Cost & timeline

- At ~50 GB (~120–140M rows), a full run (provision → generate/load → both engines → sweeps →
  capture → teardown) is ~12–18 h ≈ **~$60–100** at $75/day. Add a cheap 10 GB shakedown + iteration
  buffer → **program ≈ $100–160 cloud**. First-run friction still applies — budget for a restart or two.
- Engineering: harness ~4–7 working days, then the run, then docs. **~1–2 weeks to published
  numbers** — so an imminent HN post should keep the "directional" framing and land these as a
  follow-up rather than rush.

## Risks & design-arounds

- **OpenSearch Data Prepper Iceberg CDC is experimental and CoW-only.** Fallback: labeled `_bulk`
  load so the ingest comparison still lands. Pin the exact Data Prepper/OpenSearch versions.
- **Compaction CronJob (TASK-340)** targets the non-windowed table — a pro here (non-windowed run
  compacts correctly); the windowed addendum would need a fix first. `scale-up.sh` does not deploy it
  (it lives in the streaming bundle, not observability), so `compare_run.py`'s GrowlerDB phase applies
  `maintenance.yaml` and triggers a one-shot compaction Job (`growlerdb-iceberg-maintenance`) before
  the query matrix, so the fair-Trino source layout is compacted.
- **Locator-heal persistence (TASK-339)** not demonstrably persistent post-compaction — report
  hydration-across-compaction as a measurement, not an assumed invariant.
- **Trino baseline** moves 470 → 483 this round; re-baseline and note it. `compare_trino.py` was
  refreshed for the non-windowed `http_logs` schema (status/user_id/path predicates, no `day` pruning —
  the table is unpartitioned) and runs as a driver Job in the GrowlerDB phase, post-compaction. Its
  headline pair is a **point lookup on `trace_id`** (the searchable X-Request-ID, bloom-filtered):
  GrowlerDB's indexed exact-term lookup vs a Trino equality skipped by the `trace_id` bloom — Iceberg
  at its selective best. (`request_id` stays key-only — identity/`_id`, not a searched term.)
- **Coordinated omission** — see fairness charter #6.
- **Duplicate `request_id`s from concurrent generation (OPEN — blocks exact convergence).** With
  `GENERATORS>1`, parallel pods commit to the one Iceberg branch; under contention pyiceberg's
  optimistic commit conflicts and its *internal* commit retry can re-apply a batch on a false-negative
  catalog response → the same rows land twice. Observed **~1.2–1.5% duplicate request_ids** across the
  2026-08-27 shakedowns. This is NOT the generator replaying (that was a separate, fixed restart-safety
  bug) and it was **NOT fixed** by making the *outer* app-level retry regenerate fresh rows (commit
  `d7fc862` — the duplication is a layer below it, inside pyiceberg). Consequence: source `COUNT(*)` >
  `COUNT(DISTINCT)`, so GrowlerDB (indexes per row) and OpenSearch (dedups by `_id`) index different
  counts and convergence can't close. **Design-around options (pick before the full run):** single-writer
  generation (`GENERATORS=1` — no branch conflicts, so the retry path never fires; slower but the
  Ingest methodology makes generation a separate up-front phase where that's acceptable), or a
  post-generation **dedup pass** (Spark rewrite keeping one row per `request_id`) before the sync
  phases. Do not run `--scale full` until the corpus is dup-free.
- **GrowlerDB connector ingest throughput ceiling (measured ~21–24k docs/s, 6 shards/ccx43).** The
  streaming connector does **not** keep up with a generator burst of ~36k rows/s — it falls behind and
  accumulates lag (~130–150s / ~1M+ rows observed), then drains after the burst. For the benchmark this
  is a *result*, not a bug: report GrowlerDB's cold-sync throughput (~21–24k docs/s) vs OpenSearch CDC
  (~15.7k docs/s at 10 GB) as the ingest headline, and report freshness both under-load and at-rest.
  Worth a separate look at whether the ceiling is the single Spark connector, the nodes' indexing rate,
  or commit cadence — a faster connector would be a real GrowlerDB win to pursue.
- **GrowlerDB `_search`-adapter query throughput (OPEN — blocks publishable query numbers).** At 10 GB
  the in-cluster driver saw GrowlerDB sustain only **~15 qps** flat across 50→800 offered QPS with a
  ~15s client-side p95, while the SAME driver did 800 qps against OpenSearch direct — yet GrowlerDB's
  **engine-internal p95 was ~5ms** (Prometheus). So the ceiling is the serving *path* (the OpenSearch-
  compat `_search` adapter on the gateway, or gateway↔node), and/or querying too soon after ingest+
  compaction while nodes do background segment/locator work. Investigate cheaply before the full run:
  native `/v1/search` vs the `_search` adapter throughput, and a quiesce/warm step before the query
  matrix. Also: `topk_hydrated` uniformly hit the 30s timeout right after compaction — consistent with
  the post-compaction locator-heal risk above; measure hydration-after-compaction explicitly.
