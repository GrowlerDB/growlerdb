# GrowlerDB vs OpenSearch — query-latency decomposition & optimization analysis

Evidence-based teardown of the 2026-08-29 head-to-head (`dev-868f826`, 10 GB non-windowed
`http_logs`, 6× ccx43 nbg1, single MinIO pod). Companion to [`comparison-plan.md`](comparison-plan.md)
(fairness charter) and [`RUNLOG.md`](RUNLOG.md). Sources: `gdb-query.json`, `os-query.json`, the
GDB-only low-concurrency probe (RUNLOG 2026-08-29), and the engine code paths cited inline.

**Scope note on which number is which.** The driver records two clocks (`compare_query.py:129`):
`service_ms` = send→recv (the engine + gateway + LAN, driver runs as an in-cluster Job so RTT is
sub-ms), and `latency_ms` = schedule→recv (adds open-loop queue wait / coordinated omission). **This
analysis uses `service_ms` throughout** — the per-request engine cost. `latency_ms` is discussed only
in §Contamination, because under load it is dominated by the driver's own worker-pool queue, not the
engine.

---

## TL;DR — four mechanisms, ranked by impact

1. **Serial file reads inside a shard's hydration (dominant, latency).** `scan_stale_index` read matching
   row-groups in a plain `for` loop, one file at a time. For `topk_hydrated` the hits' `ts` values are
   scattered across the 7-day sorted layout → ~20 distinct row-groups spread over ~13 compacted files →
   the shard reads those files as serial object-store round-trips. CPU is idle; round-trip-latency-bound.
   **Prototyped & fixed** (`scan_stale_index_conc`, `buffered` file reads): **5.95–6.41× faster** on a
   12-file scattered top-k with injected 15 ms/read latency (see *Prototype results* below).
2. **Two catalog `load_table` REST calls per hydration, per shard (latency, small).** The old
   `attach_prune_hints` read `sort_field_names` fresh (a separate `load_table`), then `hydrate →
   load_and_plan` loaded the table again — two serial catalog round-trips before any data byte.
   **Prototyped & fixed** (`hydrate_pruned`): the sort-order names now come off the *same* table load
   the scan uses → **one `load_table` per hydration, not two**.
3. **Scatter-gather floor + client-pool contamination (index-only).** Every query fans out to 6 nodes
   over gRPC and waits for the slowest (`gateway.rs:1020-1034`); the merge is cheap. This adds a
   ~3–8 ms floor over OpenSearch even for `match_all`. Separately, the driver's single 64-worker pool
   is shared across all query types, so the two multi-second topk types starve it and inflate the
   *reported* throughput collapse (a harness artifact, quantified below).
4. **Fundamental `_source`-vs-hydrate tax + single MinIO.** OpenSearch serves `_source` from local
   Lucene; GDB re-reads Iceberg from object store, and every hydrated hit is a GET against one MinIO
   pod on one volume. Mechanisms 1–2 cut the per-request serial cost and the total GET count, but the
   residual — object-store round-trip latency × row-groups/hit, throughput-capped by one pod — is
   structural. Target: materially better than the 19.5 s under-load / sub-second at rest, not beating
   OpenSearch's 10.7 ms.

> **Correction (verified in code during prototyping).** An earlier draft of this analysis claimed a
> **6× over-hydration** — that each of the 6 shards hydrated its local top-20, so a 20-row answer cost
> ~120 Iceberg reads. **That was wrong.** `Gateway::search` (`gateway.rs:869`) **strips** the hydrate
> flag before the shard scatter, merges to the global top-20, then hydrates that page **once** via
> `hydrate_hits → get_by_key_unadmitted` (`gateway.rs:1524,1651`), which groups the 20 keys by owning
> shard and scatters each subset **concurrently**. So GDB already does query-then-fetch: ~20 hydrations
> total, ~3–4 per shard, in parallel — **no over-hydration.** The original Option 1 ("add query-then-fetch")
> is therefore already implemented; the real levers are 1 and 2 above, both now prototyped.

---

## Master table — all conditions, both engines, same driver/queries

`service_ms` p50 / p99. GDB@200 and OS@200 from `gdb-query.json`/`os-query.json` (both offered 200 qps
+ sweep 50→800). OS achieved 199.8/200, 0 dropped, and is flat across the whole sweep → **OS@200 is
also OS at low concurrency** (it never saturates here). GDB@10 is the separate GDB-only probe (fresh
compacted table). GDB has no clean low-conc p99 published; "—" where not measured (not backfilled).

| query | kind | OS p50 | OS p99 | GDB@10 p50 | GDB@200 p50 | GDB@200 p99 | GDB@200 ÷ GDB@10 |
|---|---|---|---|---|---|---|---|
| point_lookup_trace_id | term (size 0) | 4.8 | 8.5 | 5.7 | 11.8 | 63.1 | 2.07× |
| term_user_id | term | 4.8 | 9.7 | 5.7 | 11.9 | 77.4 | 2.09× |
| cidr_client_ip | ip_cidr | 4.9 | 9.4 | 7.0 | 12.4 | 121.7 | 1.77× |
| match_all_count | count | 4.8 | 13.7 | 7.8 | 12.5 | 57.6 | 1.60× |
| autocomplete_user_id | suggest¹ | 7.9 | 22.4 | 12.9 | 14.8 | 60.9 | 1.15× |
| match_user_agent | match | 4.9 | 8.4 | 12.4 | 15.1 | 147.1 | 1.22× |
| bool_should | boolean | 5.1 | 9.3 | 25.0 | 27.3 | 133.8 | 1.09× |
| term_status | term | 4.8 | 12.4 | 25.2 | 24.2 | 67.9 | 0.96× |
| term_method | term | 4.8 | 12.1 | 25.6 | 25.0 | 150.1 | 0.98× |
| range_response_size | range | 4.9 | 9.3 | ~32 | 32.8 | 49.9 | ~1.02× |
| range_response_time | range | 4.9 | 9.6 | 31–32 | 33.7 | 60.0 | ~1.06× |
| phrase_path | phrase | 4.9 | 8.8 | 35.9 | 38.2 | 53.8 | 1.06× |
| bool_must_filter | boolean | 5.3 | 8.3 | 76.7 | 76.4 | 200.1 | 1.00× |
| topk_recent | retrieval (size 20) | 11.3 | 33.2 | 431 | 3645.1 | 6414.3 | 8.46× |
| topk_hydrated | retrieval (size 20) | 10.7 | 20.9 | 1153 | 19516.7 | 27893.7 | 16.9× |

¹ autocomplete is a disclosed endpoint asymmetry (GDB native `/v1/suggest` vs OS `completion` FST) —
different paths by design, per the charter.

**The single most important reading:** the index-only rows are **robust under load** (≤2×, and the
worst absolute p50 — bool_must 76 ms — is *flat* from 10→200 qps). Only the two hydrated top-k rows
blow up (8–17×). The headline "GDB collapses to 31 qps / 19,171 dropped" is **entirely** the two topk
types; with them isolated, GDB serves the other 13 types at 12–76 ms with 0 errors even at 200 qps
offered.

---

## Decomposition by family

### Family A — selective index-only (point_lookup, term_user_id, cidr, match_all): 6–12 ms

At 10 qps these sit at 5.7–7.8 ms — within ~1–3 ms of the engine-internal
`growlerdb_query_retrieval_duration_seconds` (~5 ms). The gap to OpenSearch's ~4.8 ms is the
**scatter-gather floor**: `match_all` (a pure count, no matching work) is GDB 7.8 ms vs OS 4.8 ms, so
the 6-way gRPC fan-out + await-slowest + merge + adapter costs **≈3 ms at rest, ~6–8 ms under load**.
There is no hydration on these (all `size:0` → `wants_hydration=false`, `opensearch.rs:506-507`).

Decomposition (10 qps): LAN RTT <1 ms · adapter DSL→Lucene translation ~sub-ms · gateway route + 6-way
fanout + merge ~3 ms · per-shard Tantivy retrieval ~5 ms (parallel across shards, so wall-clock ≈ one
shard + slowest-of-6 tail).

### Family B — broad / expensive index-only (term_status, term_method, range, phrase, match_ua, bool_*): 12–77 ms

These are dominated by **per-shard query execution cost**, not fan-out. `term_status` matches ~87% of
docs (`status:200`), `bool_must_filter` is a conjunction of a `match` + two filters over large posting
lists → 76 ms. The fan-out floor (~3–8 ms) is a minority of the total here. Evidence they are
engine-CPU-bound, not I/O-bound: GDB@200÷GDB@10 ≈ 1.0 for every row in this family — **flat under
load** on a 96-core cluster (CPU is not the scarce resource; there's no I/O in this path). OpenSearch's
~5 ms on the same predicates reflects Lucene's warmed count caches / skip lists; GDB's tens-of-ms is
real retrieval work, an engine optimization target but **not** a correctness or object-store issue, and
already comfortably sub-100 ms.

### Family C — hydrated top-k (topk_hydrated, topk_recent): the object-store path

This is where the time actually goes. Per-request wall-clock at 10 qps ≈ **slowest-of-6 shard
hydrations of its share of the merged top-20** (the query-then-fetch path in the Correction above:
`get_by_key_unadmitted` groups the 20 keys by owning shard and scatters each subset concurrently,
`gateway.rs:1724-1763`). Engine-internal `growlerdb_hydration_duration_seconds` post-compaction is p50
406 / p95 1530 / p99 2300 ms *per shard call*; the tail over the shards that hold keys lands the driver's
`topk_hydrated` at ~1153 ms — the 406→1153 gap is the slowest-owning-shard tail plus that shard's serial
file reads (mechanism 1).

Why `topk_hydrated` (1153 ms) ≫ `topk_recent` (431 ms) at the same 10 qps, both hydrating 20 rows:
**how scattered the hits' sort-key values are across the `ts`-sorted layout.**
- `topk_recent` = the 20 *most recent* `status:500` (sort `ts` desc) → their `ts` cluster at the end of
  the range → ~1 row-group → few serial reads → 431 ms.
- `topk_hydrated` = the 20 highest `response_time_ms` `Chrome`/`200` (sort `response_time_ms` desc) →
  their `ts` is essentially random → ~20 distinct row-groups → ~20 serial reads → 1153 ms.

This is D54 working as designed (a sort-clustered table reads ~1 row-group/hit) — the cost is
`(row-groups touched) × (serial single-MinIO round-trip)`, and `topk_hydrated`'s hits are maximally
scattered.

Per-shard hydration breakdown (structural, from the code path), **before → after the prototype**:
`load_table` for `sort_field_names` (1 catalog REST) **[removed by mechanism-2 fold]** → `load_and_plan`
`load_table` (1 catalog REST, plan cached per snapshot) → **predicated `plan_files`** to prune by `ts`
min/max (manifest read, not cached) → **N row-group reads** (footer/page-index + data bytes) —
**was serial, now `buffered(8)` (mechanism-1 fix)** → decode + key-verify (CPU, small). For scattered
keys the N reads dominate; the second catalog call is now the only fixed per-hydration REST tax.

---

## Contamination (question 7), quantified

The plan weights sum to 32. At 200 qps offered, per second: `topk_hydrated` = 3/32 ≈ **18.8 req/s ×
19.5 s** = 366 worker-seconds; `topk_recent` = 2/32 ≈ **12.5 req/s × 3.6 s** = 45 worker-seconds. The
two topk types alone demand **≈411 concurrently-held workers per second** against a pool of
`max_workers=64` (`compare_query.py:293`). The pool is therefore **permanently saturated by topk**;
every other request waits in the executor queue. That queue wait is the entire story of `latency_ms`
p50 = 62 s (coordinated omission), and it caps achieved throughput at the pool's drain rate → the
reported **31.1 qps / 19,171 dropped**.

Crucially, `service_ms` for the index-only types stays low (they inflate ≤2×, the sub-10 ms types
picking up ~6 ms of gateway/MinIO contention) because once a worker is free the engine serves them
fast. **So the throughput collapse is a shared-pool measurement artifact layered on a real
object-store-throughput limit** — the engine's index-only path is not the bottleneck. Isolating topk
(separate pool or separate pass) removes the artifact; the remaining topk cost is the genuine ~20-GET
object-store load per query (query-then-fetch hydrates only the merged top-20, distributed across shards).

---

## Ranked optimization options

Legend: **[cheap]** config/code, low risk · **[med]** real code, contained · **[fund]** fundamental
(`_source`-vs-hydrate); target is *materially better*, not beating OpenSearch.

### topk_hydrated / topk_recent (target <1 s at low conc — currently 1.15 s / 431 ms; and un-collapse under load)

| # | option | expected impact | basis | cost/risk | status |
|---|---|---|---|---|---|
| 1 | **Query-then-fetch: hydrate the merged global top-k once, not per-shard.** | Would avoid over-hydration — but **already how GDB works** (`gateway.rs:869` strips hydrate, `hydrate_hits`/`get_by_key_unadmitted` hydrate the merged 20, distributed across owning shards). ~20 GETs/query, not 120. | Verified in code (see Correction). | — | **already implemented** — no change. |
| 2 | **Parallelize the per-shard file reads inside `scan_stale_index`** (`buffered(8)` over the plan's files, byte budget honored between completed files). | **Measured 5.95–6.41×** on a 12-file scattered top-k with 15 ms/read injected latency (721–821 ms serial → 122–128 ms). At-rest `topk_hydrated` ~1.15 s → est. ~0.2–0.4 s; little help for `topk_recent` (few files). | Reads are single-MinIO round-trips, CPU idle. Local probe = upper bound; real gain capped by MinIO concurrent-GET ceiling. | **[cheap]** loses ordered early-exit (bounded over-read of ≤7 extra files); byte budget still bounds. | **prototyped** (`scan_stale_index_conc`); 37/37 source + 350 engine tests green. |
| 3 | **Fold the two `load_table` REST calls into one** — resolve the sort-order names off the table the scan already loads. | 2 → 1 catalog REST per hydration per shard (~5–20 ms cluster tax removed). | The old `attach_prune_hints` did a separate `sort_field_names` `load_table`. | **[cheap]** by construction; correctness covered by existing hydrate tests. REST-count delta measurable only against a live catalog. | **prototyped** (`hydrate_pruned` + `prune_hint_values`); inline_hydration + lookup_service tests green. |
| 4 | **(Harness) give topk its own worker pool / raise `max_workers`.** | Removes the throughput-collapse artifact so the *engine's* topk behavior is what's measured. | §Contamination: 64-worker shared pool, 411 worker-s/s demand. | **[cheap]** driver-only; measurement fidelity, not engine speed. | open (re-measurement plan). |
| — | **Fundamental floor.** After 2–3, topk cost ≈ `(row-groups/hit) × (MinIO round-trip)`. A distributed/real object store lowers and parallelizes that latency; single-pod single-volume caps concurrent GETs. Cannot approach OS's 10.7 ms (local `_source`), but sub-second at rest and no under-load collapse are reachable. | [fund] | D54; single MinIO pod. | — | — |

### Index-only broad (bool_must 76, phrase 38, range 33, term 24–25): engine-CPU

| # | option | expected impact | basis | cost/risk |
|---|---|---|---|---|
| 5 | Warm/precompute count paths; profile Tantivy count over high-cardinality postings (`term_status` 87% selectivity). | OS does these in ~5 ms → headroom exists; est. 2–4× on the broad terms. Needs a profile before committing a number. | Flat under load (CPU-bound, not I/O); GDB@200÷GDB@10 ≈ 1.0. | **[med]** engine work; lowest priority — already <100 ms, 0 err, robust. |
| 6 | Trim the scatter-gather floor (gRPC fan-out overhead vs OS ~5 ms baseline). | ~3–8 ms off every type. | `match_all` GDB 7.8 vs OS 4.8 isolates the floor. | **[med]** broad blast radius; measure first. |

### Index-only selective (point_lookup, term_user_id, cidr, match_all): 6–12 ms

| # | option | expected impact | basis | cost/risk |
|---|---|---|---|---|
| 7 | Single-shard fast-path when the predicate pins one shard. | 12 → ~6 ms. | Skips 6-way fanout. | **[med]** but `trace_id`/`user_id` are **not** the routing key (`request_id` is) → can't pin → **not applicable** here. Documented as a non-lever. |

These are already ≈ OpenSearch + the unavoidable fan-out floor; **no action recommended** — they are
not the problem the <1 s target is about.

---

## Prototype results (local, no cluster spend)

Both cheap levers were implemented and validated locally (`cargo test`, fs-backed Iceberg fixtures, a
latency-injecting `FileIO`; no Docker). The fixture generator is `crates/growlerdb-source/tests/fixtures/
gen_multifile_prune.py` (12 data files, one scattered `(request_id AND ts)` target per file, so the top-k
plan selects every file — the scattered worst case).

**Mechanism 1 — parallel file reads.** Test `parallel_file_reads_beat_serial_hydration`
(`growlerdb-source`, `--ignored`) reads the 12-file plan with 15 ms injected per object-store read,
three strategies, asserting identical rows found:

| strategy | time | speedup vs serial | rows |
|---|---|---|---|
| serial (one task per reader — the original code) | 736 ms | 1× | 12/12 |
| `buffered(8)` at the growlerdb layer (`scan_stale_index_conc`) | 119 ms | **6.17×** | 12/12 |
| native iceberg `with_data_file_concurrency_limit(8)` (all tasks → one reader) | 116 ms | **6.37×** | 12/12 |

**Key finding: the two concurrent strategies are equal, and iceberg-rust already parallelizes across
files natively** — `ArrowReader::read` fans out over the task stream with
`try_buffer_unordered(concurrency_limit_data_files)` (default = num_cpus, `arrow/reader/pipeline.rs`).
Every growlerdb Iceberg read path **defeated** this by feeding **one task per `read()`** in a `for`
loop, so the limit never engaged. Two production fixes landed from this:
- **Hydration key scan** keeps the growlerdb `buffered(N)` form: it reads one task per future, so it
  retains each batch's `(file, position)` provenance — needed for `scan_stale_index`'s deterministic
  duplicate-PK winner. The native merged stream loses provenance (it can't attribute a batch to a file).
- **Plain full scan** (`read_tasks`, used by `read_current` → the large multi-partition scan the
  question is about) switched to the **native** path — all tasks to one reader with
  `with_data_file_concurrency_limit(8)` — since row→document mapping needs no provenance. This is the
  idiomatic fix and directly benefits a scan fanning across many partitions/large files.

The 15 ms/read with unbounded local-disk parallelism makes the speedup an **upper bound**; on one MinIO
pod the real gain is capped by its concurrent-GET ceiling (see *Where hydration parallelism can live*).
Correctness: the single-file row-group-prune test (`rowgroup_bytes`, still 5 of 40 row groups) and all
37 `growlerdb-source` + 350 `growlerdb-engine` unit tests pass unchanged.

**Mechanism 2 — one `load_table` per hydration (`hydrate_pruned` + `prune_hint_values`).** The engine's
two hydration call sites (`hydrate.rs` `get_by_key`, `lookup_service.rs`) now pass a closure that derives
prune values from the sort-order names read off the *same* table `hydrate_pruned` loads, deleting the
separate `sort_field_names` `load_table`. Correctness covered by the query-then-fetch gateway test
(`inline_hydration`, 5/5) and `lookup_service` (9/9); the REST-count delta (2→1) is by construction.

**Also shipped (node-layer, production).** The concurrent read is the default hydration path, with
env-tunable knobs so an operator matches concurrency to the object store's GET headroom:
`GROWLERDB_HYDRATE_FILE_CONCURRENCY` (across-file, default 8), `GROWLERDB_ICEBERG_RANGE_FETCH_CONCURRENCY`
(within-file column-chunk fetches, default 4); large plain scans (`read_tasks`) use iceberg's native
`concurrency_limit_data_files`. And the benchmark's Iceberg store is now **switchable** off the single
MinIO pod onto the same nbg1 Hetzner Object Storage bucket (`S3_PROFILE=hetzner`), so the under-load
hydration number can be taken against a real, in-region store instead of the single-pod artifact.

## Measured at scale — Hetzner run (2026-08-29, 10 GB, 6× ccx43 nbg1, image dev-a9177ee)

First full run on a real, in-region object store (Hetzner OS, `S3_PROFILE=hetzner`). Every Hetzner
client path validated (generator write, Data Prepper read, Spark connector read, engine hydrate;
convergence EXACT both engines, `sample_integrity: PASS`).

**topk_hydrated, at rest (10 qps, mixed, 0 err) — the objective (`<1 s`) is met:**

| | old single-MinIO 10 qps | **Hetzner 10 qps (this run)** |
|---|---|---|
| topk_hydrated p50 / p99 | 1153 ms | **437 / 525 ms** (2.6×) |
| topk_recent p50 / p99 | 431 ms | **239 / 295 ms** |

Index-only at rest unchanged/robust (point_lookup 4.8, term 21, bool_must 75 ms, 0 err). So Hetzner +
concurrent reads put per-request hydration **well under the 1 s target**.

**Under load (200 qps mixed + 50→800 sweep) — still tail-bound:** topk_hydrated **6.6 s p50** (was 19.5 s
on single-MinIO, ~3×) but **p99 ≈30 s with 8 client timeouts**; achieved 52 qps (was 31). The errors are
**all 30 s client timeouts, zero Hetzner-side 5xx/throttle** — Hetzner is healthy. The tail is a
**concurrency pile-up**: engine-internal hydration is p50 5.2 s / p95+ ≥10 s under load (vs ~0.4 s at
rest) because ~30 topk req/s × `buffered(8)` each = hundreds of concurrent GETs flood the store and the
nodes. Index-only retrieval stayed clean (internal p50 16 ms). Freshness on Hetzner: p50 2.55 s, 0
timeouts (the driver-S3 fix).

**Tail fix shipped (this round):** a node-wide cap on concurrent object-store reads across ALL in-flight
hydrations — `read_conc::INFLIGHT_READS`, `GROWLERDB_HYDRATE_MAX_INFLIGHT_READS` (default 32) — so a
top-k burst can't flood the store; requests queue for a slot and complete in bounded time. Correctness
tested (48 wanted reads bounded to 32, all keys still found); **its p99 impact needs the next run to
measure.** Remaining: tune the knobs on a real store (a sweep — needs a run), the 10 s engine hydration
ceiling, and isolate topk in the driver (a dedicated pool) so index-only throughput isn't starved.

### Follow-up round — local (no cluster spend): the 10 s "ceiling" resolved, driver isolated, table reuse

Three changes landed locally (gate-green; the two measurement changes are prerequisites for the knob
sweep to be readable), before the next cluster session:

- **The 10 s engine hydration ceiling is a metrics artifact, not a deadline (resolved by code-read).**
  There is no 10 s timeout in the hydration path: `hydrate.rs` has none, `source/lib.rs` bounds the key
  scan by a 256 MiB **byte budget** (not time), and the only real deadlines are the node channel
  `REQUEST_TIMEOUT = 30 s` and `per_attempt = REQUEST_TIMEOUT / candidates` (30 s single-primary, 15 s
  with one replica) — matching the observed 30 s client timeouts and the 30 s gateway scatter deadline.
  The flat p95/p99 = 10000 ms was **Prometheus right-censoring**: the shared `_duration_seconds` bucket
  set (`telemetry/lib.rs`) topped out at a largest finite boundary of **10.0 s**, so any request slower
  than 10 s fell in `+Inf` and `histogram_quantile` reported the last finite boundary (10.0). It masked
  how bad the under-load tail is (the real engine-internal p95/p99 is between 10 s and the 30 s timeout);
  it did **not** cut real requests. **Fix:** extended the buckets to `15, 20, 30 s` so the tail is
  observable — a prerequisite for reading the knob sweep from the engine histogram (a p99 moving 28 s→15 s
  was invisible before, both pinned at 10000 ms). Driver `service_ms` already saw the real tail; the
  decomposition (retrieval vs hydration) did not.
- **Driver: topk isolated + a symmetric low-concurrency pass (re-measurement plan steps 1–2).**
  `compare_query.py` now runs a **pass matrix**: each query **group** — `index` (index-only + autocomplete,
  no hydration), `topk` (the two hydrated retrievals), `mixed` (all, the realistic-throughput number) —
  runs as its **own** open-loop pass with its **own** executor, so topk can no longer starve the shared
  64-worker pool and inflate index-only's reported throughput (§Contamination). `--low-qps` adds an
  at-rest pass per group, so **at-rest and under-load are captured in the same run for both engines**.
  `compare_run.py` drives `--groups index,topk,mixed --low-qps 10` for both GrowlerDB and OpenSearch.
  Validated locally against a mock engine (a 300 ms topk sleep leaves the isolated `index` pass at p99
  ~10 ms while the `mixed` pass shows the ~300 ms contamination — the artifact, reproduced and removed).
- **Table reuse (`--reuse-table`) for cheap GDB-only iteration.** The persisted Hetzner corpus survives
  teardown (`S3_PROFILE=hetzner`, left compacted). `--reuse-table` registers the latest persisted
  `metadata.json` into the fresh Polaris (skips the ~20 min generation and, since it is already compacted,
  the compaction), turning `--phase all` into `register → gdb_coldsync → growlerdb → finalize` (GDB-only —
  OpenSearch's Data Prepper can't ingest the compacted layout). Latest-metadata discovery + the register
  path verified against the live bucket (read-only) and offline.

### Measured at scale — cluster run (2026-08-29, reused corpus, image dev-3a2f977)

GDB-only run on the persisted, compacted **25,425,380-row** `http_logs` table (registered via
`--reuse-table` — no regeneration; sort order `[ts, request_id]` declared so the ts prune hint fires),
6× ccx43 nbg1, `S3_PROFILE=hetzner`, image `dev-3a2f977` (histogram buckets to 30 s + in-flight cap
default 32). Index boot-built (not connector-backfilled), so the autocomplete completion sidecar was
**not** built — `autocomplete_user_id` runs the live term-dict fallback (178 ms p50, disclosed below),
not the ~15 ms sidecar path. OpenSearch was **not** run this round (GDB-only reuse); no head-to-head row.
Each config is its own isolated open-loop pass (`compare_query.py` groups). `service_ms` throughout.

**Task (d), confirmed live.** With the extended buckets the engine `growlerdb_hydration_duration_seconds`
now reports the tail instead of pinning at 10000 ms. Per config, under the isolated topk pass
(200 qps offered), engine-internal hydration p50 / p95 / p99 (ms):

| config (FILE,RANGE,INFLIGHT) | hyd p50 | hyd p95 | hyd p99 |
|---|---|---|---|
| 8,4,100000 (no-cap) | 5063.9 | 12202.7 | 14937.6 |
| 8,4,16 | 2157.9 | 9600.3 | 13453.2 |
| 8,4,32 (default) | — | — | — ¹ |
| 8,4,64 | 1661.8 | 9979.9 | 14465.9 |
| 4,2,32 | 2169.1 | 11675.3 | 14807.3 |
| 16,4,32 | 919.1 | 12284.0 | 14882.8 |
| 8,8,32 | 981.4 | 9926.5 | 14247.2 |

¹ the default-config engine histogram was scraped over a window spanning the low+high passes (p50 925.6
/ p95 10713.6 / p99 14764.1); every p95/p99 above sits between 9.6 s and 14.9 s — the real tail, formerly
censored flat at 10000 ms.

**Task (a) — in-flight cap sweep. `topk_hydrated` under load (isolated topk pass, 200 qps offered):**

| INFLIGHT | th p50 | th p99 | err | qps | tr p50 | tr p99 |
|---|---|---|---|---|---|---|
| 100000 (no-cap) | 835.6 | 13635.9 | 0 | 30 | 28.0 | 173.2 |
| 16 | 1834.5 | 13785.8 | 0 | 31 | 26.6 | 181.0 |
| 32 (default) | 971.7 | 13847.9 | 0 | 30 | 14.9 | 68.1 |
| 64 | 815.0 | 13700.4 | 0 | 30 | 28.2 | 148.5 |

**The cap does not move the p99 tail** — every value lands at 13.6–13.9 s, 0 err, ~30 qps. It changes
only p50, and **too-tight (16) nearly doubles it** (1834 vs 815–972 ms); 32 / 64 / no-cap are within
~150 ms of each other. The under-load errors of the prior run (8× 30 s client timeouts) are **not** the
cap's doing — they were the shared-pool coordinated omission (a slow topk holding 64 workers), removed by
the driver isolation. **The cap is not the tail lever on a real object store.**

**Task (b) — read-concurrency knob sweep. `topk_hydrated` under load (isolated topk pass, 200 qps offered):**

| FILE | RANGE | INFLIGHT | th p50 | th p99 | err | qps |
|---|---|---|---|---|---|---|
| 8 | 4 | 32 (default) | 971.7 | 13847.9 | 0 | 30 |
| 4 | 2 | 32 | 910.1 | 13852.8 | 0 | 30 |
| 16 | 4 | 32 | 837.6 | 13884.0 | 0 | 29 |
| 8 | 8 | 32 | 1043.9 | 13515.3 | 0 | 30 |

Same story: **p99 is invariant (13.5–13.9 s) across every FILE×RANGE×INFLIGHT setting**; p50 jitters within
~200 ms (16,4 and 64 marginally lowest, 8,8 marginally highest — more within-file range concurrency does
not help). There is **no per-request-latency-vs-tail balance to strike** on this store: the tail is the
Hetzner-OS concurrent-GET ceiling (~30 topk qps), which no read-concurrency knob moves. Default (8,4,32)
is near-optimal; the only actionable finding is *do not lower the cap to 16*.

**At rest (10 qps low-conc pass) — knob-independent.** `topk_hydrated` p50 / p99 (ms), all seven configs:
310–320 / 376–485; `topk_recent` 14–29 / 22–35; 0 err throughout. Sub-second at rest regardless of knobs
(the cap/concurrency never engages at 10 qps). Index-only at 10 qps: 5–80 ms p50 except autocomplete 202 ms.

**The realistic mixed workload does not collapse (200 qps offered, one shared pool, weighted plan):**

| group | achieved qps | dropped | topk_hydrated p50/p99 | index-only p50 range | err |
|---|---|---|---|---|---|
| mixed | 189 | 0 | 314.0 / 6342.4 ms | 2.7–70.6 ms | 0 |

At its realistic 5/32 weight topk runs at ~30 qps (its store ceiling, not over-driven), so it doesn't
saturate the pool: the mix serves **189 qps with 0 drops**, index-only 2.7–70 ms, topk_hydrated 314 ms
p50 / 6.3 s p99. **Caveat on comparability:** this is not directly comparable to the prior run's
"52 qps / 19,171 dropped / topk 19.5 s" — that number was the shared-pool mix swept to 800 qps on the
old driver; the driver methodology changed (task c) precisely so the measurement is clean. The isolated
topk pass above (200 qps on topk alone = 6.6× its ceiling) is what surfaces the pure store-bound tail;
the mixed pass is the realistic load.

**Net for the four mechanisms.** At rest, sub-second hydration holds (315 ms p50 / ~400 ms p99). Under
load the residual is exactly the *fundamental floor* named in the TL;DR (§4): `(row-groups/hit) ×
(object-store round-trip)`, throughput-capped by the store — on Hetzner OS ~30 topk qps with a ~13.7 s
tail under 6.6× overdrive, ~6.3 s p99 at the realistic ~30 qps. Neither the in-flight cap nor the
read-concurrency knobs move that tail; the driver isolation makes it *measurable* (0 timeouts, clean
per-type numbers) but does not lower it. The scale-out levers that would (Level 2/3 below, a distributed
store's aggregate GET bandwidth) remain the real path past this floor.

## Where hydration parallelism can live (coordinator ↔ nodes)

The reads do **not** happen on the coordinator. Tracing the top-k path:

- **Level 0 — already distributed to nodes.** `Gateway::search` (coordinator) strips hydrate, scatters a
  *coordinates-only* search, merges to the global top-k, then `hydrate_hits → get_by_key_unadmitted`
  (`gateway.rs:1524,1724-1763`) groups the top-k keys **by owning shard** (hash of the primary key) and
  scatters each subset to its **owning node** concurrently (JoinSet, fanout semaphore = 256). Each *node*
  runs the Iceberg read against object store. So hydration is already load-balanced across nodes by
  key-ownership — for a scattered top-k the ~20 winners hash ≈ uniformly → ~3–4 keys/node over 6 nodes.
  The coordinator does only cheap routing + merge; **its concurrency is not the hydration bottleneck.**
- **Level 1 — per-node read concurrency (this round's prototype).** Within a node, the keys' row-groups
  were read serially; now `buffered(N)` overlaps them. Cuts per-node at-rest latency (~6× local upper
  bound). This is the win that lands cleanly on the single-MinIO bench.
- **Level 2 — decouple read placement from index placement (the "sub-query hand-off").** Today a key's
  read runs on the node that owns its *index* (because that node holds the presence check + the `ts`
  prune hint). But the data is in *shared* object store, so read placement need not equal index placement.
  The coordinator could emit **portable hydration sub-tasks** — `(key, ts-hint, projection)`, self-contained
  because store-less hydration is just a predicated scan — and distribute them across **all** nodes by
  read-load, not key-ownership. Owning nodes still do the cheap index part (presence + hint) and return
  `(key, ts)`; the coordinator repartitions the *reads*. This balances key-ownership skew (one hot shard)
  and, on a distributed object store, uses more nodes' aggregate GET bandwidth.
- **Level 3 — partition by (file, row-group).** Group all keys that fall in the same row-group into one
  read (dedup GETs), spread distinct row-groups across nodes. Hydration becomes a coordinator-planned
  distributed scan — the most efficient form of the sub-query hand-off.

**The ceiling that gates Level 2/3.** Spreading reads across more nodes raises aggregate throughput only
if the **object store** can serve more concurrent GETs. Against **one MinIO pod on one volume** it cannot
— more reader nodes just saturate the same pod. So Level 2/3 pay off when (a) the object store is
genuinely distributed (real S3 / multi-node MinIO), or (b) key-ownership is skewed and one node is a
hotspot. On the current bench, **Level 1 captures the at-rest latency win; Level 2/3 are the scale-out
design for a real object store**, worth building when the store is no longer a single pod.

## Symmetric re-measurement plan (closes the fairness gaps)

Every gap below is a *measurement* gap, not an engine gap. The point is that every published number is
comparable.

1. **Isolate topk from index-only in the driver.** Run three passes per engine so the topk types can't
   starve the shared pool (the artifact in §Contamination): **(A)** index-only + autocomplete,
   **(B)** topk-only, **(C)** the full mixed plan (kept, as the realistic-mix number, but read as
   throughput not per-type latency). Either give each kind its own pool or run passes A/B separately.
2. **Both engines at low concurrency AND under load.** Add an OS 10-qps pass (today OS is only run at
   200+sweep; it happens to be unsaturated there, but publish the explicit low-conc pass so the
   GDB@10 vs OS@10 comparison is symmetric rather than "GDB@10 vs OS@200"). Publish p50/p95/p99 for
   both at both loads.
3. **Capture OS storage live, same run.** GDB storage is captured each run; OS on-disk (`_source` +
   completion FST, at the same replica count) must be captured in the *same* run for the
   storage-vs-latency trade to be same-conditions. Disclose replica parity (GDB single-primary vs OS
   replicas=1).
4. **Emit the engine-internal split alongside driver numbers.** Scrape
   `growlerdb_query_retrieval_duration_seconds`, `growlerdb_hydration_duration_seconds`,
   `growlerdb_query_duration_seconds{hydrated}` per type so the decomposition (retrieval vs hydration
   vs fan-out) is published, not inferred. Add a per-request **hydrated-rows counter** to confirm
   query-then-fetch stays at ~20 hydrations/query (not the earlier mistaken ~120) as a regression guard.
5. **Native `/v1/search` vs `_search` adapter A/B.** One boolean probe (same query, `hydrate=false`
   both ways, native vs adapter) to publish the adapter's true overhead (structurally expected to be a
   few ms; §Family A already bounds it). Cheap, local.

---

## What is answerable now vs needs a paid run

**Answerable now (done above), no cluster spend:** all seven questions — the decomposition, the four
mechanisms with code evidence, the contamination math, the ranked options — come from the committed
result JSONs + engine code + the existing engine-internal metrics.

**Done locally (this round), no cluster spend:** mechanisms 1 and 2 implemented and validated with
fs-backed Iceberg fixtures + a latency-injecting `FileIO` (see *Prototype results*): parallel file reads
5.95–6.41×; the `load_table` fold 2→1; both correctness-clean (37 source + 350 engine unit tests, plus
the query-then-fetch and single-file-prune tests). Verifying that query-then-fetch already exists
(no over-hydration) was also settled by code reading here, not a run.

**Still cheap-local, not yet done:** native `/v1/search` vs `_search` adapter A/B (re-measurement step 5).

**Genuinely needs a paid run:** only the final *symmetric published numbers* (re-measurement steps 1–4
at 10 GB on 6× ccx43) and validating the two prototyped fixes' impact at scale under real object-store
contention. Recommend landing mechanisms 1–2 + the driver pool fix first (local-verified), then a single
symmetric run — not a run to re-confirm the diagnosis, which the evidence already settles.
