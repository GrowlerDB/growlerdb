# GrowlerDB-vs-OpenSearch benchmark — next round: close the gaps

Handoff prompt for the agent taking the comparison benchmark from **shakedown #4 (passing)** to a
publishable head-to-head. Read this whole file, then `bench/scale/comparison-plan.md` (plan + fairness
charter), the shakedown #4 row in `bench/scale/RUNLOG.md`, and the memory note `os-comparison-bench.md`.

## Where things stand (don't re-derive)

Shakedown #4 (2026-08-27) was the **first fully-completing** GrowlerDB-vs-OpenSearch run. All prior
harness bugs are fixed and the **cold-sync methodology is validated live**: generate → settle →
`STAGE=deps` bring-up → `phase_gdb_coldsync` (nodes empty via `DEFINE_ONLY`, connector cold-backfills)
→ query → transition → OpenSearch cold-sync → query. Corpus is **dup-free** (exact convergence,
`rows_behind: 0`, both engines). Branch: **`feat/os-comparison-bench`**. Scale image: **`e69c40f`**
(built from the branch via `scale-images` `workflow_dispatch`, tags `oscmp` + the commit SHA; carries
the engine `_source`-hydration fix). Captured per-component metrics: `bench/scale/runs/2026-08-28T*`.

**Run recipe (validated):** `terraform apply` → `STAGE=deps SPAN_DAYS=7 WORKLOAD=http_logs
IMAGE_TAG=<sha> GENERATORS=3 GH_USER=… GHCR_PAT=… GROWLERDB_LICENSE=… deploy/k8s/scale-up.sh` →
port-forward prometheus → `python bench/scale/compare_run.py --scale shakedown`. ~6× ccx43 nbg1, ~$8/run.

## Current head-to-head (10 GB, 25.98M rows, 6× ccx43) — the gaps to close

| Axis | GrowlerDB | OpenSearch | Goal |
|---|---|---|---|
| Ingest (cold-sync) | ~16.8k docs/s | ~17.5k docs/s | **GDB clearly faster** |
| Index-only query (svc p50) | 20–80ms | ~5ms | **~even** |
| Autocomplete | 21.9ms | 8.2ms | **~even** |
| Full-retrieval `topk_hydrated` | 30s TIMEOUT 🔴 | 10.8ms | GDB works (then compare) |
| Freshness (at rest) | 2.06s | 57.7s | already GDB (~28×) ✅ |
| Index storage (primary) | 4.21 GB | 14.34 GB | already GDB ✅ (reframe, see #5) |

**Target end state (Kira):** ingest + storage in **GrowlerDB's favor**; index-only + autocomplete
**close to even**. Keep every change **simple and sensible** — smallest change that moves the number.

## Tasks

### 1. Connector ingest throughput — make GrowlerDB clearly faster than OpenSearch
**Finding (measured):** the ~16.8k docs/s was NOT engine/hardware-bound. During the cold-sync the node
fleet used **6% CPU (5.8/96 cores)**, the connector **1.4 of its 6 cores**, `write_queue_depth ≤ 2`
(no backpressure), disk-io util 0, no `RESOURCE_EXHAUSTED`. The single `local[6]` Spark connector's
pipeline is the ceiling — and it isn't even using its own 6 cores. Nodes burst to ~32k/s aggregate.

**Do (in this order — config before architecture):**
1. **First check whether the single Spark pipeline can be scaled/configured for more throughput.** Find
   why it uses only 1.4/6 cores: is the Iceberg changelog read single-threaded? Are the gRPC writes to
   nodes serialized? Is the micro-batch trigger/commit cadence the limiter (it committed ~8.66M-row
   batches)? Read `connector/src/main/java/io/growlerdb/connector/*` and
   `deploy/k8s/streaming/connector.template.yaml` (`local[6]`, cpu limit).
2. Try config/tuning: more read parallelism (Spark partitions on the source scan), higher write
   concurrency to nodes, async/pipelined commits, streaming/continuous trigger vs the big-batch
   trigger, more executor cores. Aim to keep the (idle) nodes fed.
3. Only if config genuinely can't unlock it: consider multiple connector instances / a sharded
   connector. Keep it as simple as the numbers allow.

**Success:** GrowlerDB cold-sync docs/s comfortably above OpenSearch's ~17.5k (the node headroom says
this is very reachable). Re-measure with the same attribution metrics (`capture.py` already has them).

### 2. Autocomplete — give GrowlerDB a dedicated completion/prefix structure
**Finding:** GrowlerDB native `/v1/suggest` (live term-dictionary scan) = 21.9ms vs OpenSearch dedicated
completion FST = 8.2ms (+179 MB storage). **Kira's decision:** the FST storage cost is small and
acceptable — **give GrowlerDB a dedicated completion/prefix structure (FST or equivalent)** so
autocomplete is competitive. Do NOT keep the live-scan asymmetry.

**Do:** assess whether GrowlerDB can build/enable a dedicated prefix structure for the suggest path
(engine change; then a `scale-images` build). Prioritize latency parity; storage is not a concern.
Update the plan's autocomplete-parity note (it currently frames suggest-vs-FST as a disclosed
asymmetry — that changes to "both build a prefix structure").

### 3. Post-compaction hydration — fix TASK-339 (unblocks full-retrieval AND fixes #4)
**Finding:** `topk_hydrated`/`topk_recent` time out at 30s post-compaction. Confirmed: `stale_locators
= 2973`, engine `hydration p95 = 10s`. Hydration **worked pre-compaction** (conv-gdb sample-integrity
passed) and **broke after** — Iceberg compaction rewrites files → GrowlerDB row locators go stale →
hydration falls to full-snapshot scans → 30s, and the heal can't complete (times out). A warmup can't
fix it (its hydrations also time out). Engine: `crates/growlerdb-engine/src/hydrate.rs` (locator heal,
`apply_live_file_bitmap`, `refresh_locators`), `crates/growlerdb-source/src/lib.rs` (`hydrate`).

**Do:** make locator resolution survive/heal across compaction cheaply (persist or fast-remap locators
on a rewrite, instead of a per-hit full-snapshot scan). This unblocks the full-document-retrieval
head-to-head (GDB hydrate-from-Iceberg vs OS `_source`) **and** removes the contention that inflates
index-only queries (see #4). This is the single most important engine fix for the comparison.

### 4. Index-only query latency — get close to even
**Finding:** GDB 20–80ms vs OS ~5ms, but this is mostly **measurement contamination**: the mixed
open-loop run included the broken `topk_hydrated` (30s locator-heal scans) which contended for node
CPU/threads and inflated even fast queries — captured engine-internal `query_latency_p50` peaked ~90ms
while `query_retrieval_p95` was ~10ms in the clean window. Plus a small gateway-hop tax (GrowlerDB's
gateway is a separate process → driver→gateway→6-node gRPC fan-out→merge, vs OpenSearch's in-cluster
coordinator).

**Do:** fixing #3 removes most of it. Then re-measure. Consider also measuring index-only queries **in
isolation** (a separate driver pass, not mixed with the retrieval/hydration types) for a clean number,
and assess whether the gateway fan-out has a cheap reduction. Target: close to OpenSearch's single-ms.

### 5. Storage & all "source" comparisons → RAW UNCOMPRESSED basis (Kira directive)
**ALL comparisons to "source" must use the ORIGINAL UNCOMPRESSED corpus (~10 GB at shakedown), NEVER
the Iceberg parquet intermediary (1.58 GB compressed).** Reframe every index:source ratio:
- GrowlerDB index 4.21 GB / ~10 GB raw = **~0.42×** (index is SMALLER than the raw data).
- OpenSearch index 14.34 GB / ~10 GB raw = **~1.43×** (bigger than the raw data — `_source` + FST).

**Do:** compute the EXACT raw uncompressed corpus bytes for the run (rows × the documented avg raw
row size in `bench/scale/synthetic-corpus.md`; the generator's per-pod genmetrics byte counter is
unreliable — resets on restart, see the Run 8 RUNLOG note). Update `capture.py`/the report so the
index:source ratio is on the raw basis (Run 8 precedent: "idx:src RAW=0.36× vs compressed 3.89×").
Never quote the parquet size as "source" in the published numbers.

### 6. Replica parity (fairness)
OpenSearch ran `number_of_replicas: 1` (29.3 GB on-disk) while GrowlerDB ran single-primary (4.21 GB).
For a fair on-disk comparison: set OpenSearch `number_of_replicas: 0` to match GrowlerDB's single-
primary (`deploy/k8s/comparison/opensearch.yaml`), OR compare **primaries only** and disclose it. Pick
one and make it consistent in the report.

## Validate + finish
Re-run a shakedown (`STAGE=deps scale-up` → `compare_run --scale shakedown`) after the fixes to confirm
the gaps close, using the attribution metrics to prove *why*. Then update `comparison-plan.md`,
`RUNLOG.md`, and the memory note, and decide on `--scale full`. Engine changes (#2, #3, #4) need a
`scale-images` build from the branch (`gh workflow run scale-images.yml --ref feat/os-comparison-bench
-f tag=oscmp`) before a run picks them up; connector changes (#1) rebuild `growlerdb-connector`.

## Guardrails (hard-won this round — do not repeat)
- **Never declare a fix working from a mid-flight/unsettled measurement.** Settle convergence and
  assert exact counts (`COUNT(*) == COUNT(DISTINCT) == doc_count`). I wrongly called the dup fix
  "ineffective" from a mid-catch-up read; a settled run proved it fine.
- **Verify root causes by reproduction/evidence, not elimination.** The dup bug was mis-attributed
  twice (pyiceberg, then Polaris) before a local repro pinned it to the pre-fix same-batch retry.
- Keep intact: the dup fix (`d7fc862` idempotent `make_cols` append-retry), the restart-safety fix
  (`79758d6`), the `scale-up.sh` `make_cols` deploy-gate, the Spark-4.0.0 maintenance image, the engine
  `_source`/`size` hydration gate, and the bounded-drain query-driver guard.
