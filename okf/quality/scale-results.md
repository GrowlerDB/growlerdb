---
type: Test Results
title: Scale-run results (tracked over time)
description: Committed, append-only record of GrowlerDB scale/performance runs in a common format — size ratios, query latency, GrowlerDB-vs-Iceberg, ingest ceiling, convergence — so results are comparable run-over-run. One row per run per headline milestone; heavy artifacts live in the gitignored bench/scale/runs/<run>/.
tags: [quality, scale, performance, benchmark, results]
timestamp: 2026-07-25T01:10:00
---

# Scale-run results (tracked over time)

The durable, comparable record of each [scale run](/quality/scale-test-plan.md). The freeform
operational ledger is `bench/scale/RUNLOG.md` (one prose row per run); **this** is the normalized,
metric-by-metric table so numbers can be compared run-over-run and regressions are visible. Each run
appends one row per **headline milestone** (post-compaction, converged). Heavy artifacts
(metric/log dumps, `SUMMARY.md`, per-query JSON) live under the gitignored
`bench/scale/runs/<run>/` — this file is the committed summary.

## How to add a run

After a run (post-compaction, converged), append one row to **each** table below, using the
definitions in [§ Metric definitions](#metric-definitions). Keep runs in chronological order. Record
the **milestone basis** explicitly (raw-uncompressed vs the old compressed basis) — runs on different
bases are **not** directly comparable (see the note under [§ Comparability](#comparability)).

## A. Run identity, size & convergence

| Run | Date | Image | Cluster | Milestone (basis) | Rows | Source (compressed) | Index bytes | idx:src RAW | idx:src compressed | Compression | Converged |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Run 7 | 2026-07-24 | v0.5.0 | 6× cpx62 | "1 GB" (**compressed** — ≈5 GB raw) | 36,122,080 | 916 MB | ~2.36 GB | ≈0.47× *(derived; not measured pre-[TASK-342])* | 2.58× | ~5× | exact |
| Run 8 | 2026-07-25 | dev-4841f7ac | 6× cpx62 | 1 GB (**raw uncompressed**) | 7,041,620 | 127 MB | 494 MB | **0.36×** | 3.89× | 10.85× | exact |

## B. Query latency — post-compaction, p50/p99 ms

End-to-end via the `_search` harness from a Mac port-forward, so all figures include a **~450 ms
Mac→nbg1 round-trip floor** (subtract it for server-side; the dashboard's internal
`growlerdb_query_duration_seconds` is ~5–80 ms). 0 errors unless noted.

| Run | match_all_count | window_1day | window_7day | term_in_window | text_request | topk_recent_hydrated | hydration p95 (internal) |
|---|---|---|---|---|---|---|---|
| Run 7 | 466 / 643 | 490 / 670 | 466 / 583 | 555 / 655 | 475 / 625 | 16114 / 18229 | ~800 ms/hit (16 s / top-20) |
| Run 8 | 469 / 682 | 474 / 773 | 471 / 756 | 500 / 774 | 473 / 631 | 1632 / 2700 | 2420 ms |

## C. GrowlerDB vs Iceberg-alone (Trino) — p50 ms, speedup = Trino / GDB

Fair baseline requires **post-compaction + bloom + day-pruned** ([TASK-343]); older runs that measured
Trino pre-compaction are flagged — their speedups flatter GrowlerDB.

| Run | Trino basis | point lookup [bloom] | term status=404 | text ~search | note |
|---|---|---|---|---|---|
| Run 7 | **pre-compaction (unfair)** | GDB 456 / Trino 4921 → **10.8×** | 0.5× (GDB hydrates hits) | 1.0× | tiny-file layout penalized Trino |
| Run 8 | post-compaction, fair (9 files, bloom, day-pruned) | GDB 464 / Trino 2503 → **5.4×** (5.8× day-pruned) | 1.7× [scan] / 1.4× [day-pruned] | 1.7× [scan] | Trino per-query includes ~1.5–2 s CLI/JVM startup |

## D. Ingest & cost

| Run | Single-node index ceiling | Cold-tier | Cost |
|---|---|---|---|
| Run 7 | ~(not stepped) | PASS — auto-park/read-through/auto-revive (in-cluster MinIO) | ~$1–2 |
| Run 8 | ~8,900 docs/s | not exercised (9 windows / 6 nodes < park threshold; PASS in Run 7) | ~$2 |

## D54 store-less hydration — live validation (non-windowed comparison shakedown)

The [store-less pruned key scan](/system/decisions/d54-store-less-hydration.md) replaced the stored
locator + compaction re-map. Validated live on a 10 GB non-windowed `http_logs` shakedown (6× ccx43
nbg1, image `dev-868f826`, 25.85 M rows, convergence exact). **Headline: `topk_hydrated` (size-20,
`sort response_time_ms desc` → 20 hits with `ts` scattered across the 7-day span) is sub-second
post-compaction — it was a 30 s timeout under the old locator path.** Shakedown-scale (directional,
not a publishable milestone); the full 100 GB head-to-head is the publishable run.

| Metric | Value | Basis |
|---|---|---|
| topk_hydrated, low concurrency | service p50 **864** / p95 924 / p99 **964** ms, 0 err | in-cluster driver, 10 qps, fresh compacted |
| hydration duration (engine-internal) | p50 431 / p95 924 / p99 **985** ms | `growlerdb_hydration_duration_seconds`, 2 m post-compaction window |
| topk_hydrated, high concurrency | ~12 s; overall collapses to ~40 qps | qps 200 + sweep→800 mixed — object-store-throughput-bound (single MinIO; the _source-vs-hydrate tradeoff) |
| index-only + autocomplete under load | 5–75 ms, 0 err | same high-concurrency mix (unaffected by the topk saturation) |
| freshness at rest | p50 2.06 / p99 2.56 s, 0 timeouts | streaming-commit visibility |
| primary index bytes | **3.90 GB** (term 1.86 / store 0.61 / postings 0.60 / positions 0.26 / fieldnorms 0.23 / fast 0.16) | 25.85 M docs, no `_source`; ≈0.28× raw NDJSON, ~310 MB below the pre-D54 index (deleted location array) |
| prune hint | fires — `sorted_by=[ts ASC, request_id ASC]` in table metadata (Spark `WRITE ORDERED BY` at compaction) | the prime prior failure mode (empty sort order → no hint → full scan), resolved |

The prune is the whole mechanism: on a `ts`-sort-clustered table iceberg-rust prunes a scattered top-k
to ~one row group per hit by `ts` min/max stats (no bloom support), so hydration reads ~K row groups —
the point-read volume, without any stored locator or re-map. On a large **unclustered/unpartitioned**
source the scan has no stats to prune on and degrades to a byte-budget-bounded broad scan (stated at
create). OpenSearch did not converge this run (a Data-Prepper-vs-compacted-table CDC artifact, not a
GrowlerDB result), so same-run head-to-head query numbers are pending the full run.

## Comparability

- **Run 7's "1 GB" is on the OLD compressed basis** (`growlerdb_source_bytes` = compressed parquet) —
  ≈**5 GB raw / 36 M rows**, so it is ~5× larger than **Run 8's 1 GB-raw** (~7 M rows). Latency,
  hydration, and index-size numbers between them reflect that **5× scale difference**, not a
  regression. [TASK-347] raises the baseline to **5 GB raw** to restore Run-7-comparable scale on the
  honest basis.
- **idx:src**: only compare the **RAW** column across runs — the compressed column shifts with codec
  ([TASK-342]). Run 7's RAW value is derived, not measured (predates the metric).
- **Trino**: only compare **post-compaction/fair** rows; Run 7's Trino is pre-compaction. Runs ≤8
  measured against Trino **470**; the bench image is **483** from Run 9 onward (aligned with the
  `connector-trino` SPI pin).
- **Query latency** includes the client→cluster RTT floor and is not the server-side number.

## Metric definitions

- **Milestone (basis)** — the storage target and whether measured against **raw uncompressed** corpus
  ([TASK-342], the convention going forward) or the legacy compressed-parquet basis.
- **Rows** — `growlerdb_source_records` at the milestone (== `growlerdb_index_docs` when converged).
- **Source (compressed)** — `growlerdb_source_bytes` (Iceberg `total-files-size`).
- **Index bytes** — `sum(growlerdb_index_bytes)`.
- **idx:src RAW** — index bytes ÷ raw uncompressed corpus (restart-durable: generator mean bytes/row ×
  `source_records`, [TASK-344]). The headline size number; index < 1× means smaller than the logical data.
- **idx:src compressed** — index bytes ÷ compressed source (config-dependent; context only).
- **Converged** — `index_docs == source_records` and the distinct-id convergence check passes.
- **Query latency** — `harness.py query` weighted mix, p50/p99 ms, end-to-end (incl. client RTT).
- **hydration p95 (internal)** — server-side `growlerdb_hydration_duration_seconds` p95 (the
  store-less stats-pruned key scan; degrades on an unclustered/unpartitioned source that can't prune).
- **Trino speedup** — `compare_trino.py` p50(Trino) ÷ p50(GDB) for equivalent predicates; must be
  post-compaction + bloom + day-pruned to be fair ([TASK-343]).
- **Single-node index ceiling** — max sustained `growlerdb_ingested_docs_total` rate for one connector
  before the backlog grows.
