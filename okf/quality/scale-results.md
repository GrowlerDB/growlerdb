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
- **hydration p95 (internal)** — server-side `growlerdb_hydration_duration_seconds` p95 (the O(rows)
  hydration ceiling, [TASK-339]).
- **Trino speedup** — `compare_trino.py` p50(Trino) ÷ p50(GDB) for equivalent predicates; must be
  post-compaction + bloom + day-pruned to be fair ([TASK-343]).
- **Single-node index ceiling** — max sustained `growlerdb_ingested_docs_total` rate for one connector
  before the backlog grows.
