---
type: Concept
title: Scale ceilings toward 10s–100s TB
description: Code-grounded map of where GrowlerDB breaks between today's scale and a 100 TB target — ingest scale-out, table maintenance, the location-array hot floor, reconcile, query fan-out, and control-plane serial constants — with the temporal-vs-non-temporal fork.
tags: [quality, scale, reliability]
timestamp: 2026-07-04T14:22:00
---

# Scale ceilings toward 10s–100s TB

A code-grounded audit (2026-07-04) of where the current implementation stops scaling on the path to a
single index over **10s–100s of terabytes** of source data. The architecture's bones are right —
horizontal sharding ([D12](/system/decisions/d12-sharding.md)), cold-tiering to object storage, and
the small-footprint [layered locator (D30)](/system/decisions/d30-layered-locator.md) — but reaching
100 TB is gated on work that is not yet built, plus one tiering surprise. **None of this is
benchmarked** ([scale numbers unvalidated](/quality/known-limitations/scale-unvalidated.md)); every
magnitude below is inferred from the code and the streaming-append model, not measured.

## The fork: temporal vs. non-temporal data

The single most important framing. The two cases scale very differently:

- **Temporal (windowed — logs, telemetry, events; the stated sweet spot).** Cold-tiering bounds the
  Tantivy bulk to the N hot windows; time-range queries prune to overlapping windows; and if the key
  correlates with time, the `PREDICATE` location strategy removes the location-array hot floor too.
  **100 TB is credible here** — *if* ingest and maintenance scale (below).
- **Non-temporal (hash-sharded, no time dimension).** There is **no cold tier** — the full 45–90 TB
  index is hot, capacity-bounded by local disk; **no query pruning** (every search broadcasts to all
  shards); the location array is all hot. This is the genuinely weak case and should be treated as a
  design boundary, not a bug.

## Ranked ceilings

| # | Ceiling | Grows with | Severity | Tracked as |
|---|---------|-----------|----------|-----------|
| 1 | **Ingest scale-out** — *(addressed, task-196 / [D32](/system/decisions/d32-parallel-ingest.md))* a **shard-group connector set** now partitions one table across W workers (executor-side owned-row filter, per-group resume, no coordination), on top of ordered checkpoints + the window-covering guard (which also fixed three latent stall/dedup bugs, task-205/206/207) and the concurrent per-shard fan-out. Residual: each *worker* is still one Spark driver — beyond its ceiling, raise W (bounded by shard count) | ingest rate | done | task-196 |
| 2 | **Source file/metadata growth degrades reads** — GrowlerDB reads O(files) (scan planning + hydration), so a source that accumulates small files / fat metadata slows queries. **Not GrowlerDB's to fix** — it never manages the source table (D30); the remedy (Iceberg maintenance) is the user's, outside GrowlerDB. GrowlerDB's job is *observability* so users can diagnose it: *(addressed, task-197)* per-index `growlerdb_source_*` gauges (data-file count, mean file size, delete files, snapshots) sampled from source snapshot metadata surface exactly this, with a [source-health runbook](/system/source-health.md). The demo/scale-test maintenance CronJob is a convenience, not a product feature | file inflow | user-owned (GrowlerDB: metrics, done) | task-197 |
| 3 | **`location.arr` hot floor** — 12 B/row, O(total rows), stays hot **even when the window is parked**; cold-tiering does not bound it (~1.2–6 TB hot-NVMe floor per 100 TB index, unbounded with retention) | total rows × retention | serious | task-201 |
| 4 | **Reconcile full-scan** — *(addressed, task-198)* reconcile is now **count-gated**: a whole-index `count_only` probe skips the row read entirely when Σ index docs == the source's `total-records`, and a per-partition gate row-reconciles only the partitions whose source `record_count` diverges from the index key-count (bounding memory to one partition). A periodic `--full` sweep remains the completeness backstop for equal-count drift. Residual: the `--full` / hash-routed fallback still buffers the whole owned set in one `Vec` | table size | done | task-198 |
| 5 | **Search fan-out** — *(addressed, task-199)* a search AND-pinning the keyword partition fields now routes to the one owning shard, and a semaphore caps concurrent fan-out; residual is hedging-to-replicas for tail latency (replica-dependent) | shard count | done | task-199 |
| 6 | **Node commit path** — *(compaction addressed, task-200)* compaction now merges bounded **size tiers** in a lock-releasing loop instead of the whole shard in one held lock, so a merge is O(a tier) not O(shard) and ingest interleaves; residual is the 3 fsyncs/batch under the single writer mutex | shard size | mostly done | task-200 |
| 7 | **Control-plane serial O(N) constants** — *(mostly addressed, task-202)* the status poller now probes shards **concurrently** (bounded), and node registration **batches** all a node's ordinals into one persist (killing the O(N²) bring-up) with the `.prev` roll made O(1) (hardlink, not a full copy). Residual: reshard build is still serial + non-resumable (AC3), and windowed indexes still lack a lag metric | shard count | mostly done | task-202 |

**Tiers 1–2 gated everything** — #1 is now addressed (parallel ingest exists; unbenchmarked), and #2 sits *upstream* of the read-path degradation (scan planning and hydration are both
O(files), so a growing file count silently degrades queries — though keeping the source compacted is
the **user's** responsibility, not GrowlerDB's; #2's deliverable is diagnostic metrics, not doing the
maintenance). **Tier 3** is the tiering surprise: cold-tiering offloads the Tantivy bulk (~97%, the
D30 claim holds) but the O(rows) location array is the residual and it is always hot.

## Correctness note (not scale)

The shipped reconcile ([D9](/system/decisions/d09-sync-model.md)) once had a **TOCTOU window**: it
reads a source snapshot, then deletes any indexed key absent from that set — but a key ingested
*after* the read was classified stale and deleted, and the checkpoint-continuity guard means the
connector won't re-send it, so the deletion could persist until the next reconcile. *(Fixed,
task-195.)* `ReconcileIndex` takes no write-fence (unlike `ReindexIndex`); instead it captures the
shard's checkpoint before the source scan and, under the writer lock, applies the stale-delete only
if the checkpoint hasn't advanced since (a match proves no commit landed during the scan). If it
advanced, the delete pass is skipped that cycle (`deletes_skipped`) — the always-safe missing-repair
still runs — and the next reconcile retries once the shard is quiescent. See the TOCTOU guard in
[D9](/system/decisions/d09-sync-model.md).

## Cheap, high-leverage wins

Small, mostly pure-GrowlerDB changes that raise headroom well before the structural work:

- **Kill the O(files×requested) hydration file lookup** — a per-file `tasks.iter().find` in
  `growlerdb-source` pass-1; replace with a `HashMap` built once per plan.
- **Stream the connector changelog** instead of `collectAsList` — *(done, task-203)* `toRows` used to
  `collectAsList` the whole trigger window into the 8g driver before bounded catch-up chunked it, so a
  post-outage backlog OOM'd (exit 52). Now the read→map→commit path **streams** (`toLocalIterator`, one
  partition at a time) and flushes bounded sub-batches, so driver memory is O(chunk); the under-read
  gate runs as a distributed `count()` and its metadata walk is bounded to the window's snapshots
  (`committed_at ≥` the resume point, full-scan fallback under clock skew) instead of `SELECT *` over
  all history each trigger.
- **Concurrent + connection-pooled status poller**; **batch node registration** into one registry
  write (part of task-202).
- **Prune `BATCH_KEYS`** older than the checkpoint horizon — *(done, task-204)* the idempotency store
  was a monotonic local leak (O(committed batches), never reclaimed). It can't be aged out
  shard-locally: the connector resumes from the **min** checkpoint across shards and re-sends already-
  applied batches to ahead shards, so a shard that dropped a record a sibling's lag could still trigger
  would hit a spurious `CHECKPOINT_GAP`. Fixed with a **resume-floor watermark** — each batch carries
  `safe_checkpoint` (the min-across-shards position the connector resumed from, read from *exclusive*
  and monotonic), and the node range-prunes every record at/below it (the batches that can never be
  re-sent). See [checkpoints & exactly-once](/product/functional/ingestion/checkpoints-exactly-once.md).

## Recommended sequencing

1. Fix the reconcile TOCTOU (correctness) + land the cheap wins. *(done — task-195 TOCTOU guard +
   O(1) hydration lookup; changelog streaming task-203; bounded idempotency store task-204.)*
2. Count-gate reconcile (task-198) — retires ceiling #4 using per-file `record_count` metadata
   already in hand, reading rows only for divergent partitions. *(done.)*
3. Then the structural bets in priority order: bounded compaction (task-200, done), search
   partition-routing (task-199, done), control-plane serial constants (task-202, done), and the
   tallest pole, ingest scale-out (task-196, done). Source-health metrics (task-197, done) surface
   the user-owned #2. With these landed the structural scale-ceilings program is complete; what
   remains (ceiling #3, the `location.arr` hot floor, task-201) is a bounded residual, not a blocker.

Refines the scale intent behind [D12](/system/decisions/d12-sharding.md) and the tiering behind
[D30](/system/decisions/d30-layered-locator.md); complements the
[unvalidated-numbers](/quality/known-limitations/scale-unvalidated.md) note (which is about
*benchmarking* the targets — this is about the *structural* ceilings the code has today).
