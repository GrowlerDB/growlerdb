---
type: Decision
title: D9. Sync model: changelog-first
description: Changelog scan by default, append-only opt-in, CDC where available, with a reconciliation backstop.
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D9. Sync model: changelog-first

**Decision.** Changelog scan by default, append-only opt-in, CDC where available, with a
**reconciliation backstop**.

**The reconciliation backstop.** Reconcile compares the index against the source's current snapshot
and repairs both directions of drift — deletes indexed keys the source no longer holds, re-indexes
source keys the index is missing (drift-derived idempotent `reconcile-{hash}` batch ids). It is the
one mechanism that both **detects and repairs** silently lost/orphaned rows *regardless of cause*, so
it complements the ingest-path guards ([D31](/system/decisions/d31-ingest-loss-guards.md)) that catch
the specific under-read class.

Reconcile is **shard-scoped** (task-195): the caller restricts the comparison to the keys a shard
owns, via the same registry-vended virtual-bucket map + shard ordinal the gateway and connector route
by (`ShardRouter::owns`). Without that, running reconcile against one shard of a sharded index would
re-index the other shards' keys into it, destroying placement — so the whole-table, single-shard form
must never run on a sharded cluster. It is exposed as a **node Admin RPC** (`ReconcileIndex`), reads
the source streamed (peak memory O(owned keys), not O(table)), and emits `growlerdb_drift_stale_total`
/ `growlerdb_drift_missing_total` / `growlerdb_drift_reconcile_total` (labelled by index + ordinal;
alert on a nonzero repair rate). A nonzero repair also logs the affected keys (bounded).

**Concurrency (TOCTOU guard).** Reconcile runs online, while ingestion may be writing the same shard.
The stale-delete would be unsafe under that race — it reads a source snapshot, then deletes indexed
keys absent from it, so a key a concurrent ingest committed *after* the snapshot read could be
mistaken for stale and dropped (and the [continuity guard](/system/decisions/d31-ingest-loss-guards.md)
means the connector won't re-send it). Guard: capture the shard's checkpoint before the source scan
and, under the writer lock (which serializes commits), delete only if the checkpoint hasn't advanced
since — a match proves no commit landed during the scan. If it advanced, the stale-delete is skipped
that cycle (the always-safe missing-repair still runs) and the next reconcile retries once the shard
is momentarily quiescent. Missing-repair is unconditional; only the delete is fenced this way.

**Count-gate (scale).** Reading the whole source every cycle is O(table) — infeasible at 100 TB.
Reconcile is **count-gated** so the in-sync common case reads no rows (task-198): (1) a **whole-index
gate** — the cluster driver first probes each shard `count_only`, and if Σ index docs == the source
table's `total-records` (a metadata read), the index is in sync and the row-level reconcile is skipped
entirely (routing-agnostic); (2) a **per-partition gate** — when the source is cleanly
identity-partitioned on the index's partition-key fields, each node compares per-partition source
`record_count` (manifest metadata) to the partition's index key-count and row-reconciles only the
divergent partitions, bounding memory to one partition. Counts are an exact *trigger*, not a proof:
equal counts can hide compensating drift (a stale+missing pair, or duplicate PKs), and the
per-partition gate iterates source partitions so a wholly-dropped partition isn't caught — a periodic
`--full` sweep is the completeness backstop. Non-partitioned / non-identity / hash-routed cases fall
back to a whole-shard scan (correct, just not optimized).

**Status.** Accepted. The shard-scoped reconcile RPC + drift metrics are live, it is **scheduled**
(an opt-in Helm CronJob runs `growlerdb reconcile <index> --control-plane …`, fanning the shard-scoped
`ReconcileIndex` out to every shard's primary; any unreachable shard fails the run so a silent skip
can't hide), and it is **count-gated** (above). The count-gate's per-partition path is unit-tested at
the metadata/decision layer; the end-to-end partitioned reconcile is validated in-cluster; and the
detect-and-repair backstop is exercised **both directions in one cycle** by an e2e drift-injection
test (`e2e::reconcile_backstop_detects_and_repairs_drift_both_ways` — deletes indexed rows the source
still holds and plants a stale row the source never held, then asserts one `reconcile` re-indexes the
missing and removes the stale, idempotently).
