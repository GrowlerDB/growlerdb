---
type: Decision
title: D31. Ingest silent-loss guards — count gate, continuity guard, lockstep, retryable backpressure
description: Defense-in-depth against silent ingest under-read — a connector expected-row-count gate, a node checkpoint-continuity guard, lockstep checkpoint advance across shards, lineage-ordered head resolution, and retryable admission backpressure — so an under-read stalls loudly instead of sealing a permanent gap.
tags: [decision, adr, ingestion, checkpoint, exactly-once, reliability]
timestamp: 2026-07-04T14:22:00
---

# D31. Ingest silent-loss guards

**Decision.** Add defense-in-depth so a changelog **under-read** (the scan returning fewer rows than
a source snapshot committed) can no longer silently advance the checkpoint over the gap. Five guards,
addressing a shard that could come up short a few rows with no error:

1. **Expected-row-count gate (connector).** Before any write for a trigger, sum `added-records`
   over the window's **append** snapshots (walked along lineage) and assert the changelog returned
   at least that many rows; a shortfall throws (no cursor advance). Windows containing
   overwrite/delete snapshots — where the changelog's net diff legitimately diverges from physical
   `added-records` — are exempt (reconcile is the backstop there). This closes the structural window:
   an empty/short window could jump the in-memory cursor to head and a later batch stamp a later
   checkpoint over the gap.
2. **Node checkpoint-continuity guard.** Each `DocBatch` now carries the `from` checkpoint it
   resumes from. *(As shipped: exact `from == current`. Relaxed to **window-covering**,
   see the refinement below: a batch is applied iff its window `(from, end]` covers the shard's
   position — `from ≤ current < end` in lineage order; `end ≤ current` no-ops; only `from`
   strictly ahead — a hole — is refused with non-retryable `FAILED_PRECONDITION CHECKPOINT_GAP`.)*
   Either way a shard never overwrites its checkpoint forward over unapplied data (a lineage gap /
   regression / cross-wiring). The decision is re-checked **at commit, under the writer mutex** —
   the stage-time check is lock-free and advisory (the commit-time re-check closed the stage/commit
   TOCTOU that let two racing writers regress the checkpoint).
3. **Lockstep checkpoint advance.** Empty per-shard sub-batches are now sent (not skipped) and a
   no-work batch advances the shard's source checkpoint via a redb-only commit (no new index
   snapshot). Every shard tracks the same source position, so shards can't drift — which both
   inflated the min-checkpoint resume re-read and is the precondition the continuity guard relies on.
4. **Lineage-ordered head.** The connector resolves the trigger head from the table's `main` branch
   ref, not `ORDER BY committed_at`; under a two-writer clock skew the newest-by-time snapshot need
   not be the lineage tip, which produced head-shadowing no-op stalls.
5. **Retryable admission backpressure.** `RESOURCE_EXHAUSTED` is retried at the connector (it is
   transient write-admission backpressure, safe to replay on the idempotent `batch_id`), and the
   node holds its admission permit for the true duration of the blocking commit — so a
   client-cancelled but still-running commit keeps its slot and the node sheds new load rather than
   spawning unbounded concurrent commits that thrash the disk under a compaction I/O storm (the
   detonator of the loss event).

**Why.** The audit traced the loss to an under-read window
sealed by the cursor jump, with the compaction I/O storm as the likely detonator via a
non-retryable `RESOURCE_EXHAUSTED`. Snapshot ids are random longs (not lineage-ordered), so the
node without a lineage order can't compare checkpoints at all, so continuity falls back to an **exact `from ==
current`** check, made sound by the lockstep advance (which keeps every shard's `from`
well-defined). The count gate is exact for append windows because the changelog counts physical
rows (a duplicate PK appears once per physical append — GrowlerDB collapses it last-write-wins
later, in the engine).

**Refinement: ordered checkpoints + window-covering.** Exact matching turned out to be
both too weak and too brittle. Too weak: three load-bearing paths *did* order the random ids
numerically anyway — the connector's resume-from-min (`Math.min`), the idempotency prune index's
range key, and the reindex `max(old, new)` belt — each a latent permanent-stall or broken-dedup
bug. Too brittle: the exact lattice depends on re-sent batch ids matching originals,
but a trigger's **tail chunk** checkpoints at the trigger-time head — a timing artifact — so a
partial fan-out failure plus a head advance during recovery wedges the ahead shard behind an
unmatchable `CHECKPOINT_GAP` forever. The fix is one primitive plus one relaxation:
`SourceCheckpoint` now carries the snapshot's Iceberg **data sequence number** (v2 tables;
branch-monotone — the only sound order, stamped by the connector from table metadata via the Java
API), and the guard accepts any batch whose window **covers** the shard's position. Covering
overlap is content-safe: the overlapped rows are byte-identical re-applies of committed ops
(idempotent delete-then-add, LWW within the batch), and the shard holds nothing past `current`.
Loss detection is preserved — `from` strictly ahead of `current` is still a hole and still
refused. Un-sequenced checkpoints (legacy persisted values, v1 tables) keep exact-match semantics
end to end.

**Consequences.** `DocBatch` gains an optional `from_checkpoint` field (backward-compatible; absent
= bootstrap/legacy — which, with sequence numbers, is no longer unguarded: a stale bootstrap window
now **no-ops instead of regressing** the checkpoint; bulk build/reindex/reconcile chunks that
re-commit at one fixed checkpoint still apply). `SourceCheckpoint` gains `iceberg_sequence_number`
(0/absent = unknown ⇒ exact-match fallback); the idempotency prune index is keyed by it (a one-time
migration clears the misordered legacy index — safe, because under covering the batch-id
records are an *optimization*: a replay no-ops by **position**, so dedup and deterministic chunk
boundaries are no longer correctness dependencies). A no-work batch is no longer a pure no-op — it
advances the checkpoint idempotently, keeping the Tantivy-then-redb ordering (D30) for real work
(the commit ordering itself is untouched by the refinement). This is defense-in-depth over D9's exactly-once, not a replacement: the count gate
targets append under-reads specifically, and the **systematic detector/repairer for any cause is
the sharded, scheduled reconcile backstop D9 promises** (shipped shard-scoped). The guards
are backed by **observability** (AC6): the connector exports its ingest signals as Prometheus metrics
(per-trigger rows read/expected, per-shard acks, stream restarts, write retries, under-read stalls)
instead of printf that rotated away, the node counts dedup hits and checkpoint-gap rejections, and
alert rules fire on any under-read / checkpoint-gap / drift-repair / sustained idle-lag. The
convergence check is upgraded to **exact-count-at-drain** on the source's distinct-id count — which
catches an under-read that lag-based checks miss and isn't fooled by duplicate ids — gated
automatically, optionally racing a live compaction (AC7).

**Status.** Accepted. Guards 1–5 are live and unit/integration-tested (the connector's
expected-row-count gate and lineage head against Spark local mode; the node's continuity guard and
lockstep empty-advance against the in-process shard). The original **loss signature** — one
trigger's writes landing on some shards but not others while the checkpoint advances — is now a
direct regression test (`store::tests::task194_missed_shard_write_trips_a_loud_checkpoint_gap`): a
missed-write shard trips a loud `CheckpointGap` rather than silently sealing the gap, complementing
the connector fan-out all-settle throw (`ShardedWriteClientFanOutTest`) that stops the offset
advancing in the first place. Refined with ordered checkpoints
+ a window-covering guard + a commit-time re-check — the enabling
foundation for parallel ingest connectors. Refines **D9** (its reconciliation backstop remains the
general-case complement) and **D10** (ingestion runtime unchanged).
