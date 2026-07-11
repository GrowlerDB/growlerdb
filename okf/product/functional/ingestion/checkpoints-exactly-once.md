---
type: Feature
title: Checkpoints & exactly-once
description: Persisted ingestion position gives exactly-once resume — no loss, no duplicates across restarts.
tags: [feature, ingestion, checkpoint, exactly-once]
timestamp: 2026-07-04T14:22:00
---

# Checkpoints & exactly-once

Ingestion progress is **checkpointed** so a restart resumes from the last committed position with an
**exactly-once** guarantee: **no committed-data loss** (RPO = 0 for acknowledged writes) and **no
duplicates**.

## Behavior

- Each batch carries an idempotent id; the node commits the index and its checkpoint crash-consistently.
- On connector or node restart, `GetCheckpoint` returns the resume point → the connector replays from
  there, so nothing is lost and nothing is double-applied.

## Chunked commit

A single batch can be large (a big source snapshot the connector couldn't sub-divide — it cuts only at
snapshot boundaries), and committing it as one Tantivy segment is O(batch) — a ~9.5s commit at 300k
rows. The node therefore applies a batch in **bounded chunks** of
`GROWLERDB_WRITE_COMMIT_CHUNK` docs (~25k default; `0` disables): each chunk runs the [layered locator](/system/decisions/d30-layered-locator.md) durability
order (location array synced → Tantivy committed → searcher reloaded), so its docs become searchable
immediately, while the **source checkpoint advances exactly once, at the end of the batch**. This is
invisible to the exactly-once contract: the intermediate chunk commits leave the index *ahead of the
un-advanced checkpoint* — the steady state this design already tolerates — so a crash before the final
checkpoint advance replays the whole batch, which the delete-then-add-by-key path applies idempotently
(and the file interns re-allocate deterministically). It bounds per-commit latency and improves
freshness (early rows queryable mid-batch) under large source snapshots, without touching the
continuity guard or the checkpoint.

## Ordered checkpoints

Iceberg snapshot ids are **random longs** — they carry no order (comparing them numerically was a
family of latent stall/dedup bugs). Every checkpoint therefore also carries the
snapshot's **data sequence number** (branch-monotone, Iceberg v2; stamped by the connector from
table metadata), and everything that must order two checkpoints — the continuity guard, the
resume-from-min across shards, the idempotency prune — orders by it. A checkpoint without one (a
legacy persisted value, or a v1 table) is *incomparable*: consumers fall back to the exact-match
semantics rather than guess.

On a sharded cluster the connector resumes from the **min** committed checkpoint across shards — in
lineage order — and re-sends the changelog from there to *every* shard; shards already ahead no-op
the overlap (below), the laggard catches up.

## Bounded idempotency store

The node keeps a small **per-batch idempotency record** (`batch_id` → committed) so a replayed
batch is recognized without re-staging. Since the window-covering guard (below) already no-ops a
replay by *position*, these records are belt-and-braces plus observability (dedup-hit metrics),
not a correctness dependency.

They are pruned with a **resume-floor watermark**. Each batch carries `safe_checkpoint` — the
position the connector resumed the whole cluster from (the lineage-min-across-shards floor,
identical on every sub-batch of a trigger, always ≤ every shard's checkpoint). The connector reads
the changelog from it *exclusive* and never resumes before it (the min is monotonic), so **no
batch at or below the floor can ever be re-sent** — the node drops those records on commit. The
prune is O(pruned) via an index ordered by sequence number (a floor or record without one prunes /
is pruned not at all — bounded over-retention, the safe direction). Both batch-partitioners carry
the floor onto every per-shard sub-batch (the Java connector's and Rust
`ShardRouter::partition_batch`): a sub-batch without it would silently prune nothing on
its shard.

## Silent-loss guards ([D31](/system/decisions/d31-ingest-loss-guards.md))

A changelog **under-read** (the scan returning fewer rows than a source snapshot committed) must not
silently advance the checkpoint over the gap. Defense-in-depth:

- **Expected-row-count gate** — per trigger, the connector asserts the changelog carried at least
  `Σ added-records` over the window's append snapshots before any write; a shortfall throws (no
  cursor advance), turning an under-read into a loud, self-healing stall instead of permanent loss.
- **Window-covering continuity guard** — each batch carries the `from` position it resumes from,
  and the node applies a batch iff its window **covers** the shard's position (`from ≤ current <
  end` in lineage order; the overlap re-applies committed ops byte-identically — content-safe). A
  window ending at/behind the shard is an idempotent replay: no-op, never a regression. Only a
  `from` **strictly ahead** of the shard — a hole — is refused (`CHECKPOINT_GAP`), so a shard can't
  overwrite its checkpoint forward over unapplied data, and a recovery re-send with shifted chunk
  boundaries (a trigger's tail chunk ends at the timing-dependent head) can no longer
  wedge an ahead shard. The decision is re-checked at **commit time under the writer mutex**,
  so racing writers resolve to no-op/refusal, never a silent checkpoint regression.
  Checkpoints without sequence numbers keep the original exact `from == current` semantics.
- **Lockstep advance** — an empty (no-work) batch still advances the shard's checkpoint, so all
  shards track the same source position (bounds the resume re-read; keeps resume-min trivial).
- **Lineage-ordered head** — the trigger head is the table's `main` ref, not newest-by-wall-clock,
  so a two-writer clock skew can't shadow the lineage tip.
- **Retryable backpressure** — `RESOURCE_EXHAUSTED` (write-admission) is retried, and node work is
  bounded to the admission slots, so a compaction I/O storm sheds load instead of failing the stream.

The **systematic** detector/repairer for lost rows regardless of cause is the reconciliation backstop
([D9](/system/decisions/d09-sync-model.md)).

## Notes

This is a core [correctness guarantee](/quality/overview.md), validated under
[chaos/resilience](/quality/reliability.md) (node crash mid-write, connector crash, catalog outage →
converge to the source with no dup/loss).
