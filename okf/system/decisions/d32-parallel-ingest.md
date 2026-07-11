---
type: Decision
title: D32. Parallel ingest — shard-group connector sets
description: Horizontal ingest scale-out via a set of W independent connector workers, each owning a disjoint shard group (s % W == i), enabled by ordered checkpoints and the window-covering continuity guard; the single connector remains for low-scale syncing.
tags: [decision, adr, ingestion, connector, scale]
timestamp: 2026-07-04T14:22:00
---

# D32. Parallel ingest — shard-group connector sets

**Decision.** Ingest scales out as a **set of W independent connector workers**, each a
full connector process owning the disjoint **shard group** `{s : s % W == i}` for its worker id
`i` (the StatefulSet pod ordinal). Each worker reads the table's changelog, **filters it
executor-side to the rows whose keys route to its owned shards**, maps, and writes ONLY its shards
— including empty lockstep sub-batches, so its group tracks its trigger head. A worker resumes
from the **lineage-min checkpoint over its own group**; its checkpoint namespace is structurally
its group. There are **two connector modes, not two connectors**: the classic single process
(`connector.yaml`, operationally unchanged — the simple low-scale path) and the set
(`connector-set.yaml`); they share every pipeline component and produce identical per-shard
batch ids and placement, so migrating between them is stop-one-start-the-other.

**Why.** One shard, one writer: the per-shard continuity guard
([D31](/system/decisions/d31-ingest-loss-guards.md)) stays sound with **no writer identity,
lease, or coordination protocol** — the design adds no control-plane state at all. The
alternative (per-source-partition assignment with every worker writing every shard) needs
per-connector checkpoint namespacing on the node, breaks per-key ordering whenever a key spans
partitions, and multiplies the guard's writer-race surface. Shard-group ownership preserves
per-key ordering for free (a key routes to one shard; its owner processes the changelog in
`_change_ordinal` order). The enabling foundation is D31's refinement: **ordered
checkpoints + the window-covering guard**. Without it, workers' independent trigger heads put
their groups on incomparable checkpoint lattices, and any regroup (scaling W, or migrating back
to the single connector) wedged on spurious `CHECKPOINT_GAP`s. With it, regrouping is plain
resume-from-min: a new owner's window either covers an inherited shard's position (applies,
content-safe) or ends at/behind it (no-ops) — proven without relying on batch-id dedup, so even
fully **pruned idempotency stores cannot wedge a regroup**.

**Consequences.** Scaling W needs no coordination (roll the StatefulSet); `W ≤ shards` (an extra
worker owns no shards and fails fast, visibly). The under-read gate still counts the UNFILTERED
changelog per worker (a global-window assertion — W distributed counts per window is the cost of
independence). The `safe_checkpoint` prune floor stays each trigger's own resume point: with the
covering guard, pruning can never manufacture a gap, so no cross-group floor coordination is
needed. Running the set and the single connector on one table simultaneously is a
**misconfiguration that fails fast** at the node (two writers on one shard → `CHECKPOINT_GAP`,
loud, no silent loss) — the runbook rule is either/or. Sources must be Iceberg **v2** for the
ordered-checkpoint guarantees (v1 degrades to exact-match semantics — safe, but regroup-wedgeable,
so pin W there). Restoring a subset of shards from backup rewinds checkpoints below pruned floors;
content stays safe under covering, but whole-index restores are the documented default. Per-worker
driver work drops to ~1/W of the rows (executor-side filter); the remaining single-process ceiling
per worker is the Spark driver itself — beyond it, raise W.

**Status.** Accepted. Unit + integration tested against a guard-enforcing node double
(skewed parallel ingest, regroup 2→1, pruned-store regroup — zero gaps) and end-to-end against
two real `growlerdb serve` shards (parallel commit, node-restart durability, no-op resume,
disjoint search coverage). Refines **D10** (the runtime is now a *set* of single-JVM Spark
processes, not one), **D31** (consumes its ordered-checkpoint/covering refinement), and **D9**
(reconcile is shard-scoped and unaffected — any shard's backstop runs regardless of which worker
feeds it). Retires [scale ceiling #1](/quality/known-limitations/scale-ceilings.md).
