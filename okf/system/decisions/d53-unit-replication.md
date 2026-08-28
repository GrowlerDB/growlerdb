---
type: Decision
title: D53. Per-unit replication factor with cold-tier read-through failover
description: The control plane assigns R holders per placement unit — one primary (sole writer) and R−1 read replicas — over the D52 pool. Writes go to the primary; reads scatter to any live holder; a dead node's units re-place onto survivors while a warm replica keeps serving. Replicas and re-placed owners get their data by reading sealed segments and cold windows read-through from shared object storage — so failover is metadata-bound, not rebuild-bound.
tags: [decision, adr, node, replicas, ha, availability, cold-tier, failover]
timestamp: 2026-07-25T09:00:00
---

# D53. Per-unit replication factor with cold-tier read-through failover

**Decision.** Add a **replication factor R** to the [placement pool](/system/decisions/d52-placement-pool.md).
The [control plane](/system/runtime/components/control-plane.md) assigns **R holders per unit**
(`(index, shard|window)`): exactly one **primary** and **R−1 read replicas**.

- **One writer per unit.** The primary is the sole writer, so the ingest continuity guard
  ([D31](/system/decisions/d31-ingest-loss-guards.md)) and exactly-once resume are preserved unchanged
  — the connector/write path targets the primary holder resolved through the CP.
- **Reads to any live holder.** The [gateway](/system/runtime/components/gateway.md) scatter-gather
  selects a **live, healthy** holder per unit (health-aware, deadline-bounded), so a single node loss
  no longer forces the honest-`partial` degradation of today ([D45](/system/decisions/d45-degraded-vs-error.md)).
- **Failover is re-placement, not rebuild.** A dead node's units **re-place onto survivors** (D33's
  idempotent dead-owner re-placement, generalized). A warm replica keeps answering during the gap, so
  a node kill is a **zero-gap read failover**.

**Replica data path — cold-tier read-through (not standing segment-shipped copies).** A replica or a
re-placed owner obtains its data by **reading the unit's sealed segments and cold windows read-through
from shared object storage**, warming to local NVMe lazily — it never rebuilds from source and never
needs a primary-to-replica copy stream. This leans on the [cold tier](/product/functional/cold-tiering.md):
sealed segments live in the backup/object store and a parked cold window offloads ~97% of index bytes
there, so a unit's data is
durable **independent of any node** and any holder can serve it. Chosen over standing segment-shipped
replicas (the [D14](/system/decisions/d14-replica-sync.md) single-shard mechanism generalized): that
gives faster hot reads but costs R× hot storage and a copy fabric, and the read-through path already
delivers metadata-bound failover with ~free cold replication. The [durability](/product/non-functional/durability.md)
RPO=0 guarantee is untouched — writes still ack on the primary's durable commit; replicas are a
**read/availability** layer, not a second write copy.

**Why.** The node was the second [SPOF](/system/high-availability.md): one primary per unit, no live
replica, reads degrade to partial during a restart, and windowed placement was **primary-only** — a
dead node's windows simply gone until it returned ([windowed replica gap](/quality/known-limitations/windowed-replica-gap.md)).
A replication factor over the D52 pool closes it with the pool's own primitive, and the cold-tier
read-through path is what makes it *fast* — turning "unavailable until rebuilt from source" into
"available, briefly slower, warming." This supersedes the single-shard `serve --replica` segment
shipping ([D14](/system/decisions/d14-replica-sync.md)) as the HA replica model and resolves the
windowed replica gap.

**Consequences.** Replica **read-your-writes lag**: a replica trails the primary by its snapshot
advance / cold-visibility interval, so a read routed to a replica can miss the freshest commits — the
`partial`/freshness surface must stay honest about it, and `require_complete`
([D45](/system/decisions/d45-degraded-vs-error.md)) can pin reads to the primary when a caller needs
zero lag. **Hot-tail read-through cost**: the hottest, most-recently-written segments may not yet be
cold-parked, so a freshly-failed-over replica reads them through object storage until warmed — slower
first queries, bounded by the pre-warm path ([D39](/system/decisions/d39-automatic-cold-tiering.md)).
**Placement map grows** (R× entries, hotter re-placement traffic) — another reason the CP must be HA
first ([D51](/system/decisions/d51-controlplane-ha.md)). **Entitlement**: replicas are additional
running nodes, so the scale-limit entitlement ([D38](/system/decisions/d38-scale-limit-entitlement.md))
counts distinct **primary-holding nodes** — a node is free until it holds a primary — rather than raw
node processes, or HA would eat the free tier. **One-writer enforcement is node-side too**: a holder running from CP assignments keeps an
atomically-swapped primary-holder view and refuses `Write`/`GetCheckpoint` for units it does not hold
as primary (structured `NOT_PRIMARY`; a replica-held read-through window is never overwritten by a
misrouted write) — so a stale or split-brain connector cannot commit to, or fabricate a resume
checkpoint on, a demoted/replica holder (see the [node](/system/runtime/components/node.md)).
**Replica serving requires the object store**, so it becomes a node *capability*: the pool heartbeat
carries a `replica_capable` declaration and placement puts replica units **only on capable nodes** —
a store-less node still hosts primaries but never silently absorbs replicas it could not serve
(HA-G2). And assignment reconcile must be a **two-way sync**: a de-assigned unit is *unloaded*
(mux entries, writer state, read-through scratch), or every node accretes mmaps and scratch for
every unit it ever held (HA-G1). New
[chaos](/quality/reliability.md) drills assert zero-gap failover (kill a unit's primary
under sustained query, assert no `partial` and continued answers with a live replica).

**Status.** Accepted; implementing on `feat/true-ha`. Per-unit R-holder placement, primary write
fencing, read failover, and cold-tier read-through replica serving are built for **windowed** units,
and the same path now covers **hash ordinal shards** — a primary **periodically publishes** a frozen
`backup_replica_snapshot` per held ordinal to object storage (a background loop, skip-if-unchanged) and
a replica opens it read-through (`open_cold_replica`), keyed by ordinal in the pool maps, so a
primary-node kill is a zero-gap read failover for a hash index too (chaos-drilled). Ordinals register
`pool_managed` so co-serving replicas don't each claim every shard, and a leader-only **placement
sweep** self-organizes the pool: it **places a primary** round-robin for every declared hash ordinal
that has none (a node need not have pre-built it) and **fills replicas** to `R` — all **without any
write** — so the operator points N interchangeable nodes at the pool with a uniform config and the CP
distributes primaries + replicas, and a batch-built read-served index or a node join/loss self-heals. Remaining: **continuous
hot shipping** (a hash ordinal's replica trails the primary's newer writes until the next publish — the
immutable-first gap, same as a hot window before it parks) and **dynamic primary assignment**. Supersedes **D14** as the replica model, resolves the
[windowed replica gap](/quality/known-limitations/windowed-replica-gap.md), depends on **D52** (the
pool) and **D51** (an HA control plane). See [high availability](/system/high-availability.md).
