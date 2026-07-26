---
type: Concept
title: High availability
description: The design for true HA — no single point of failure and graceful failover for every component — built on a replicated control plane and a control-plane-driven placement pool of interchangeable, multi-index, replicated shard-hosting nodes.
tags: [system, ha, availability, control-plane, placement, replicas]
timestamp: 2026-07-25T09:00:00
---

# High availability

GrowlerDB's four components ([architecture](/system/architecture.md)) have uneven availability
today. This concept is the design that closes the gaps — **no single point of failure, graceful
failover everywhere** — the enterprise bar. It is the umbrella over three decisions
([D51](/system/decisions/d51-controlplane-ha.md), [D52](/system/decisions/d52-placement-pool.md),
[D53](/system/decisions/d53-unit-replication.md)) and the `true-ha` backlog epic; the engine work is
**designed, not yet built** (status below).

## Where we stand

| Component | State | HA today | Gap |
|---|---|---|---|
| [Gateway](/system/runtime/components/gateway.md) | Stateless | ✅ `Deployment` + HPA, disposable | none |
| [Connector](/system/runtime/components/connector.md) | Stateless | ✅ connector-set, per-group self-heal ([D32](/system/decisions/d32-parallel-ingest.md)) | single-connector mode is a per-table *ingest* SPOF (search unaffected) |
| [Node](/system/runtime/components/node.md) | Stateful-but-rebuildable | ⚠️ one primary per shard/window, self-heal only | **no live replica** (reads degrade to partial during restart) **and one index per process** (a StatefulSet per index) |
| [Control plane](/system/runtime/components/control-plane.md) | Stateful registry | ❌ `replicas: 1`, single-writer | **hard SPOF** |

Two real gaps: the **control-plane SPOF** and the **node** (no failover *and* a
node-per-index density wall). Both close with the two moves below.

## The design in one picture

```
        ┌─────────────────────────── control plane (N replicas) ───────────────────────────┐
        │   stateless CP pods behind a Service; all durable registry state in a replicated  │
        │   external store (Postgres/etcd). Any replica serves reads; writes are one txn.   │  D51
        └───────▲───────────────────────────────────────────────────────────────▲──────────┘
   RegisterNode │ (heartbeat, ephemeral inventory)          Resolve*Owner / GetIndex │
        ┌───────┴───────────────────────────────────────────────────────────────┴──────────┐
        │                    placement pool — N interchangeable shard-host nodes            │
        │   each node serves a CP-assigned SET of (index, shard|window) UNITS, many indexes │  D52
        │   in one process; CP assigns R holders per unit (1 primary writer + R−1 replicas) │  D53
        └───────▲───────────────────────────────────────────────────────────────▲──────────┘
   writes → primary holder                                    reads → any live holder
   (connector / write path)                                   (gateway scatter-gather, health-aware)
                                              │
                     sealed segments + cold windows live in shared OBJECT STORAGE
                     → a re-placed/replica holder reads-through and answers now (D53)
```

## Move 1 — a replicated control plane ([D51](/system/decisions/d51-controlplane-ha.md))

The CP is a small, low-write [registry](/system/runtime/components/control-plane.md); reads already
run **off the data lock** and the node inventory is already **ephemeral** (nodes re-register within a
heartbeat after a CP restart, [D33](/system/decisions/d33-windowed-topology.md)). So HA needs only a
**durable, replicated registry with fast leader failover** — not a bespoke consensus engine.

The registry's persistence is abstracted behind a backend seam. The **embedded single-writer JSON**
backend stays the default for [single-binary](/system/deployment/single-binary.md) / Compose
(zero-dependency simplicity). A new **externalized backend** (Postgres — the
[deps kustomize](/system/deployment/helm-k8s.md) already provisions it — or etcd) holds all durable
registry state, so the CP runs as **N stateless replicas** behind a Service and delegates consensus
to a mature store. The single-writer advisory lock becomes a store-level transaction / conditional
write; the compare-and-swap placement semantics ([distribution](/system/distribution.md)) carry over
unchanged (`PLACEMENT_CONFLICT` becomes an optimistic-concurrency retry). Rejected: putting the CP on
the hot query path — it stays a registry, never a query router ([D35](/system/decisions/d35-multi-index-routing.md)).

## Move 2 — a universal placement pool of multi-index, replicated nodes ([D52](/system/decisions/d52-placement-pool.md), [D53](/system/decisions/d53-unit-replication.md))

Today `growlerdb serve` binds to **one** index and one `--shard-ordinal` — so every index needs its
own StatefulSet, and a shard has exactly one holder. [D33](/system/decisions/d33-windowed-topology.md)
already broke both assumptions **for windowed indexes**: nodes start empty, register into a
**placement pool**, and the CP assigns them windows on first ask (`RegisterNode` / `ResolveUnitOwner`,
the unit-general placement call D52 generalized the former window-only `ResolveWindowOwner` into), so
any node hosts any window.

**[D52](/system/decisions/d52-placement-pool.md) generalizes that pool to every index and shard
type.** A node becomes a generic **shard host**: it serves the set of `(index, shard|window)`
**units** the CP assigns it — from many indexes, in one process — instead of a single hard-bound
`--index --shard-ordinal`. The gateway already routes per-index ([D35](/system/decisions/d35-multi-index-routing.md)),
so it resolves each unit to whichever node currently holds it. This **eliminates the node-per-index
wall**: one pool of N nodes serves *all* indexes; a hundred small indexes bin-pack onto a handful of
nodes; adding an index is a CP registration, not a new StatefulSet.

**[D53](/system/decisions/d53-unit-replication.md) adds a replication factor** to the pool: the CP
assigns **R holders per unit** — one **primary** (the sole writer, so the continuity guard
[D31](/system/decisions/d31-ingest-loss-guards.md) is preserved) and R−1 **read replicas**. Writes
go to the primary; reads scatter to **any live holder** (health-aware selection). A dead node's units
**re-place onto survivors** (D33's idempotent dead-owner re-placement, now generalized) while a warm
replica keeps serving — so a node loss is a **zero-gap read failover**, not the partial-results
degradation of today. The [durability](/product/non-functional/durability.md) RPO=0 story is unchanged
(writes still ack on the primary's durable commit); replicas are a read/availability layer.

R is a **cluster-wide** setting (`GROWLERDB_REPLICATION_FACTOR`, default `1` = primary-only, the D52
behavior). At `R > 1` a resolve places the replica holders into the durable shard map, and the gateway
reads that holder set to route each read through a **failover node** across a unit's `[primary,
replicas…]` (trying the primary, failing over to a live replica). A replica **learns its assignments
by subscribing to a CP push** (`SubscribeAssignments` — the CP streams each node a fresh snapshot of
its holder set on every placement change) and obtains its data by **reading the parked unit's frozen
sidecars + segments read-through from object storage** (`open_cold_replica`) — no rebuild, no copy
stream. So placing a replica → the node opens it read-through → the gateway fails reads over to it,
end to end. (Cold/parked windows today; continuous hot-window shipping is a later step, and hash-shard
replica serving follows the pool hash path.)

Same primitive, three wins: **multi-index density** (kills node-per-index), **read failover**
(kills the node SPOF), and **rebalancing** (moving a unit is placing a unit).

## The cold tier is what makes failover fast

Standing replicas and re-placement would be worthless if a new holder had to **rebuild from source**
first. The [cold tier](/product/functional/cold-tiering.md) + the layered locator
([D30](/system/decisions/d30-layered-locator.md)) make it a metadata operation instead:

- **Shared durable substrate.** Sealed segments live in the backup/object store, and a parked cold
  window offloads **~97% of index bytes to object storage** (D30). A unit's data is durable in the
  shared store **independent of any node**, so re-placing a dead owner's cold unit is a **metadata-only
  assignment** — the new holder reads the *same* objects through the range-cache; nothing rebuilds.
- **Near-instant bootstrap.** A new replica answers immediately by reading cold bytes read-through,
  warming to local NVMe lazily (the pre-warm path already exists, [D39](/system/decisions/d39-automatic-cold-tiering.md)).
  "Unavailable until rebuilt" becomes "available, briefly slower, warming."
- **~Free cold replication.** Cold replicas hold no local copy — they read-through the shared store;
  replication factor costs local storage only for the **hot tail**.

So the cold tier converts node availability from **rebuild-bound to metadata-bound** — the reason the
pool fails over in seconds. Replica read-path for the hot tail is object-store read-through as well
([D53](/system/decisions/d53-unit-replication.md) chose the cold-tier-led path over standing
segment-shipped copies), so a replica never needs its own local rebuild.

## Availability posture, after

- **Gateway** — unchanged (already HA).
- **Connector** — unchanged; the single-connector *ingest* SPOF is documented and the connector-set
  ([D32](/system/decisions/d32-parallel-ingest.md)) is the HA ingest path.
- **Node** — replicated units + cold-tier-fast re-placement ⇒ a node loss is a zero-gap read
  failover; many indexes share one pool.
- **Control plane** — N stateless replicas over a replicated store ⇒ no SPOF; a replica loss is a
  reconnect, a leader loss is a store failover.

Validated the GrowlerDB way — by [injecting faults and asserting recovery](/quality/reliability.md):
new chaos drills for **node kill under query with a live replica** (assert zero-gap, no `partial`)
and **CP replica/leader loss under mutation** (assert routing + registration continuity).

## Status

**Designed, not yet built.** This concept + D51–D53 are the accepted design; implementation is staged
in the `true-ha` backlog epic (CP backend seam → externalized store → placement pool → per-unit
replication → cold-tier fast-bootstrap → chaos drills), landing as sub-PRs on the `feat/true-ha`
feature branch ([D50](/system/decisions/d50-branching-model.md)). Until it lands, HA is as the
[availability](/product/non-functional/availability.md) NFR and
[sharded HA](/system/deployment/sharded-ha.md) describe: shards spread + PDBs + PV self-heal + honest
partial results, with the CP a single instance.
