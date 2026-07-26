---
type: Component
title: Node
description: Builds and serves an index (or a shard/window); stateful but rebuildable.
tags: [component, node, index, serve]
resource: /crates/growlerdb-engine
timestamp: 2026-07-04T14:22:00
---

# Node

Builds an index from an Iceberg table and **serves** it — search, suggest, lookup, admin, and the
Write endpoint for ingestion. **Stateful but rebuildable**: its local
[index store](/system/storage/index-store.md) can be restored from backup or rebuilt from Iceberg.

## Responsibilities

- **Build** a full index or a specific `--shards N --shard-ordinal K` partition (filtered by the
  [router](/system/distribution.md)).
- **Serve** over gRPC + REST; register to the [control plane](/system/runtime/components/control-plane.md)
  at a routable advertise address.
- **Windowed serve** — serve per-window multiplexers; **replica** mode — a read-only surface that
  hot-swaps on a snapshot advance.
- Health-driven [auto-compaction](/product/functional/index-management/compact.md); the source-lineage
  guard serves degraded on a recreated source.

## Trust boundary

A Node's gRPC surface carries **no per-user auth of its own** in distributed mode — authn, RBAC,
and tenant enforcement all live at the [gateway](/system/runtime/components/gateway.md) (tenant
scoping on reads additionally fails closed node-side). The Node's boundary is the **shared
service token** (`GROWLERDB_SERVICE_TOKEN` / `--service-token`): when configured, every
data-plane RPC (Write/Search/Lookup/Suggest/Admin/System, all serve modes) must present it, the
same token the control plane already enforces. All mesh callers — gateway, control plane,
connector, the ops CLI — stamp it from the same env var; the Helm chart wires it from
`credentials.serviceToken`. Unset ⇒ the data plane is **open** and the Node logs a loud warning:
acceptable only single-node or behind strict network isolation. **Deployment requirement: never
expose a Node port beyond the cluster network; the token is defense-in-depth behind that, not a
substitute for it.**

## Admission control

Heavy read ops — `Export` and `Aggregate`, full scans on the blocking pool — share one
node-wide budget: `GROWLERDB_MAX_HEAVY_READS` concurrent (default 8; Helm `node.maxHeavyReads`),
across all served shards and windows. Saturation load-sheds with `RESOURCE_EXHAUSTED` so a flood
of exports can't starve every other query's blocking work; the permit is held for an export's
whole stream. Writes have their own in-flight cap (`--max-inflight`).

## Availability & the designed pool model

A plain `serve` process **binds to one index** and one `--shard-ordinal` (one primary, no live
replica) — so every such index needs its own StatefulSet and a node loss degrades reads to honest
partial. The successor generalizes [D33](/system/decisions/d33-windowed-topology.md)'s windowed
placement pool to **every index**: a node becomes an interchangeable **shard host** serving a
CP-assigned **set of `(index, shard|window)` units** from many indexes in one process
([D52](/system/decisions/d52-placement-pool.md), kills node-per-index), and the CP assigns **R holders
per unit** — one primary writer + read replicas that hydrate read-through from shared object storage
([D53](/system/decisions/d53-unit-replication.md), zero-gap failover). See
[high availability](/system/high-availability.md).

**Landing incrementally.** `growlerdb serve-pool --index A --index B …` already serves the windows of
**many windowed indexes from one process** over one gRPC endpoint — reads dispatch per `(index,
window)` through the pool multiplexers, so the node-per-index wall is gone for pre-built windowed
indexes. A request addressed to a unit the node doesn't serve is refused with the structured
`UNIT_NOT_SERVED` detail (`FAILED_PRECONDITION`) — a stale-route signal, not a client error, that the
gateway's read failover matches to try the unit's next holder (a missing selector stays
`INVALID_ARGUMENT`: no holder could satisfy it). With `--register` it heartbeats into the **index-agnostic placement pool** (`RegisterNode`
now carries only the endpoint) and announces every served index's windows, so a cluster gateway can
route to it. **Writes** land the same way: `serve-pool` mounts a `PoolWriteService` that dispatches
each `Write` / `GetCheckpoint` on the `(index, window)` selector (`WriteRequest` /
`GetCheckpointRequest` carry an `index`; empty = the sole served index, a drop-in for a single-index
node) to that index's windowed writer, which creates the window shard on first write and publishes it
into the same live maps the read multiplexers front — so a just-ingested window is queryable and
re-announced with no restart. The connector stamps the index on each sub-batch, so it streams ingest
to a pool node exactly as to a single-index windowed node.

**Replica serving** ([D53](/system/decisions/d53-unit-replication.md)): a pool node registered into the
pool also **subscribes to CP assignment pushes** (`SubscribeAssignments`) and, for each **replica**
window the CP assigns it, fetches the parked window's cold marker from object storage and opens it
**read-through** (`open_cold_replica`) into the same per-index maps the read multiplexers front — so a
placed replica starts serving with no rebuild and no copy stream, and the gateway's failover routes
reach it. Needs the object store (`GROWLERDB_BACKUP_BUCKET`); cold/parked windows today. Still to come:
**dynamic assignment** (a node loading a primary unit it wasn't started with), hot-window replica
shipping, and hash-shard replica serving.

## Notes

One StatefulSet pod per shard in the sharded chart (ordinal = pod index). In `growlerdb-engine`.
Index names are validated at definition parse (`[a-zA-Z0-9_-]`, ≤128 chars) because they become
shard directory paths and object-storage prefixes.
