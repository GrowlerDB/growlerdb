---
type: Component
title: Node
description: Builds and serves an index (or a shard/window); stateful but rebuildable.
tags: [component, node, index, serve]
resource: /crates/growlerdb-engine
timestamp: 2026-07-26T12:00:00
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

**Landing incrementally.** `growlerdb serve-pool --index A --index B …` already serves **many indexes
from one process** over one gRPC endpoint — both **windowed** indexes (units are time windows, routed
on `window`) and **hash-sharded** indexes (units are ordinal shards, routed on the `shard` ordinal) —
so the node-per-index wall is gone for pre-built indexes of either kind. Each served index registers
its **unit kind** (`SharedIndexKinds`), and the pool read/write multiplexers pick the selector per
index accordingly; the maps are `i64`-keyed either way (a window id or an `ordinal as i64`), so one
endpoint fronts both. A request addressed to a unit the node doesn't serve is refused with the
structured `UNIT_NOT_SERVED` detail (`FAILED_PRECONDITION`) — a stale-route signal, not a client
error, that the gateway's read failover matches to try the unit's next holder (a missing selector
stays `INVALID_ARGUMENT`: no holder could satisfy it). With `--register` it heartbeats into the
**index-agnostic placement pool** (`RegisterNode` now carries only the endpoint) and announces each
served index's units — a windowed index its windows, a hash index its held ordinals + total shard
count — so a cluster gateway can route to it. **Writes** land the same way: `serve-pool` mounts a
`PoolWriteService` that dispatches each `Write` / `GetCheckpoint` on the `index` selector, then routes
within the index on `window` (windowed) or the `shard` ordinal (hash) — `WriteRequest` /
`GetCheckpointRequest` carry both `index` (empty = the sole served index, a drop-in for a single-index
node) and `shard`. A windowed write goes to that index's windowed writer, which creates the window
shard on first write; a hash write goes straight to the ordinal's single-shard writer (the ordinal set
is fixed at boot — a hash ordinal is built offline and CP-placed, not created on first write). Both
publish into the same live maps the read multiplexers front, so a just-ingested unit is queryable with
no restart. The connector stamps the index — and, for a hash index, the ordinal — on each sub-batch,
so it streams ingest to a pool node exactly as to a single-index node.

**Write fencing** ([D53](/system/decisions/d53-unit-replication.md)'s one-writer-per-unit, node
side): when the node runs from CP assignments (`--register`), every pushed assignment snapshot
atomically swaps its **primary-holder view**, and a `Write` / `GetCheckpoint` addressed to an
`(index, window)` the node does not hold as **primary** is refused with the structured `NOT_PRIMARY`
detail (`FAILED_PRECONDITION` — distinct from `UNIT_NOT_SERVED`, so callers can tell *wrong node
for writes* from *not serving*; the connector treats it as non-retryable and re-resolves placement).
A refused write creates no shard and touches no read map; a refused checkpoint read never fabricates
an empty resume point on the wrong node. Defense in depth: a window the node serves only
**read-through** (a replica-held parked snapshot) is never overwritten by a misrouted first write —
that's a `WINDOW_PARKED` refusal, and the replica reconcile inserts its cold entry only if the
window is still absent, so a hot window created meanwhile always wins. When a unit's primary moves
away, the node starts refusing on the very next snapshot — and **unloads** the unit (below).
Standalone (no `--register`) the fence is unrestricted — classic `serve` / `serve-pool`
create-on-first-write is unchanged.

**Replica serving & capability** ([D53](/system/decisions/d53-unit-replication.md)): a pool node
registered into the pool also **subscribes to CP assignment pushes** (`SubscribeAssignments`; first
subscribe waits for the first successful registration, reconnects with jittered exponential backoff)
and, for each **replica** unit the CP assigns it, fetches the unit's cold marker from object storage
and opens it **read-through** (`open_cold_replica`) into the same per-index maps the read multiplexers
front — so a placed replica starts serving with no rebuild and no copy stream, and the gateway's
failover routes reach it. This covers **both** unit kinds: a windowed index's parked **windows**
(marker at `cold/{index}/w{window}`) and a hash index's **ordinal shards** (`cold/{index}/{ordinal}`,
a frozen `backup_replica_snapshot` of the shard — hash ordinals never park, so the primary publishes a
point-in-time snapshot the replica serves read-through). The reconcile keys both by the same
`i64`-indexed maps (an index is all one kind), for either role (a parked window / a published shard is
served read-through whether this node is its primary or a replica). Serving replicas needs the object
store (`GROWLERDB_BACKUP_BUCKET` / `GROWLERDB_OBJECT_STORE_FS`), so the node's pool heartbeat carries a
**`replica_capable`** declaration — true only when one is configured — and the CP **places replica
units only on capable nodes** (an old binary or a store-less `--register` never silently absorbs
replicas it could not serve; primaries are placed by load alone). A pool node registers its ordinals
**`pool_managed`** (the CP places them via `ResolveUnitOwner`, an empty announced set claiming nothing)
so co-serving replicas don't each grab every shard as primary. Still to come: **dynamic assignment** (a
node loading a primary unit it wasn't started with), and **continuous hot shipping** — a hash ordinal's
replica serves the last published snapshot, so it trails the primary's newer writes until the next
backup (immutable-first, the same gap a hot window has before it parks).

**Unit unload** (HA-G1): assignment reconcile is a two-way sync. Each pushed snapshot's de-assigned
set — units a *previous* snapshot assigned to this node (either role) that the new one doesn't — is
**unloaded**: the window's read-mux entries and writer state are dropped (in-flight requests hold the
shard `Arc`, so removal only unpublishes; mmaps close when the last request finishes) and the unit's
`.replica/{index}/w{N}` read-through scratch is deleted after a short drain grace (a re-assignment
later just re-downloads the sidecars). Boot windows the CP never assigned are **not** unloaded —
only assignment-driven units are. The first snapshot after boot also sweeps `.replica` scratch
orphaned by previous runs (only then: a blind sweep at startup would race the subscription).

**Boot quarantine & shutdown** (HA-G4): a window shard that fails to *open* at `serve-pool` boot is
**quarantined** — logged loudly and skipped, the rest of the process serves; the unit reads as
`UNIT_NOT_SERVED` and a registered pool's CP re-places it (misconfiguration such as a bad
`--data-dir` or unresolvable definition still fails the boot). `serve-pool` shuts down gracefully on
**SIGINT or SIGTERM** (plain Kubernetes sends SIGTERM; only the Helm preStop sends SIGINT). There is
no deregistration RPC — a stopping node simply ceases to heartbeat and ages out of the pool's
liveness TTL, after which the dead-owner sweeper re-places its units.

## Notes

One StatefulSet pod per shard in the sharded chart (ordinal = pod index). In `growlerdb-engine`.
Index names are validated at definition parse (`[a-zA-Z0-9_-]`, ≤128 chars) because they become
shard directory paths and object-storage prefixes.
