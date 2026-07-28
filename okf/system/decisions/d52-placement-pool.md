---
type: Decision
title: D52. Universal placement pool — interchangeable, multi-index shard-hosting nodes
description: Generalize D33's windowed placement pool to every index and shard type. A node becomes a generic shard host that serves a control-plane-assigned SET of (index, shard|window) units from many indexes in one process, instead of binding to a single --index --shard-ordinal. Eliminates the node-per-index StatefulSet wall and provides the substrate for per-unit replication.
tags: [decision, adr, node, placement, control-plane, ha, multi-tenancy, density]
timestamp: 2026-07-25T09:00:00
---

# D52. Universal placement pool — interchangeable, multi-index shard-hosting nodes

**Decision.** Generalize [D33](/system/decisions/d33-windowed-topology.md)'s control-plane-driven
**placement pool** from windowed indexes to **every index and shard type**. A
[node](/system/runtime/components/node.md) becomes a generic **shard host**: it registers into the
pool (`RegisterNode`) and serves the **set of assignment units** the control plane gives it, where a
**unit** is an `(index, shard-ordinal)` for a hash-sharded index or an `(index, window)` for a
windowed one. One node process serves **many units from many indexes** — replacing today's hard bind
to a single `growlerdb serve --index X --shards N --shard-ordinal K`.

- **Nodes start empty and interchangeable**, no fixed index or ordinal. The CP places units on
  live nodes (extending `ResolveWindowOwner` to a unit-general `ResolveUnitOwner`, least-loaded, idempotent,
  dead-owner re-placement), from the durable `bucket_owners` / window maps
  ([distribution](/system/distribution.md)).
- **The node loads/creates a unit on assignment** — a windowed unit create-on-first-write as today
  ([D33](/system/decisions/d33-windowed-topology.md)); a hash unit loads its shard from the shared
  store (cold-tier read-through, [D53](/system/decisions/d53-unit-replication.md)) or builds it
  filtered from source. It publishes each served unit live into a **per-unit search/suggest/write
  multiplexer** (generalizing the windowed mux and `Gateway::swap_windowed`) and re-announces its
  served units each heartbeat.
- **The [gateway](/system/runtime/components/gateway.md) routes per-unit.** It already resolves per
  index ([D35](/system/decisions/d35-multi-index-routing.md)); it now resolves each `(index, shard|window)`
  to the node(s) currently holding it and hot-reloads on the announced set — no restart, no per-index
  gateway process.

**Why.** `serve` binding to one index is the **node-per-index wall**: every index needs its own
StatefulSet (N indexes → N StatefulSets, N PVCs, N pods even for tiny indexes), which is untenable for
an enterprise with many small indexes and blocks dense multi-tenancy. D33 already proved
interchangeable CP-assigned nodes work for windows; extending the same pool to all indexes makes
**density a bin-packing decision the CP makes**, not a deploy-time topology — a hundred small indexes
pack onto a handful of nodes, and adding an index is a CP registration. The same placement primitive
is also the substrate for **replication** ([D53](/system/decisions/d53-unit-replication.md)) and
**rebalancing** (moving a unit is placing a unit) — one mechanism, three wins.

**Rejected alternatives.** (a) *Per-index StatefulSet + a bolt-on replica set* — leaves the density
wall standing and duplicates placement logic per index. (b) *A static multi-index node* (a node
serves a fixed list of indexes from flags) — avoids the CP but reintroduces build-time topology and
can't rebalance or fail over, the exact inflexibility D33 rejected for `window % N`. (c) *CP as router*
— reaffirmed out ([D35](/system/decisions/d35-multi-index-routing.md), [D51](/system/decisions/d51-controlplane-ha.md)).

**Consequences.** **Security/trust boundary is preserved and reused**: the node still carries no
per-user auth in distributed mode — authn/RBAC/tenant enforcement stay at the gateway, and the
`GROWLERDB_SERVICE_TOKEN` gates every mesh RPC ([node trust boundary](/system/runtime/components/node.md)).
Per-index RBAC ([D35](/system/decisions/d35-multi-index-routing.md)) and node-side tenant fail-closed
apply per resolved unit unchanged. **Isolation caveat**: co-tenant units now share a process, so the
node's resource budgets (`GROWLERDB_MAX_HEAVY_READS`, write in-flight cap) become **cross-index** and
need per-unit fairness so one index can't starve another — a noisy-neighbor concern that per-index
StatefulSets got for free (called out in [scale ceilings](/quality/known-limitations/scale-ceilings.md)).
The Helm chart gains a **pool mode** (a node `Deployment`/StatefulSet sized to capacity, not to one
index's shard count) alongside the existing per-index chart, which stays valid for single-index
deployments. Connector routing already resolves owners via the CP, so it follows the generalized
placement with no protocol change. Extends **D33** (windowed placement → universal), depends on
**D35** (per-index gateway routing) and **D51** (an HA CP to hold the larger, hotter placement map).

**Status.** Accepted; implementing on `feat/true-ha`. The pool now serves **both** unit kinds from one
process — windowed indexes (routed on `window`) and hash-sharded indexes (routed on the `shard`
ordinal), each index declaring its kind via `SharedIndexKinds` so the read/write multiplexers pick the
selector — with per-unit replication/failover ([D53](/system/decisions/d53-unit-replication.md)) built
for windowed units and following the same path for hash ordinals. The pool now **self-organizes**: a
leader-only placement sweep distributes hash primaries round-robin, and a node the CP assigns a primary
it doesn't hold **builds it on assignment** from source (single-shard today) — so the operator points N
interchangeable nodes at the pool with a **uniform config** and the CP designates who owns what, no
per-node build/primary designation. **Cold-start fast path:** a *never-placed* primary is placed as
soon as a brief initial settle clears (`INITIAL_PLACEMENT_SETTLE_MS`, a few seconds — long enough for
co-booting nodes to register so primaries round-robin balanced), rather than waiting out the full
liveness grace ([`NODE_HEARTBEAT_TTL_MS`](/system/decisions/d53-unit-replication.md), ~30 s, which
still gates *re-placement* of already-held units for anti-flap); the sweep ticks several times per
heartbeat interval so a fresh pool converges in seconds. Live-verified end to end (define-only nodes →
round-robin placement → build-on-assignment → replica read-through → zero-gap failover on a primary
kill). See
[node](/system/runtime/components/node.md) and [high availability](/system/high-availability.md).
