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

**Status.** Accepted (design). Implementation staged in the `true-ha` epic; not yet built. See
[high availability](/system/high-availability.md).
