---
type: Decision
title: D35. Multi-index routing from one gateway, with per-index RBAC
description: One gateway can front many indexes at once, routing each request to its named index's shard-set resolved lazily from the control plane and hot-reloaded per index; the control plane stays a registry (not a query router); an empty index resolves to a default or the sole served index else is rejected; authorization sees the resolved target index so a token scoped to one index cannot read another, and the engine-level tenant filter is preserved.
tags: [decision, adr, gateway, routing, security, rbac, multi-tenancy]
timestamp: 2026-07-09T12:00:00
---

# D35. Multi-index routing from one gateway, with per-index RBAC

**Decision.** A single **Gateway** can serve **many indexes** at once (task-240). Each request names
its target index (`SearchRequest.index` and the equivalent field now on `Aggregate`/`GetByKey`/
`Suggest`/`Describe`/admin requests); the Gateway **resolves that name to a per-index route** at
entry and operates on it. The old single-index `--index` mode is unchanged (byte-for-byte); a new
`--all-indexes` mode fronts every registered index.

**Routing model.** The Gateway holds a map `resolved-index-name → IndexRoute`, where an `IndexRoute`
bundles what a single-index gateway held in one cell — the shard set + key router (+ optional windowed
descriptors) — plus that index's keyword partition fields, each behind its **own** hot-swap cell. A
`RouteResolver` (the CLI's implementation closes over the control-plane endpoint + node TLS) fetches
the index's `GetIndex` and connects a `Node` per shard on **first** request for that index, then
spawns a **per-index** hot-reloader that reuses the existing swap machinery at per-index granularity.
A resolved route is cached; a `NOT_FOUND` is **negative-cached briefly** (5 s) so a burst of requests
for a bad name doesn't storm the control plane, while an index created moments ago becomes queryable
quickly. A transient resolve failure is `UNAVAILABLE` and is **not** cached (the next request retries).

**Empty-index rule.** An empty `index` field resolves to `default_index` if set; else, if exactly one
index is currently served, that one; else `InvalidArgument("index required; endpoint serves N
indexes")`. Single-index mode keeps task-99 scoping exactly: an empty or matching name resolves to the
served index; a *different* name is `NOT_FOUND`. The OpenSearch `/_search` (`_all`) path maps to the
empty/default index; `/{index}/_search` routes to that index.

**Auth boundary change — per-index RBAC (not deferred).** Authorization now sees the request's
**resolved target index**. The auth context carries an optional `index` and an **index allowlist**
stamped from the token's `indexes` claim (JWT) or a `KeyIdentity` (API key). When the allowlist is
non-empty, a request whose resolved index is not in it is `PermissionDenied` **before any shard is
touched** — so a token valid for index A cannot read index B. An empty allowlist is unrestricted
(back-compat: existing tokens keep working). The allowlist check is *additive* to the role→scope
check, and index-agnostic control-plane operations (no resolved index) are gated by scope alone.

**Tenant isolation preserved.** Tenant scoping stays enforced **at the node/shard** (search rewrites a
mandatory `AND tenant_field = verified_tenant`; hydration post-filters against the authoritative row;
suggest fails closed on a tenant-scoped index), driven by the verified `x-growlerdb-tenant` metadata
the authn boundary stamps. Multi-index routing forwards that trusted metadata to each route's shards
unchanged, so a request routed to a resolved index applies that index's own tenant filter — cross-
tenant reads stay impossible even through a resolved route (proved by a multi-index tenant-isolation
test).

**Why.** A deployment with many indexes shouldn't need one gateway process per index (N gateways, N
console origins, N ports). One stateless gateway fronting all indexes, routing per request, matches
how the control plane already models routing (per index, not per gateway) and how the console's index
selector already works. Landing per-index RBAC in the *same* change is a GA/security requirement: a
multi-index endpoint without per-index authorization would let any authorized reader read every
index.

**Rejected alternative — control plane as query router.** We considered making the control plane a
routing *proxy* that forwards queries to the owning shards. Rejected: it would put the control plane
(a lightweight registry) on the hot query path, make it a throughput bottleneck and a new failure
domain for reads, and duplicate the gateway's scatter-gather/merge. The control plane stays a
**registry** (`GetIndex`/`ListIndexes`); the gateway remains the query router and resolves routes
*from* the registry, caching them.

**Lifecycle bounds.** Node connections are **lazy** (a down shard never fails the resolve; the channel
re-resolves DNS on reconnect). Per-index reloaders poll `GetIndex` on the existing interval and swap
ordinal shards or windowed windows in place; a read error keeps the current topology (an outage must
not blank a route). Multi-index **readiness** is the control plane's reachability, *not* any one index
resolving — a fresh cluster with no indexes still serves. Routes and negative-cache entries are held
for the process lifetime (bounded by the set of indexes requested); there is no eviction yet (a
follow-up if an endpoint fronts an unbounded churn of short-lived indexes).

**Consequences.** The live-CP multi-index path carries no partition-field pruning hints (like the
existing single-index live-CP path), so a partition-pinned search fans out instead of pruning —
correct, just unoptimized; wiring partition fields through `GetIndex` is a follow-up. Distributed
write/admin RPCs (reindex/alter/compact/backup) still resolve per index but remain single-shard-only
(honest `Unimplemented` on a multi-shard index), unchanged by this decision.

**Status.** Accepted (task-240). `Gateway` holds `single` (static, single-index) or `routes` + a
`RouteResolver` (multi-index); every read/write handler resolves by index at entry via
`guard_and_resolve`; `RbacPolicy` enforces the index allowlist; the CLI `gateway --all-indexes` builds
a control-plane-backed resolver; the compose gateway fronts all indexes. Depends on the auth seam
(task-35/36) and control-plane registry (task-49/77/219).
