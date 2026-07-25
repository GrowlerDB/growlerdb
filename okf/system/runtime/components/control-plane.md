---
type: Component
title: Control plane
description: The cluster registry — indexes, shards, routing, tokens, roles — and the source of routing truth.
tags: [component, control-plane, registry]
resource: /crates/growlerdb-controlplane
timestamp: 2026-07-04T14:22:00
---

# Control plane

A lightweight gRPC service (the `ControlPlane` API) holding the cluster's registry: index definitions,
shard/ordinal assignments, the [bucket routing map](/system/distribution.md), API
[tokens](/product/functional/auth/tokens.md), [role bindings](/product/functional/rbac-and-tenancy.md),
built-in credentials, session epochs, and the per-index activity log.

## Responsibilities

- **Vends routing** — [gateways](/system/runtime/components/gateway.md) build their shard routing from
  `GetIndex` (primaries + bucket map) and hot-reload on change.
- **Single writer** — an exclusive advisory lock; mutations apply in memory then persist **off the
  data lock** (registry JSON + `.prev` fallback + sidecars for activity/sessions), so routing reads
  never block on fsync.
- Serves auth-state lookups (O(1) token hash index) with a consistent lock order.

## Internal-RPC credential

The internal RPCs (registration, shard-map reads, placement) are a service-to-service layer, distinct
from the user [RBAC](/product/functional/rbac-and-tenancy.md) that governs data-plane requests. They can
be gated with a shared **service token** (`GROWLERDB_SERVICE_TOKEN`): when set, every RPC must carry the
matching token (constant-time checked) or is rejected — closing the internal RPCs to callers outside the
mesh, independent of the user-auth mode. Unset ⇒ open (local dev). The control plane can also serve over
[TLS/mTLS](/product/functional/auth/mtls.md), optional and off by default.

## Availability

By default the CP is a **single instance** (`replicas: 1`) — the cluster's one hard SPOF. HA
([D51](/system/decisions/d51-controlplane-ha.md)) moves durable state behind a **backend seam**: the
embedded single-writer JSON store stays the default for single-binary/Compose, and an **externalized
backend** (Postgres) holds all durable registry state so the CP runs as **N stateless replicas**
behind a Service, delegating consensus to the store. Reads are already off the data lock and the node
inventory is already ephemeral, so this is a store swap, not a new consensus engine.

**Running it:** `control-plane --registry-postgres <url>` (env `GROWLERDB_REGISTRY_POSTGRES`; needs a
build with the `postgres` feature) points every replica at one shared Postgres. Each starts as a warm
**standby**: it serves reads and continuously reloads its in-memory catalog when the store's
version advances (the leader wrote). Exactly one replica holds a **session-level advisory lock** — the
leader — and is the sole writer; a non-leader refuses every persist so it can never corrupt the store.
Coordination is **active-passive**: readiness tracks leadership, so `/readyz` is `200` only on the
leader and the Service routes all traffic to it while standbys stay warm but out of rotation. When the
leader dies, Postgres releases the lock, a standby wins it within a fraction of a second (`kill -9`
→ promotion verified live), flips to ready, and joins the Service. Standbys **serving reads** directly
(active-reads, per the D51 diagram) is a follow-up. See [high availability](/system/high-availability.md).

## Notes

Implemented in `growlerdb-controlplane`. Its persistent state is small; durability is temp+fsync+rename.
