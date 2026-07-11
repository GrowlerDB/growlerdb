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

## Notes

Implemented in `growlerdb-controlplane`. Its persistent state is small; durability is temp+fsync+rename.
