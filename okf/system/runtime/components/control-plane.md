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

## Notes

Implemented in `growlerdb-controlplane`. Its persistent state is small; durability is temp+fsync+rename.
