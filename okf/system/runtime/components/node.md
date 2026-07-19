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

## Notes

One StatefulSet pod per shard in the sharded chart (ordinal = pod index). In `growlerdb-engine`.
Index names are validated at definition parse (`[a-zA-Z0-9_-]`, ≤128 chars) because they become
shard directory paths and object-storage prefixes.
