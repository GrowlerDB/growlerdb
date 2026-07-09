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

## Notes

One StatefulSet pod per shard in the sharded chart (ordinal = pod index). In `growlerdb-engine`.
