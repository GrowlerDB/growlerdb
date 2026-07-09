---
type: Component
title: Gateway
description: The stateless public Engine API — routes, scatter-gathers, merges, and serves the console.
tags: [component, gateway, api]
resource: /crates/growlerdb-engine
timestamp: 2026-07-04T14:22:00
---

# Gateway

The **stateless** public [Engine API](/product/interfaces/rest.md) (gRPC + REST). It fronts the
[nodes](/system/runtime/components/node.md), and serves the [console UI](/system/runtime/components/console-ui.md).

## Responsibilities

- **Routing** — builds shard routing from the [control plane](/system/runtime/components/control-plane.md)
  (`GetIndex`), with a bounded startup wait and periodic **hot-reload** (swap on a real topology change).
- **Scatter-gather** — fans Search/Suggest out to the target shards, merges top-K, **dedupes** by
  composite key (safe mid-reshard), and surfaces an honest `partial` flag; enforces **per-shard
  deadlines** and a **page-fetch ceiling**.
- Fronts Lookup ([hydration](/product/functional/hydration.md)) and Admin; hosts the
  [OpenSearch adapter](/product/interfaces/opensearch-adapter.md); validates auth
  ([authn](/product/functional/auth/login.md)).

## Notes

Stateless → horizontally scalable; routing is derived, so a gateway is disposable. In `growlerdb-engine`.
