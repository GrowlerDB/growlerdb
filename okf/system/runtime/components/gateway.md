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
  A gateway fronts **one index** (`--index`) *or* **every registered index** (`--all-indexes`), routing
  each request to its named index's shard-set — resolved lazily from the control plane on first use and
  hot-reloaded per index ([D35](/system/decisions/d35-multi-index-routing.md)). An empty `index`
  resolves to the endpoint's default/sole index, else is rejected.
- **Scatter-gather** — fans Search/Suggest out to the target shards, merges top-K, **dedupes** by
  composite key (safe mid-reshard), and surfaces an honest `partial` flag; enforces **per-shard
  deadlines** and a **page-fetch ceiling**.
- Fronts Lookup ([hydration](/product/functional/hydration.md)) and Admin; hosts the
  [OpenSearch adapter](/product/interfaces/opensearch-adapter.md); validates auth
  ([authn](/product/functional/auth/login.md)) and enforces **per-index RBAC** — authorization sees the
  resolved target index, so a token scoped to one index can't read another. The node-level tenant
  filter is preserved through routing.

## Notes

Stateless → horizontally scalable; routing is derived, so a gateway is disposable. In `growlerdb-engine`.
