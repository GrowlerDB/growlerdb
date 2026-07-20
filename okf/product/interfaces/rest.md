---
type: Interface
title: REST API
description: The /v1/* HTTP+JSON Engine API served by the gateway (and a bare node).
tags: [interface, rest, api]
resource: /docs/rest-api.md
timestamp: 2026-07-20T00:00:00
---

# REST API

The `/v1/*` HTTP+JSON surface of the Engine API, served by the [gateway](/system/runtime/components/gateway.md)
(and by a bare [node](/system/runtime/components/node.md) for its own index). Same-origin with the
[console UI](/product/interfaces/ui.md), which the engine serves. Every request carries a bearer token
the gateway validates.

## Route groups

- **Query:** `POST /v1/search`, `/v1/keys:get` (hydration), `/v1/suggest`, `/v1/facets`, `/v1/explain`.
- **Index admin:** `/v1/index:describe`, `:reindex`, `:alter`, `:compact`, `:backup`,
  `:backup-status`, `:activity`; `/v1/source:describe`; `/v1/cold` (storage tiers).
- **Ingestion:** `GET /v1/ingestion`, `/v1/ingestion/{name}`.
- **Identity & access:** `/v1/config` (auth mode, runtime config), `/v1/me`, `POST /v1/login`,
  `/v1/users`, `/v1/roles`.
- **Observability:** `/v1/stats/query`, `/v1/stats/query_range`, `/v1/stats/alerts`, `/v1/alerts`.

## Notes

The full endpoint reference is in [docs/rest-api.md](/docs/rest-api.md). Behavior of each capability
is described under [product/functional](/product/functional/index.md); the wire types mirror the
[gRPC API](/product/interfaces/grpc.md). The same listener also serves the
[MCP Streamable HTTP transport](/product/interfaces/mcp-server.md) at `POST /mcp` — the agent face
of this query surface.
