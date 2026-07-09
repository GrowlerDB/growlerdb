---
type: Interface
title: gRPC API
description: The Protobuf/gRPC services that clients, connectors, and internal components use.
tags: [interface, grpc, api, proto]
resource: /crates/growlerdb-proto/proto
timestamp: 2026-07-04T14:22:00
---

# gRPC API

The Protobuf/gRPC surface (definitions in `crates/growlerdb-proto`). It is the primary programmatic
interface — the [REST API](/product/interfaces/rest.md) is a JSON facade over the same operations, and
the [client SDKs](/product/interfaces/client-sdks.md) + [connectors](/product/interfaces/sql-udfs.md)
speak it directly.

## Services

- **Search** — full-text query, returning ranked coordinates.
- **Suggest** — autocomplete over field values.
- **Lookup** — hydration: coordinates → authoritative rows.
- **Admin** — index management (describe, reindex, alter, compact, backup, aliases).
- **Write** — ingestion: the [connector](/system/runtime/components/connector.md) streams document
  batches to a [node](/system/runtime/components/node.md).
- **ControlPlane** — the cluster registry (indexes, shards, routing, tokens, roles).
- **System** — health, readiness, and metadata.

## Notes

The [gateway](/system/runtime/components/gateway.md) fronts Search/Suggest/Lookup/Admin and
scatter-gathers across shards; Write is served by nodes; ControlPlane by the
[control plane](/system/runtime/components/control-plane.md).
