---
type: Concept
title: Architecture
description: The overall shape — components, the data flow, and the JVM/Rust boundary.
tags: [system, architecture]
timestamp: 2026-07-04T14:22:00
---

# Architecture

GrowlerDB is a small set of cooperating components over Apache Iceberg + object storage. The core
engine is Rust; the ingestion connector and SQL adapters are JVM; the two meet over **gRPC**.

![GrowlerDB architecture — ingest path (data → Iceberg → Spark connector → index nodes building Tantivy segments) and query path (client → gateway scatter-gather → nodes → hydrate keys back to authoritative Iceberg rows), with the control plane as routing truth and OpenTelemetry to the LGTM stack](../../docs/img/architecture.png)

## Components

- **[Control plane](/system/runtime/components/control-plane.md)** — lightweight registry (indexes,
  shards, routing/bucket map, tokens, roles); the source of routing truth.
- **[Gateway](/system/runtime/components/gateway.md)** — stateless public Engine API; routes and
  scatter-gathers to nodes, merges top-K, serves the console.
- **[Node](/system/runtime/components/node.md)** — stateful-but-rebuildable; builds and serves an
  index (or a shard/window), exposes search + Write gRPC.
- **[Connector](/system/runtime/components/connector.md)** — stateless Spark worker reading the Iceberg
  changelog and streaming batches to a node's Write service.

## Data flow

```
Iceberg source ──(connector: changelog/append)──▶ Node (build local Tantivy index)
Query ──▶ Gateway ──(scatter-gather)──▶ Nodes ──▶ ranked coordinates ──▶ merge ──▶ client
Coordinates ──(hydrate: keys:get)──▶ authoritative Iceberg rows
```

Find-by-text in the [local index store](/system/storage/index-store.md); fetch-by-key from Iceberg.
Sharding/routing and the scatter-gather are detailed in
[distribution](/system/distribution.md); query internals in
[query execution](/system/query-execution.md).

## The JVM/Rust boundary

The Rust engine and the JVM connector/SQL adapters communicate over gRPC — a stable, language-neutral
contract (`crates/growlerdb-proto`) rather than a native FFI. Deployment shapes are in
[deployment](/system/deployment/index.md).
