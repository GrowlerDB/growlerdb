---
type: Concept
title: Repository layout
description: The Cargo workspace and subprojects that make up the codebase.
tags: [system, repo, codebase, crates]
timestamp: 2026-07-04T14:22:00
---

# Repository layout

A single repository: a **Cargo workspace** of Rust crates plus JVM, web, and client subprojects. (This
is the codebase; the project *touchpoint* is [product/interfaces/git-repo](/product/interfaces/git-repo.md).)

## Rust crates (`crates/`)

- **growlerdb-core** — shared types: query AST, routing/buckets, timestamp/windowing, index
  definition, durable write.
- **growlerdb-proto** — Protobuf/gRPC definitions (the JVM/Rust contract).
- **growlerdb-index** — the [local index store](/system/storage/index-store.md): Tantivy segments,
  redb locators, cold bundles, hotcache.
- **growlerdb-source** — the Iceberg source reader (scans + hydration reads).
- **growlerdb-backup** — backup/restore + replica segment shipping.
- **growlerdb-controlplane** — the registry (indexes, shards, tokens, roles, activity).
- **growlerdb-engine** — gateway + node services (search, admin, write, routing, REST, authn,
  OpenSearch adapter).
- **growlerdb-telemetry** — OpenTelemetry instrumentation + SLIs.
- **growlerdb-client** — the Rust gRPC client.
- **growlerdb-cli** — the `growlerdb` binary.

## Subprojects

- **`ui/`** — the Svelte console. **`clients/python`** — the Python SDK.
- **`connector/`** — the Spark changelog connector + SQL search UDF (Java). **`connector-trino/`** —
  the Trino connector (Java, separate JDK).
- **`deploy/`** — Compose, Helm, k8s manifests (+ IaC). **`docs/`** — the Jekyll docs site.
  **`bench/`** — the benchmark harness. **`okf/`** — this knowledge base.
