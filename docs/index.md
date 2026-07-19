---
title: Home
layout: default
nav_order: 1
---

# GrowlerDB
{: .fs-9 }

Open-source **retrieval engine — full-text, vector, and hybrid search over your data**. Your source
(Apache Iceberg today) stays the system of record; GrowlerDB keeps a fast derived index and returns
the matching **primary keys**, which hydrate back to the authoritative rows.
{: .fs-6 .fw-300 }

[Get started](getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/GrowlerDB/growlerdb){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## The model in one minute

- You define an **index** over an Iceberg source table — which columns to index, the composite
  key, and an optional `tenant_field`. A connector keeps the index in sync with the table.
- A **search** runs against the derived index and returns **document coordinates** (the composite
  key) + scores — not documents.
- Your client (or built-in hydration) then **fetches the authoritative rows from Iceberg by key**
  (`POST /v1/keys:get`), governed by the catalog. The lake stays the source of truth.

This is the inverse of a search engine that *owns* its documents — and it's why GrowlerDB layers
onto a lakehouse instead of duplicating it.

## Documentation

New here? Start with **[Getting started](getting-started)** (Compose → first search → hydrate →
console). The rest of the site is grouped by what you're trying to do:

### Start here

| Page | What it covers |
|---|---|
| [Getting started](getting-started) | Compose stack → your first search → hydrate → console. |
| [Install & run modes](install) | Build from source; embedded, `serve`, `gateway`, `control-plane`. |

### Configure & connect

| Page | What it covers |
|---|---|
| [Configuration](configuration) | CLI flags, env vars, and the index-definition YAML (fields, key, tenant). |
| [Connecting your own Iceberg table](external-iceberg) | Point GrowlerDB at your own external table on S3, plus the connector. |
| [Storage & tiering](storage-tiering) | Hot vs cold object-storage tiering — when to use it (and when to keep hot). |

### Reference

| Page | What it covers |
|---|---|
| [Reference](reference) | Entry point to the [query language](query-language), the [REST & gRPC API](rest-api), and the [OpenSearch `_search` adapter](opensearch-adapter). |

### Is it the right fit?

| Page | What it covers |
|---|---|
| [Comparison & positioning](comparison) | When GrowlerDB fits (and when it doesn't) vs Elasticsearch & Trino. |
| [Performance (directional)](performance) | Directional latency/throughput numbers vs Elasticsearch & Trino. |
| [Migrating from Elasticsearch / OpenSearch](migration-from-elasticsearch) | Concepts + the two integration paths. |

### Operate & what's next

| Page | What it covers |
|---|---|
| [Deployment](deployment) | Local Compose and Kubernetes (Helm). |
| [GA criteria](ga-criteria) | What's GA-ready vs. the road to 1.0. |
| [Roadmap & known limitations](roadmap) | What's coming next, and the current limits to know about. |

## Feature overview

- **Search over your data** with PK hydration (`/v1/search` → `/v1/keys:get`).
- **Full-text, vector & hybrid retrieval** — lexical, semantic, and hybrid (RRF) search, an optional reranker, and a read-only MCP server for AI agents.
- **Query language** — native structured AST + a Lucene/KQL string parser.
- **Distributed** — control plane + stateful searcher nodes + a scatter-gather gateway.
- **Secure & multi-tenant** — OIDC/JWT, API keys, mTLS; control-plane RBAC; non-widenable tenant
  scoping.
- **Observable** — OpenTelemetry traces/metrics/logs, Prometheus, bundled Grafana SLI dashboards.
- **Console UI** + an optional **OpenSearch `_search` adapter**.
