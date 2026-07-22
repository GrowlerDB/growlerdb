---
title: Home
layout: default
nav_order: 1
---

# GrowlerDB
{: .fs-9 }

Open-source retrieval engine for full-text, vector, and hybrid search over your
data. Your source (Apache Iceberg today) stays the system of record. GrowlerDB
keeps a fast derived index and returns the matching primary keys, which hydrate
back to the authoritative rows.
{: .fs-6 .fw-300 }

[Get started](getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/GrowlerDB/growlerdb){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## The model in one minute

You define an index over an Iceberg source table: which columns to index, the
composite key, and an optional `tenant_field`. A connector keeps the index in
sync with the table.

A search runs against that derived index and returns document coordinates (the
composite key) plus scores, not documents. Your client, or built-in hydration,
then fetches the authoritative rows from Iceberg by key (`POST /v1/keys:get`),
governed by the catalog. The lake stays the source of truth.

A normal search engine owns its documents. GrowlerDB does the opposite, which is
why it layers onto a lakehouse instead of copying it.

## Documentation

New here? Start with [Getting started](getting-started). It takes you from an
empty machine to your first search, then on to hydrate and the console. The rest
of the pages are grouped by what you're doing, so you can find your way at your
own pace.

### Start here

| Page | What it covers |
|---|---|
| [Getting started](getting-started) | Compose stack, your first search, hydrate, console. |
| [Install & run modes](install) | Build from source; embedded, `serve`, `gateway`, `control-plane`. |

### Configure & connect

| Page | What it covers |
|---|---|
| [Configuration](configuration) | CLI flags, env vars, and the index-definition YAML (fields, key, tenant). |
| [Connecting your own Iceberg table](external-iceberg) | Point GrowlerDB at your own external table on S3, plus the connector. |
| [Storage & tiering](storage-tiering) | Hot vs cold object-storage tiering, and when to use each. |

### Reference

| Page | What it covers |
|---|---|
| [Reference](reference) | Entry point to the [query language](query-language), the [REST & gRPC API](rest-api), and the [OpenSearch `_search` adapter](opensearch-adapter). |

### Is it the right fit?

| Page | What it covers |
|---|---|
| [Comparison & positioning](comparison) | When GrowlerDB fits, and when it doesn't, next to Elasticsearch and Trino. |
| [Performance (directional)](performance) | Directional latency and throughput numbers vs Elasticsearch and Trino. |
| [Migrating from Elasticsearch / OpenSearch](migration-from-elasticsearch) | Concepts plus the two integration paths. |

### Operate & what's next

| Page | What it covers |
|---|---|
| [Deployment](deployment) | Local Compose and Kubernetes (Helm). |
| [GA criteria](ga-criteria) | What's ready today and what's left before 1.0. |
| [Roadmap & known limitations](roadmap) | What's coming next, and the current limits to know about. |

## Feature overview

Search over your data with primary-key hydration (`/v1/search` then
`/v1/keys:get`). Retrieval is lexical (Lucene/KQL), semantic (local-default
embeddings), or hybrid (RRF), with an optional reranker and a read-only MCP
server for AI agents. Queries go through a native structured AST or a Lucene/KQL
string parser.

To run at scale, GrowlerDB is distributed (a control plane, stateful searcher
nodes, and a scatter-gather gateway) and multi-tenant: OIDC/JWT, API keys, and
mTLS; control-plane RBAC; and tenant scoping that callers can't widen. Every
service emits OpenTelemetry traces, metrics, and logs to Prometheus and bundled
Grafana dashboards. A console UI ships with it, plus an optional OpenSearch
`_search` adapter.
