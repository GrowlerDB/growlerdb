---
title: Home
layout: default
nav_order: 1
---

# GrowlerDB
{: .fs-9 }

Open-source **text search engine over Apache Iceberg**. Iceberg stays the system of record;
GrowlerDB keeps a fast derived full-text index and returns the matching **primary keys**, which
hydrate back to authoritative rows in the lake.
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

| Page | What it covers |
|---|---|
| [Getting started](getting-started) | Compose stack → your first search → hydrate → console. |
| [Install & run modes](install) | Build from source; embedded, `serve`, `gateway`, `control-plane`. |
| [Configuration](configuration) | CLI flags, env vars, and the index-definition YAML (fields, key, tenant). |
| [Reference](reference) | Query language, the REST/gRPC API, and the OpenSearch `_search` adapter. |
| [Migrating from Elasticsearch / OpenSearch](migration-from-elasticsearch) | Concepts + the two integration paths. |
| [Deployment](deployment) | Local Compose and Kubernetes (Helm). |
| [Storage & tiering](storage-tiering) | Hot vs cold object-storage tiering — when to use it (and when to keep hot). |
| [GA criteria](ga-criteria) | What's GA-ready vs. the road to 1.0. |

## Feature overview

- **Search over Iceberg** with PK hydration (`/v1/search` → `/v1/keys:get`).
- **Query language** — native structured AST + a Lucene/KQL string parser.
- **Distributed** — control plane + stateful searcher nodes + a scatter-gather gateway.
- **Secure & multi-tenant** — OIDC/JWT, API keys, mTLS; control-plane RBAC; non-widenable tenant
  scoping.
- **Observable** — OpenTelemetry traces/metrics/logs, Prometheus, bundled Grafana SLI dashboards.
- **Console UI** + an optional **OpenSearch `_search` adapter**.
