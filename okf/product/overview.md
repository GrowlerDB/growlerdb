---
type: Concept
title: Product overview
description: What users can do with GrowlerDB and the touchpoints they reach it through.
tags: [product, overview]
timestamp: 2026-07-04T14:22:00
---

# Product overview

GrowlerDB lets users run **fast full-text, vector, and hybrid search over your data** and retrieve
the authoritative rows — without standing up and syncing a separate search store. Apache Iceberg is
the flagship source today. See the [system-of-record thesis](/overview.md).

## What you can do

- **Index** an Iceberg table — choose fields/types, the composite key, and optional time
  [windowing](/product/functional/windowing-time.md) — and keep it **continuously in sync** via
  [ingestion](/product/functional/ingestion/index.md).
- **Search** it — full-text ([query](/product/functional/search/query.md) in Lucene/KQL) plus
  [vector & hybrid retrieval](/product/functional/search/vector.md) with optional reranking — with
  facets, suggestions, sorting, pagination, highlighting, and export.
- **Hydrate** hits to the authoritative Iceberg rows ([governed](/product/functional/hydration.md)),
  or render from cached display fields with no hydration.
- **Manage** indexes ([create/alter/drop/reindex/compact/backup, aliases & ILM](/product/functional/index-management/index.md)),
  **scale** (shards, windows, [cold tiering](/product/functional/cold-tiering.md),
  [replicas](/product/functional/replicas.md)), and **secure**
  ([auth](/product/functional/auth/index.md), RBAC, tenant isolation).
- **Observe** it ([SLI dashboards + alerts](/product/functional/observability.md)).

## How you reach it

Multiple [interfaces](/product/interfaces/index.md) over one Engine API: REST + gRPC, the console UI,
the CLI, client SDKs (Python/Rust), an OpenSearch-compatible `_search` adapter, and SQL UDFs
(Trino/Spark). The SPA console is served by the engine itself (same origin).

## Who / when

See [actors](/product/actors/index.md) (personas & roles) and
[use cases](/product/use-cases/index.md) (grounding scenarios).

## What it is / is not

GrowlerDB is a **retrieval engine — full-text, vector & hybrid — over your data** (Iceberg-flagship),
not a system of record, not an analytics/OLAP engine, not a datastore, and not detection/alerting (that
is the app layer above it). Full framing in the [overview](/overview.md) and
[D44](/system/decisions/d44-product-scope-retrieval.md).
