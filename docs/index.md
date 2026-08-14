---
title: Home
layout: default
nav_order: 1
---

# GrowlerDB
{: .fs-9 }

An open-source retrieval engine for full-text, vector, and hybrid search over your Apache Iceberg data.
GrowlerDB keeps a fast, derived index locally and returns matching primary keys (coordinates) which hydrate back to the authoritative rows in your lakehouse. The data lake remains your single source of truth.
{: .fs-6 .fw-300 }

[Get started](getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/GrowlerDB/growlerdb){: .btn .fs-5 .mb-4 .mb-md-0 }

GrowlerDB is pre-1.0 and under active development — the engine is feature-complete for its core
surface but has not been run in production. See the [GA criteria](ga-criteria) and
[roadmap](roadmap) for what's done and what's still ahead.
{: .fs-3 .fw-300 }

---

## The model in one minute

GrowlerDB operates on a three-step cycle:

1. **Index:** Point GrowlerDB at an Apache Iceberg table. A lightweight connector streams table changes to build a local index.
2. **Search:** Run lexical, semantic, or hybrid queries against the index. The query returns document coordinates (the composite primary key) and scores.
3. **Hydrate:** Fetch the full, authoritative rows from your Iceberg catalog using the returned coordinates.

Traditional search engines store a complete, separate copy of your documents. GrowlerDB stores only what is needed to search, using primary keys to bridge the index and the data lake.

## Choose your path

To help you get started quickly, we have organized the documentation into paths based on your role:

* **Application developers:** Learn how to write search queries, configure local embeddings, and use the REST/gRPC APIs. Start with [Getting started](getting-started) and the [Query language](query-language).
* **Platform engineers:** Deploy and manage the distributed stack, configure OIDC/JWT security, and monitor services. Start with [Install and run modes](install) and [Deployment](deployment).
* **Data engineers:** Define index schemas, connect external tables, and set up the Spark connector. Start with [Configuration](configuration) and [Connecting your own Iceberg table](external-iceberg).

---

## Retrieval options

When retrieving results, you can choose from three paths depending on your latency and data needs:

* **Cached fields:** You can configure the index to store specific columns. These values return with the search hits immediately, requiring no Iceberg lookups.
* **Full hydration:** For the authoritative record, your client fetches the full row by key using `POST /v1/keys:get`. This is typically used when a user opens a specific document.
* **Inline hydration:** You can request inline hydration by setting `hydrate: true` in your search body. The engine collapses the search and lookup steps, returning the authoritative row directly in the search response.

---

## Documentation map

Use the links below to find specific guides and reference pages.

### Start here

| Page | What it covers |
|---|---|
| [Getting started](getting-started) | Run the local Compose stack, execute your first search, and hydrate results. |
| [Install and run modes](install) | Build from source and run in embedded, serve, gateway, or control-plane modes. |

### Configure and connect

| Page | What it covers |
|---|---|
| [Configuration](configuration) | Configure the engine with CLI flags, environment variables, and YAML index schemas. |
| [Connecting your own Iceberg table](external-iceberg) | Connect external tables on AWS S3 and set up the Spark ingestion connector. |
| [Storage and tiering](storage-tiering) | Use time-windowing to park older, immutable index shards on object storage. |

### Reference

| Page | What it covers |
|---|---|
| [Reference](reference) | Explore the query language syntax, REST/gRPC API endpoints, and the OpenSearch compatibility adapter. |

### Architecture and positioning

| Page | What it covers |
|---|---|
| [Comparison and positioning](comparison) | Compare GrowlerDB's design and use cases with Elasticsearch and Trino. |
| [Performance](performance) | Review directional search and hydration latency measurements. |
| [Migrating from Elasticsearch](migration-from-elasticsearch) | Learn how to move your application search from Elasticsearch or OpenSearch. |

### Operations and roadmap

| Page | What it covers |
|---|---|
| [Deployment](deployment) | Deploy GrowlerDB locally with Docker Compose or at scale with Kubernetes and Helm. |
| [GA criteria](ga-criteria) | Track our criteria and check-off lists for the 1.0 release. |
| [Roadmap and known limitations](roadmap) | Review upcoming features and current engine constraints. |
