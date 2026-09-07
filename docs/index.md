---
title: GrowlerDB
layout: default
nav_order: 1
---

# GrowlerDB
{: .fs-9 }

An open-source retrieval engine for full-text, vector, and hybrid search over your Apache Iceberg data.
GrowlerDB keeps a fast, derived index locally and returns matching (cached) fields and primary keys (coordinates). The data lake remains your single source of truth.
{: .fs-6 .fw-300 }

[Get started](getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/GrowlerDB/growlerdb){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## The cycle

GrowlerDB operates on a three-step cycle:

1. **Index:** Point GrowlerDB at an Apache Iceberg table. A connector streams table changes to build a local index.
2. **Search:** Run lexical, semantic, or hybrid queries against the index. The query returns documents including cached fields, primary key coordinates, and scores.
3. **Hydrate:** Fetch the full, authoritative rows from your Iceberg catalog using the returned coordinates.

Traditional search engines store a complete, separate copy of your documents. GrowlerDB stores only what is needed to search, using primary keys to bridge the index and the data lake. With cached fields, paginated lists and paging can be fully powered by GrowlerDB.

## Retrieval options

When retrieving results, you can choose from three paths depending on your latency and data needs:

* **Search - Cached fields:** You can configure the index to store specific columns. These values return with the search hits immediately, requiring no Iceberg lookups.
* **Search - Inline hydration:** You can request inline hydration by setting `hydrate: true` in your search body. The engine collapses the search and lookup steps, returning the authoritative row directly in the search response.
* **Get - Full record:** For the authoritative details, your client fetches the full row by key using `POST /v1/keys:get`. This is typically used when a user opens a specific document.

---

## Choose your path

To help you get started quickly, we have there are three paths.

* **Application developers:** Learn how to write search queries, configure local embeddings, and use the REST/gRPC APIs. Start with [Getting started](getting-started) and the [Query language](query-language).
* **Platform engineers:** Deploy and manage the distributed stack, configure OIDC/JWT security, and monitor services. Start with [Install and run modes](install) and [Deployment](deployment).
* **Data engineers:** Define index schemas, connect external tables, and set up the Spark connector. Start with [Configuration](configuration) and [Connecting your own Iceberg table](external-iceberg).

---

## Open source & commercial

GrowlerDB is open source under AGPL-3.0, free for self-hosted production use up to 3 nodes. Need more nodes, support with an SLA, or to embed GrowlerDB in a closed product? See [License & support](license).
