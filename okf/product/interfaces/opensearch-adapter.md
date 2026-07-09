---
type: Interface
title: OpenSearch _search adapter
description: An OpenSearch-compatible read endpoint so existing _search clients can query GrowlerDB.
tags: [interface, opensearch, compatibility]
resource: /docs/opensearch-adapter.md
timestamp: 2026-07-04T14:22:00
---

# OpenSearch `_search` adapter

An optional, OpenSearch-compatible **read** endpoint — `POST <index>/_search` — so tools and clients
that already speak the `_search` request/response shape can query a GrowlerDB index without code
changes. Enabled with `gateway --opensearch`; **off by default**.

## Scope

A documented subset of the `_search` DSL (the query/filter/sort/paginate surface GrowlerDB supports),
mapped onto the native [search](/product/functional/search/query.md) capability. Read-only — indexing
and admin use the native [REST](/product/interfaces/rest.md)/[gRPC](/product/interfaces/grpc.md) API.

## Notes

Supported subset + mappings: [docs/opensearch-adapter.md](/docs/opensearch-adapter.md). This is a
compatibility surface of GrowlerDB, not a reimplementation of another product.
