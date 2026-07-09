---
type: Actor
title: Search analyst / end user
description: Queries indexes to find events/records and pulls the authoritative rows as evidence.
tags: [actor, analyst, end-user]
timestamp: 2026-07-04T14:22:00
---

# Search analyst / end user

The person who **searches** — an operations/reliability engineer chasing an incident, an analyst
investigating, or any end user finding records.

## Goals

- Issue [queries](/product/functional/search/query.md) (Lucene/KQL) over a time-bounded window,
  refine with [facets](/product/functional/search/facets.md) and
  [suggestions](/product/functional/search/suggest.md), sort and page through results.
- Read a results page from cached display fields, then
  [hydrate](/product/functional/hydration.md) a hit to the full authoritative record as evidence.
- Stay within their [tenant/role scope](/product/functional/rbac-and-tenancy.md) automatically.

## Reaches it through

The [console UI](/product/interfaces/ui.md) (Search/Explore) primarily; also the
[REST](/product/interfaces/rest.md) API and the OpenSearch-compatible
[`_search` adapter](/product/interfaces/opensearch-adapter.md).
