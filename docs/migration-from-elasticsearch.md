---
title: Migrating from Elasticsearch
layout: default
nav_order: 6
---

# Migrating from Elasticsearch / OpenSearch

GrowlerDB can stand in for the **search** role of an Elasticsearch/OpenSearch cluster over data
that lives in (or can land in) Apache Iceberg. This guide covers the conceptual differences and the
two integration paths.

## The key difference: GrowlerDB doesn't own your documents

| | Elasticsearch / OpenSearch | GrowlerDB |
|---|---|---|
| System of record | the engine's own `_source` | **Apache Iceberg** (the lakehouse) |
| A search returns | full documents | **document coordinates** (the composite key) + score |
| Getting the row | already in the hit | **hydrate by key** from Iceberg (`/v1/keys:get`), catalog-governed |
| Ingestion | `_bulk` / index API | a **changelog connector** keeps the index in sync with the source table |
| Mappings | index mapping | an [index definition](reference) over a source table |

So the mental shift is: **the lake is the source of truth; GrowlerDB is a derived index.** You
don't migrate documents *into* GrowlerDB — you point GrowlerDB at the Iceberg table they already
live in (or replicate them there), and it indexes them.

## Path 1 — adopt the native API (recommended)

1. **Land your data in Iceberg** if it isn't already (most lakehouse stacks already do this; or
   replicate from your current store).
2. **Define an index** over the source table: which columns to index and their types, the composite
   key (partition + identifier), and an optional `tenant_field` for multi-tenant isolation. Use the
   console's **Indexes → Create** (it introspects the table schema) or the control-plane API.
3. **Query** with `POST /v1/search` (Lucene/KQL string or the structured AST) and **hydrate** rows
   with `POST /v1/keys:get`. Re-point your application at these endpoints.

This gives you the full feature surface (collapsing, `search_after` paging, suggestions,
aggregations, tenant scoping) and the cleanest semantics.

## Path 2 — the OpenSearch `_search` adapter (drop-in, partial)

To reuse existing OpenSearch clients/tooling with minimal change, enable the optional adapter
(`gateway --opensearch`) and point clients at the gateway:

```sh
curl -s GATEWAY/myindex/_search -H 'content-type: application/json' -d '{
  "query": { "bool": {
    "must":   [{ "match": { "title": "alert" } }],
    "filter": [{ "range": { "ts": { "gte": "1700000000" } } }]
  }},
  "size": 20, "sort": [{ "ts": "desc" }]
}'
```

It translates a **documented subset** of the `_search` Query DSL to native queries and returns
OpenSearch-shaped documents (`_id` from the key, `_source` via hydration). Supported: `match`,
`match_phrase`, `multi_match`, `term`, `terms`, `range`, `bool`, `match_all`, plus `from`/`size`/
`sort`. **Read-path only.** See the full support matrix + caveats in
[opensearch-adapter.md](opensearch-adapter).

### What won't carry over

- **Writes** (`_bulk`, index/update/delete APIs) — ingestion is via the changelog connector, not a
  write API. Point your pipeline at the Iceberg table.
- **Aggregations / scripting / mappings / ingest pipelines / percolators** — not served by the
  adapter; use the native aggregation API where available, and define mappings as index definitions.
- **Exact scoring parity** — BM25 ranks results, but per-clause scoring nuances differ.

## Multi-tenancy

If you used index-per-tenant or filtered aliases, map it to GrowlerDB's `tenant_field`: set it on
the index, and every read gets a mandatory, non-widenable `tenant = <verified claim>` filter from
the caller's token. See [SECURITY.md](https://github.com/GrowlerDB/growlerdb/blob/main/SECURITY.md).

## Checklist

- [ ] Source data is in Iceberg (or replicated there).
- [ ] Index definition created (columns, key, `tenant_field`).
- [ ] App reads moved to `/v1/search` + `/v1/keys:get` — or the `_search` adapter for a faster cutover.
- [ ] AuthN enabled at the gateway; tenant claims present in client tokens.
- [ ] Ingestion connector running and caught up (check the **Ingestion** screen).
