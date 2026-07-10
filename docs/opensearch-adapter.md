---
title: OpenSearch adapter
layout: default
parent: Reference
nav_order: 3
---

# OpenSearch-compatible `_search` adapter

GrowlerDB ships an **optional, read-path-first** adapter that speaks a documented subset of the
OpenSearch `_search` Query DSL (decision D4). It exists for **client
& tool ecosystem compatibility and migration** — the native [`/v1/search`](rest-api)
PK API remains the primary, first-class surface.

It is **off by default**. Enable it on the gateway:

```sh
growlerdb gateway --node-addr http://node:50051 --opensearch ...
```

Then point an OpenSearch client at the gateway's REST port:

```sh
curl -s localhost:8081/myindex/_search -H 'content-type: application/json' -d '{
  "query": { "bool": {
    "must":   [{ "match": { "title": "alert" } }],
    "filter": [{ "range": { "ts": { "gte": "1700000000" } } }]
  }},
  "size": 20,
  "sort": [{ "ts": "desc" }]
}'
```

## How it works

The adapter **translates the DSL into GrowlerDB's native query string**, which the engine parses
into its [canonical AST](query-language) and executes through the normal search
path. Results are then shaped as OpenSearch documents:

- **`_id`** is synthesized from the composite key:
  the partition field values then the identifier values, joined by `#` (e.g. `42#u1`).
- **`_source`** is filled by **PK hydration** ([`GetByKey`](rest-api)) — the
  authoritative row from Iceberg, governed by the catalog. Search stays
  PK-based internally; the client sees documents.
- The response carries `took`, `timed_out`, `_shards`, and `hits.total` (a true cross-shard count).
  A down shard sets `_shards.failed` (never a silent gap).

The `Authorization` / tenant headers are forwarded to the engine, so the adapter is governed by the
same auth + tenant scoping as the native API.

## Supported query DSL

| Clause | Support | Maps to |
|---|---|---|
| `match_all` | ✅ | `MatchAll` (via the `*:*` idiom — a cheap all-docs query) |
| `match` | ✅ | analyzed term(s); multi-token ⇒ OR of tokens |
| `match_phrase` | ✅ | `Phrase` (ordered, adjacency) |
| `multi_match` | ✅ | OR of `field:value` across `fields` |
| `term` | ✅ | `Term` (exact / analyzed per field type) |
| `terms` | ✅ | OR of `Term`s |
| `range` (`gte`/`gt`/`lte`/`lt`) | ✅ | `Range` with inclusive/exclusive bounds |
| `bool` (`must`/`filter`/`must_not`/`should`) | ✅ | `Bool` — see `should`/`filter` caveats |
| `exists`, `prefix`, `wildcard`, `fuzzy`, `regexp`, `ids`, … | ❌ | clear `501` error |

### Request body
- **`from` / `size`** → offset / page size (default `size` = 10).
- **`sort`** → native sort keys. Accepts `"field"`, `{ "field": "asc"|"desc" }`, and
  `{ "field": { "order": ... } }`. `_score` entries are dropped (native ranks by score by default).
- **`highlight`** → opt into server-side highlighting. `fields` (the map keys) names the TEXT fields
  to highlight; `number_of_fragments` → max fragments per field and `fragment_size` → the fragment
  window (top-level or per-field, a per-field value winning). Each hit then carries a standard
  OpenSearch `highlight` object (`field → ["…<em>term</em>…"]`, HTML-escaped). Custom pre/post tags,
  `type`, and `order` are ignored — GrowlerDB emits `<em>`-marked, escaped fragments.
- **`query`** absent → match-all.

## Caveats (documented limitations, not bugs)

- **`bool.should`** is honored for **matching** only when there is **no** `must`/`filter` (matching
  OpenSearch's default `minimum_should_match`). With a `must`/`filter` present, `should` is
  scoring-only in OpenSearch — not expressible in the query string — so it's dropped from the
  predicate. `minimum_should_match` is not supported.
- **`bool.filter`** is treated like `must` (a required conjunct). The non-scoring distinction isn't
  modelled by the read adapter.
- **Value charset.** `term`/`terms`/`range`/`match` token values must be **simple tokens** (no
  whitespace or query metacharacters); a value like `"a b"` or `"a:b"` returns a clear `501`
  pointing you to the native API. `match_phrase` accepts spaces (it becomes a quoted phrase).
- **Write path / aggregations / scripting / mappings / percolators** are **out of scope** — use
  GrowlerDB's connectors for ingestion and `/v1/search` aggregations
  for analytics.

Unsupported input always returns a structured error (`{"error": {"type", "reason"}, "status"}`)
rather than a wrong result.
