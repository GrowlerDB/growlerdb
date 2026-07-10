---
title: Query language
layout: default
parent: Reference
nav_order: 1
---

# Query language
{: .no_toc }

1. TOC
{:toc}

---

GrowlerDB has a **canonical structured query AST**; the string forms below parse into it. Most
callers use the **Lucene-style string** (the default for `/v1/search`'s `query`); a **KQL** variant
(lowercase operators) is selected per request with `"syntax": "kql"` (REST) / `SearchRequest.syntax`
(gRPC). The console's search box has a Lucene/KQL toggle.

## String syntax (Lucene)

| Form | Example | Meaning |
|---|---|---|
| Term | `status:active` | `status` equals/contains `active` (analyzed on `TEXT`, exact on `KEYWORD`). |
| Default-field term | `iceberg` | Term against the index's default text field. |
| Phrase | `title:"iceberg search"` | Ordered tokens; add slop with `~2`. |
| Boolean | `a AND b`, `a OR b`, `NOT c`, `-c` | Combine clauses; `()` groups. |
| Field-grouped set | `category:(guide OR reference)` | The field prefix distributes over the group — equivalent to `category:guide OR category:reference` (works with `AND`/implicit-AND too). |
| Bool term | `archived:true`, `active:false` | Exact match on a `BOOL` field. |
| Range | `age:[18 TO 65]`, `published:[2024-01-01 TO *]` | `[` `]` inclusive, `{` `}` exclusive; mix freely; `*`/empty = unbounded. On a `DATE` field a bound is epoch **micros** or an **ISO-8601 / RFC3339** date (`2024-01-01`, `2024-01-01T00:00:00Z`). |
| Wildcard | `device_id:sensor-*`, `code:??x` | `*` (many) / `?` (one). A leading `*` is cost-guarded. |
| Fuzzy | `name:jon~1` | Edit distance 0–2 (`~` alone = 2). |
| Prefix | `path:/var/*` | Literal prefix match. |
| Regex | `id:/ab.*/` | Regex against indexed terms. |
| CIDR | `gateway_ip:10.0.0.0/8` | IP-in-block (requires an `IP` field). |
| Boost | `title:iceberg^2` | Scale a clause's score. |
| Match-all | `*:*` | Every document (a cheap all-docs query). |

### KQL

Selecting `syntax: "kql"` parses the same shapes with **lowercase** `and` / `or` / `not` (Kibana
Query Language). Only the search path honors the selector today; aggregate/export stay Lucene.

## The AST (clauses)

The string parses into these nodes — the structured API and the
[OpenSearch adapter](opensearch-adapter) target them directly:

`MatchAll` · `Term` · `Terms` (set membership) · `Match` (analyzed, AND/OR of tokens) · `Phrase` ·
`Prefix` · `Wildcard` · `Fuzzy` · `Regex` · `Exists` · `Range` · `IpCidr` · `Bool`
(`must`/`should`/`must_not`/`filter`) · `Boost`.

Field existence and type rules are validated **at execution** against the index schema, where field
types are known.

## Sorting, paging, collapsing

`/v1/search` (and the gRPC `Search`) also accept:

- **`sort`** — sort keys (a composite-key tiebreaker is applied implicitly); empty = by relevance
  `_score` descending. Each key is a numeric/date/KEYWORD **fast field**, or the reserved **`_score`**
  for relevance — alone (`[{ "field": "_score", "desc": true }]`) or as a tiebreaker among fields
  (e.g. `rank desc, _score desc`). A `_score` sort is **offset-paged only** (a score isn't a stable
  keyset key).
- **`offset` + `limit`** — `from`/`size` paging, bounded by a page-fetch ceiling.
- **`search_after`** — opaque keyset cursor from a prior response's `next_cursor` (stable deep
  paging; requires a sort over fast fields — a `_score` sort is rejected here).
- **`collapse`** — collapse to the top hit per distinct value of a fast field, with a per-hit group
  count.
- **`pit_id`** — read against a frozen point-in-time snapshot.

## What a search returns

Hits are **document coordinates** (the composite key) + a BM25 score — **not** documents. Fetch the
authoritative rows from Iceberg by key with [`/v1/keys:get`](rest-api#post-v1keysget). `total` is a
true cross-shard match count, and a `partial` flag is set if any shard was down.
