---
title: REST & gRPC API
layout: default
parent: Reference
nav_order: 2
---

# REST & gRPC API
{: .no_toc }

1. TOC
{:toc}

---

The gateway serves the Engine API over REST/JSON (`--rest-addr`) and gRPC (`--addr`); the
REST surface mirrors the gRPC methods 1:1. The console UI is served on the same REST port. Examples
below assume the gateway REST port is `8081` (the Compose mapping). Send
`Authorization: Bearer <token>` (or `ApiKey <key>`) when AuthN is enabled.

## Query (always available)

### `POST /v1/search`
Run a query; returns ranked document coordinates plus scores (not documents).

```sh
curl -s localhost:8081/v1/search -H 'content-type: application/json' -d '{
  "query": "title:iceberg", "limit": 10, "offset": 0,
  "sort": [{ "field": "ts", "desc": true }]
}'
```
```json
{ "hits": [ { "coordinates": { "identifier": [ { "name": "id", "value": "doc-2" } ] },
             "score": 1.0,
             "fields": { "title": "Iceberg internals" } } ],
  "total": 1 }
```
A hit carries `fields`, the index's `cached` display fields, so a results page renders
document-like rows without a hydration round-trip (D23). Omitted when the index caches none; the
authoritative row is still `/v1/keys:get`.
Also accepts `search_after` (keyset cursor), `collapse` (fast field), and `pit_id`. See
[Query language](query-language).

Highlighting (opt-in). Set `"highlight"` to have each hit carry a `highlight` object of matched
fragments per field, reflecting the *analyzed* match (stemming, per-field analysis, phrase positions).
Off by default (a per-hit cost). `"highlight": {}` highlights the index's default highlightable TEXT
(`cached`) fields; name fields and/or bound the output with `fields` / `max_fragments` / `fragment_size`:

```sh
curl -s localhost:8081/v1/search -H 'content-type: application/json' -d '{
  "query": "title:iceberg", "limit": 10,
  "highlight": { "fields": ["title"], "max_fragments": 1, "fragment_size": 150 }
}'
```
```json
{ "hits": [ { "coordinates": { "identifier": [ { "name": "id", "value": "doc-2" } ] },
             "score": 1.0,
             "fields": { "title": "Iceberg internals" },
             "highlight": { "title": [ [ { "text": "", "marked": false },
                                          { "text": "Iceberg", "marked": true },
                                          { "text": " internals", "marked": false } ] ] } } ],
  "total": 1 }
```

Highlights are `field → fragments → segments`, where each segment is an XSS-safe `{text, marked}` run
(no HTML); a `marked` run is a matched term, rendered inside `<mark>`. A field with no matching fragment
(and a non-highlightable field name) is simply absent. The `highlight` object is omitted entirely when
the request didn't opt in.

An omitted or `0` `limit` returns a bounded page (default 10), not the whole result set; for a
full scan use the keyset `search_after` scroll rather than an unbounded page. When a shard fails to respond
the response carries `"partial": true` (and `/v1/suggest`, `/v1/keys:get` carry `"failed_shards": N`)
so the result's incompleteness is never silent; the flag is omitted on a complete result.

Inline hydration (opt-in). Set `"hydrate": true` to get the search → `/v1/keys:get` round trip
in one call: each hit also carries `row`, its authoritative source row, resolved through the
same governed, key-verified path as `/v1/keys:get` (`hydrate_columns` projects it; empty = all).
Unlike `fields` (index-cached copies), `row` holds the source-of-truth values. Only the returned page
is hydrated, and a page above the hydration batch maximum (1000) is rejected up front. A row that
fails to resolve (a failed shard, a tenant-filtered or source-missing key, or a source outage)
degrades per hit: the hit keeps its coordinates + `fields` and carries `hydrate_error` instead of
`row`; the search itself never fails on hydration. Also accepted by `search:semantic` and
`search:hybrid` (rows attach to the fused page).

```sh
curl -s localhost:8081/v1/search -H 'content-type: application/json' -d '{
  "query": "title:iceberg", "limit": 10, "hydrate": true, "hydrate_columns": ["title", "body"]
}'
```
```json
{ "hits": [ { "coordinates": { "identifier": [ { "name": "id", "value": "doc-2" } ] },
             "score": 1.0,
             "fields": { "title": "Iceberg internals" },
             "row": { "title": "Iceberg internals", "body": "…the full source text…" } } ],
  "total": 1 }
```

### `POST /v1/keys:get`
Hydrate authoritative rows from Iceberg by coordinate (catalog-governed). `columns` empty = all.

```sh
curl -s localhost:8081/v1/keys:get -H 'content-type: application/json' -d '{
  "keys": [ { "identifier": [ { "name": "id", "value": "doc-2" } ] } ],
  "columns": []
}'
```

### `POST /v1/suggest`
Prefix / fuzzy term suggestions for a field (`{ "field", "text", "limit", "fuzzy", "max_edits" }`).

### `POST /v1/explain`
Explain why one specific document scores as it does for a query: the BM25 clause tree plus the
analyzed terms per field. Opt-in and per-hit (the default search path never computes it); this backs
the console's per-hit "explain" popover.

```sh
curl -s localhost:8081/v1/explain -H 'content-type: application/json' -d '{
  "index": "docs", "query": "title:iceberg",
  "coordinates": { "identifier": [ { "name": "id", "value": "doc-2" } ] }
}'
```
```json
{ "found": true, "matched": true, "score": 0.81,
  "detail": { "description": "weight(title:iceberg)", "score": 0.81, "details": [ … ] },
  "analyzed": [ { "field": "title", "terms": ["iceberg"] } ],
  "timings": { "index_ms": 0.4, "hydration_ms": 0.0, "total_ms": 0.5 },
  "shards_scanned": 1, "shards_total": 1 }
```
Accepts `"syntax": "kql"` like search. `found` is whether the coordinate exists; `matched` whether the
query matches it (`detail`/score are present only when it matches).

### `POST /v1/index:describe`
Per-index stats merged across shards (`num_docs`, `snapshot`, `checkpoint`, …) when the gateway
fronts the index, plus the full mapping: `fields` lists every mapped field with its `type`
and what a query can do with it (`indexed` = term-queryable, `fast` = range/sort/aggregate,
`cached` = returned with hits), alongside `time_fields` / `sort_fields` / `vector_fields`. This is
the schema clients compose valid queries from (console pickers, the MCP `describe_index` tool).

### `POST /v1/index:reindex`
Rebuild an index from its source and atomically swap it live (`{ "index": "<name>" }`; empty ⇒
the served index). Returns `{ "doc_count", "snapshot" }` of the rebuilt index. Writes are briefly
fenced on the owning Node for the duration, and the rebuild is single-flight: a second
concurrent reindex returns `412`. Single-shard (embedded) deployments only: a multi-shard
gateway returns `501` (distributed reindex orchestration is future work; reindex each shard's Node
directly). This is the Engine-side trigger behind the console's reindex button.

### `POST /v1/index:alter`
Plan (and optionally apply in-place) an **index-definition change** (`{ "index", "definition_yaml",
"apply" }`; empty `index` ⇒ the served index). Diffs the candidate definition against the served one
and returns `{ "is_noop", "requires_reindex", "reindex_reasons", "in_place_changes", "applied" }`.
Metadata-only changes (rename, `sensitive` flip, `max_bytes` redeclare) are in-place and applied
live when `apply` is `true`; changes that alter the indexed representation (fields, types, analyzers,
`fast`/`cached`, key, source) set `requires_reindex` with human-readable `reindex_reasons` and are
not applied, so run `/v1/index:reindex` for those. Single-shard (embedded) only: a multi-shard
gateway returns `501`; a node started without source access returns `501`.

## Aggregations & facets

GrowlerDB aggregates over the documents a query matches, merging buckets across shards. There are two
surfaces: the full aggregation API over gRPC (`Aggregate`), and a REST facets convenience
(`/v1/facets`) that runs a terms aggregation per field for a left-rail facet UI.

### gRPC `Aggregate` (full surface)

`Aggregate(AggregateRequest) returns (AggregateResponse)` on the `Search` service. `aggs` is a JSON
object of name → agg spec, where each spec is the externally-tagged `Agg` enum. Supported
aggregations:

| Agg | Spec | Notes |
|---|---|---|
| **Terms** | `{ "Terms": { "field", "size" } }` | Top-`size` buckets of a fast field by descending doc count. |
| **Stats** | `{ "Stats": { "field" } }` | `count` / `min` / `max` / `sum` / `avg` over a numeric fast field. |
| **DateHistogram** | `{ "DateHistogram": { "field", "fixed_interval" } }` | Fixed-width time buckets of a DATE fast field (e.g. `"1d"`, `"3600s"`). UTC-only, fixed interval: no calendar (month/quarter) intervals, timezone, or `offset` (offset client-side). |
| **Range** | `{ "Range": { "field", "ranges": [ { "from", "to" }, … ] } }` | Buckets over user-defined `[from, to)` ranges of a numeric fast field; omit a bound to leave it open. |

Aggregations require aggregatable (fast) fields; declare `fast: true` on the field in the index
definition. Request/response (`results` is a JSON object of name → result):

```json
// AggregateRequest
{ "query": "status:critical",
  "aggs": "{ \"by_metric\": { \"Terms\": { \"field\": \"metric\", \"size\": 10 } }, \"over_time\": { \"DateHistogram\": { \"field\": \"ts\", \"fixed_interval\": \"1h\" } } }",
  "index": "telemetry" }

// AggregateResponse.results (parsed)
{ "by_metric": { "buckets": [ { "key": "vibration", "doc_count": 812 }, … ],
                 "sum_other_doc_count": 40, "doc_count_error_upper_bound": 0 },
  "over_time": { "buckets": [ { "key": 1719792000000000, "doc_count": 128 }, … ] } }
```

**Shard-undercount flag.** `AggregateResponse.failed_shards` is `0` on a complete result; when `> 0`
that many shards didn't respond and the merged buckets under-count those shards' documents, a
flagged gap, never silent. Terms buckets also carry Tantivy's `doc_count_error_upper_bound`
(cross-shard top-N is exact only within the over-fetch window) and `sum_other_doc_count` (the long
tail below the top-`size`). `Aggregate` needs the Search scope, same as a query.

### `POST /v1/facets`

A REST convenience for faceted navigation: for each requested field it runs one terms aggregation
(reusing the distributed `Aggregate` path, with no parallel facet engine), scoped to the docs the `query`
matches, so counts reflect the current refinement. Non-aggregatable fields are skipped (not an
error). Full aggregations (stats / date-histogram / range) are gRPC-only; REST exposes only terms
facets.

```sh
curl -s localhost:8081/v1/facets -H 'content-type: application/json' -d '{
  "index": "catalog", "query": "*:*",
  "fields": ["category", "author"], "size": 10
}'
```
```json
{ "facets": [
    { "field": "category", "buckets": [ { "value": "guide", "count": 4 },
                                        { "value": "reference", "count": 3 } ] },
    { "field": "author",   "buckets": [ { "value": "carol", "count": 3 }, … ] } ] }
```
Up to 12 fields per call; `size` defaults to 10 (capped at 100). When a shard fails the response adds
`"partial": true` (omitted on a complete result). Needs the Search scope.

## Index management (gateway `--control-plane`)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/indexes` | List registered indexes (name + status). |
| `POST` | `/v1/indexes` | Create from a definition (`{ "definition": "<yaml>" }`). |
| `GET` | `/v1/indexes/{name}` | Routing config (status, shard count, routing strategy). |
| `DELETE` | `/v1/indexes/{name}` | Drop an index. |
| `POST` | `/v1/source:describe` | Introspect a source table's schema (`{ "table": "ns.table" }`), the create-form helper. |
| `GET` | `/v1/ingestion` · `/v1/ingestion/{name}` | Per-index sync status: source head vs. each shard's committed checkpoint (lag). |
| `GET` | `/v1/aliases` | List alias → index mappings. |
| `POST` | `/v1/aliases` | Point an alias at an index (`{ "alias", "index" }`); admin only. |
| `DELETE` | `/v1/aliases/{alias}` | Remove an alias; admin only. |
| `GET` | `/v1/index:activity` (`POST`) | Recent index-lifecycle activity. |
| `GET` | `/v1/license` | Enterprise-license status (licensee, nodes in use vs. limit). |

Reads (`GET`) need the index-read scope; alias writes need admin.

```sh
curl -s localhost:8081/v1/ingestion | python3 -m json.tool
```

## Index maintenance

Node-local maintenance of the served shard. Reindex/alter are documented above; these operate on
segments and backups. Maintenance operations require operator/admin privileges.

### `POST /v1/index:compact`
Merge the served shard's segments. Returns the live segment count before and after
(`{ "index": "<name>" }` ⇒ empty for the served index).
```json
{ "segments_before": 7, "segments_after": 1 }
```

### `POST /v1/index:backup`
Back up the served shard to object storage (`{ "index", "prefix" }`). Returns
`{ "snapshot", "file_count", "created_ms", "prefix" }`. Returns `501` when the node has no backup target
configured.

### `POST /v1/index:backup-status`
Last-backup status (`{ "index" }`): `{ "configured", "present", "snapshot", "created_ms",
"file_count" }`, with `configured: false` when the node has no backup target.

## Saved queries

Per-user saved searches backing the console's saved-query menu. All need the search scope.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/saved-queries` | List the caller's saved queries. |
| `POST` | `/v1/saved-queries` | Create one. |
| `PUT` | `/v1/saved-queries/{id}` | Update one. |
| `DELETE` | `/v1/saved-queries/{id}` | Delete one. |

## Users, roles & tokens (admin)

Administer the built-in credential store (the control-plane's `--login-secret`/`--builtin-auth`
accounts). Listing roles needs index-read; everything else needs the admin scope.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/users` | List users. |
| `PUT` | `/v1/users/{subject}/roles` | Set a user's roles (`{ "roles": ["reader", …] }`). |
| `GET` | `/v1/roles` | List the known roles and their scopes. |
| `GET` | `/v1/tokens` | List issued API tokens (metadata, never the secret). |
| `POST` | `/v1/tokens` | Mint an API token. |
| `DELETE` | `/v1/tokens/{id}` | Revoke a token. |

## Storage tiering

### `GET /v1/cold`
Cold-tier status for a windowed index: each window's hot/cold tier plus the shared read-through
cache's hit/miss/byte stats. Returns `404` on a non-windowed index (nothing to tier). See
[Storage & tiering](storage-tiering).

## Metrics proxy (gateway `--prometheus`)

`GET /v1/stats/query`, `/v1/stats/query_range`, `/v1/stats/alerts` proxy to the configured
Prometheus (same-origin, for the console's SLI panels).

## MCP transport (`POST /mcp`)

The Model Context Protocol [Streamable HTTP transport](https://modelcontextprotocol.io) is the
read-only agent face of the same query surface, served on the same listener. JSON-RPC 2.0 over
POST; sessionless; `GET` answers 405 (no server-initiated stream); a browser-sent `Origin` must be
loopback or match the request's `Host`. Auth is the same bearer as `/v1/*` (a closed deployment
401s a missing/invalid token with `WWW-Authenticate: Bearer`); tool calls (`search` with optional
inline hydration, `hydrate`, `aggregate`, `list_indexes`, `describe_index`) re-enter the `/v1`
surface under the caller's own token. See the
[getting-started MCP section](getting-started#7-connect-an-ai-agent-mcp).

## OpenSearch adapter (gateway `--opensearch`)

`POST /{index}/_search` (and `POST /_search`) takes a documented DSL subset → native query, returning
results as OpenSearch documents. See [OpenSearch adapter](opensearch-adapter).

## gRPC services

Same surface over gRPC (proto package `growlerdb.v1`):

- **Gateway**: `Search`, `Suggest`, `Aggregate`, `Explain`, `Lookup` (GetByKey), `Admin`.
- **Node** (`serve`): the above plus `Write` (apply changelog batches + `GetCheckpoint`) and
  `System`.
- **Control plane**: `ControlPlane`: `CreateIndex` / `DropIndex` / `ListIndexes` / `GetIndex` /
  `DescribeSource` / `RegisterServedIndex` / `IngestionStatus` / `PlanReshard` / `ApplyReshard` /
  `MoveBucket`.
  `GetIndex` also vends the index's virtual-bucket map (empty ⇒ default `fnv % shards`
  routing). `RegisterServedIndex` takes `shard_ordinals` so node *k* claims only shard *k* of a
  multi-node index. `PlanReshard` computes the bounded bucket→shard reassignment for a new shard
  count (read-only). `ApplyReshard` executes a growth reshard: it builds the new shards from source
  (filtered to their buckets), atomically commits the new bucket map at cutover, then trims the old
  shards, with no missing-read window (old shards stay complete until the cutover instant; the brief
  overlap is deduped). `MoveBucket` is online skew relief: it moves one bucket off a busy shard to a
  quieter one (no full reshard), building target → committing map → trimming source.

## Status codes

Errors map gRPC status → HTTP: `InvalidArgument` → 400, `Unauthenticated` → 401,
`PermissionDenied` → 403, `NotFound` → 404, `ResourceExhausted` → 429, else 500. Bodies carry a
structured `{ "error" | "message", … }`.
