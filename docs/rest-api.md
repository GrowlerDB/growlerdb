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

The gateway serves the Engine API over **REST/JSON** (`--rest-addr`) and **gRPC** (`--addr`); the
REST surface mirrors the gRPC methods 1:1. The console UI is served on the same REST port. Examples
below assume the gateway REST port is `8081` (the Compose mapping). Send
`Authorization: Bearer <token>` (or `ApiKey <key>`) when AuthN is enabled.

## Query (always available)

### `POST /v1/search`
Run a query; returns ranked **document coordinates** + scores (not documents).

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
A hit carries `fields` — the index's `cached` display fields — so a results page renders
document-like rows **without** a hydration round-trip (D23). Omitted when the index caches none; the
authoritative row is still `/v1/keys:get`.
Also accepts `search_after` (keyset cursor), `collapse` (fast field), and `pit_id`. See
[Query language](query-language).

**Highlighting** (opt-in) — set `"highlight"` to have each hit carry a `highlight` object of matched
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
(no HTML) — a `marked` run is a matched term, rendered inside `<mark>`. A field with no matching fragment
(and a non-highlightable field name) is simply absent. The `highlight` object is omitted entirely when
the request didn't opt in.

An **omitted or `0` `limit` returns a bounded page** (default 10), not the whole result set — for a
full scan use the keyset `search_after` scroll, not an unbounded page. When a shard fails to respond
the response carries `"partial": true` (and `/v1/suggest`, `/v1/keys:get` carry `"failed_shards": N`)
so the result's incompleteness is never silent; the flag is omitted on a complete result.

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

### `POST /v1/index:describe`
Per-index stats merged across shards (`num_docs`, `snapshot`, `checkpoint`, …) when the gateway
fronts the index.

### `POST /v1/index:reindex`
Rebuild an index from its source and **atomically swap it live** (`{ "index": "<name>" }`; empty ⇒
the served index). Returns `{ "doc_count", "snapshot" }` of the rebuilt index. Writes are briefly
**fenced** on the owning Node for the duration, and the rebuild is **single-flight**: a second
concurrent reindex returns `412`. **Single-shard (embedded) deployments only** — a multi-shard
gateway returns `501` (distributed reindex orchestration is future work; reindex each shard's Node
directly). This is the Engine-side trigger behind the console's reindex button.

### `POST /v1/index:alter`
Plan (and optionally apply in-place) an **index-definition change** (`{ "index", "definition_yaml",
"apply" }`; empty `index` ⇒ the served index). Diffs the candidate definition against the served one
and returns `{ "is_noop", "requires_reindex", "reindex_reasons", "in_place_changes", "applied" }`.
Metadata-only changes (rename, `sensitive` flip, `max_bytes` redeclare) are **in-place** and applied
live when `apply` is `true`; changes that alter the indexed representation (fields, types, analyzers,
`fast`/`cached`, key, source) set `requires_reindex` with human-readable `reindex_reasons` and are
**not** applied — run `/v1/index:reindex` for those. Single-shard (embedded) only — a multi-shard
gateway returns `501`; a node started without source access returns `501`.

## Index management (gateway `--control-plane`)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/indexes` | List registered indexes (name + status). |
| `POST` | `/v1/indexes` | Create from a definition (`{ "definition": "<yaml>" }`). |
| `GET` | `/v1/indexes/{name}` | Routing config (status, shard count, routing strategy). |
| `DELETE` | `/v1/indexes/{name}` | Drop an index. |
| `POST` | `/v1/source:describe` | Introspect a source table's schema (`{ "table": "ns.table" }`) — the create-form helper. |
| `GET` | `/v1/ingestion` · `/v1/ingestion/{name}` | Per-index sync status: source head vs. each shard's committed checkpoint (lag). |

```sh
curl -s localhost:8081/v1/ingestion | python3 -m json.tool
```

## Metrics proxy (gateway `--prometheus`)

`GET /v1/stats/query`, `/v1/stats/query_range`, `/v1/stats/alerts` proxy to the configured
Prometheus (same-origin, for the console's SLI panels).

## OpenSearch adapter (gateway `--opensearch`)

`POST /{index}/_search` (and `POST /_search`) — a documented DSL subset → native query, results as
OpenSearch documents. See [OpenSearch adapter](opensearch-adapter).

## gRPC services

Same surface over gRPC (proto package `growlerdb.v1`):

- **Gateway** — `Search`, `Suggest`, `Lookup` (GetByKey), `Admin`.
- **Node** (`serve`) — the above plus `Write` (apply changelog batches + `GetCheckpoint`) and
  `System`.
- **Control plane** — `ControlPlane`: `CreateIndex` / `DropIndex` / `ListIndexes` / `GetIndex` /
  `DescribeSource` / `RegisterServedIndex` / `IngestionStatus` / `PlanReshard` / `ApplyReshard` /
  `MoveBucket`.
  `GetIndex` also vends the index's virtual-bucket map (task-77; empty ⇒ legacy `fnv % shards`
  routing). `RegisterServedIndex` takes `shard_ordinals` so node *k* claims only shard *k* of a
  multi-node index. `PlanReshard` computes the bounded bucket→shard reassignment for a new shard
  count (read-only). **`ApplyReshard` executes a growth reshard**: builds the new shards from source
  (filtered to their buckets), atomically commits the new bucket map at cutover, then trims the old
  shards — no missing-read window (old shards stay complete until the cutover instant; the brief
  overlap is deduped). **`MoveBucket` is online skew relief**: move one bucket off a busy shard to a
  quieter one (no full reshard) — build target → commit map → trim source.

## Status codes

Errors map gRPC status → HTTP: `InvalidArgument` → 400, `Unauthenticated` → 401,
`PermissionDenied` → 403, `NotFound` → 404, `ResourceExhausted` → 429, else 500. Bodies carry a
structured `{ "error" | "message", … }`.
