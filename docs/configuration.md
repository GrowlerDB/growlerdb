---
title: Configuration
layout: default
nav_order: 4
---

# Configuration
{: .no_toc }

1. TOC
{:toc}

---

## Global flags

These apply to every subcommand (env var in parentheses):

| Flag | Default | Purpose |
|---|---|---|
| `--data-dir <dir>` (`GROWLERDB_DATA_DIR`) | `.growlerdb` | Local index store directory. |
| `--metrics-addr <host:port>` | _off_ | Serve `/healthz`, `/readyz`, Prometheus `/metrics`. |
| `--ui-dir <dir>` (`GROWLERDB_UI_DIR`) | _off_ | Serve the built console SPA from the REST front. |

## Environment {#environment}

You configure the Iceberg source and object store through environment variables, which
override the local-dev defaults:

| Variable | Default | Notes |
|---|---|---|
| `GROWLERDB_CATALOG_URI` | `http://localhost:8181/api/catalog` | Iceberg REST catalog. |
| `GROWLERDB_WAREHOUSE` | `growlerdb` | Catalog warehouse name. |
| `GROWLERDB_CATALOG_CREDENTIAL` | none | Catalog OAuth `client:secret` (Polaris). |
| `GROWLERDB_CATALOG_SCOPE` | none | Optional catalog OAuth scope. |
| `GROWLERDB_S3_ENDPOINT` | `http://localhost:9000` | Object-store endpoint. |
| `GROWLERDB_S3_ACCESS_KEY` | `minioadmin` | Object-store access key. |
| `GROWLERDB_S3_SECRET_KEY` | `minioadmin` | Object-store secret key. |
| `GROWLERDB_S3_REGION` | `us-east-1` | Object-store region. |
| `GROWLERDB_BACKUP_BUCKET` | none | Bucket for `backup`/`restore` (reuses the `GROWLERDB_S3_*` credentials/endpoint). |
| `GROWLERDB_MODEL_DIR` | `~/.cache/growlerdb/models` | Where the local embedder loads models from (`<dir>/<model-id>/`). Only the index nodes, which embed `VECTOR` fields at ingest, read it. |
| `GROWLERDB_LICENSE` | none | Enterprise scale-limit license token (set on the control plane). Unset ⇒ the free tier. |

In Kubernetes the [Helm chart](deployment#kubernetes-helm) wires these from a ConfigMap (non-secret)
and a Secret (credentials); the credentials should come from a `Secret`, never inline.

### Gateway limits & safety {#gateway-limits}

The gateway reads a few admission and safety knobs from the environment, so you can tune them for
your hardware without a rebuild. Set each on the gateway process.

| Env var | Default | Effect |
|---|---|---|
| `GROWLERDB_MAX_CONCURRENT_QUERIES` | `256` | Queries admitted at once; over the cap, a query gets `429` (load-shed). `0` = unbounded. |
| `GROWLERDB_MAX_FETCH` | `10000` | Ceiling on `offset + limit` per query; over it returns `InvalidArgument`. `0` = unbounded. |
| `GROWLERDB_MAX_CONCURRENT_FANOUT` | `256` | Per-shard RPCs in flight across all scatter-gathers. `0` = unbounded. |
| `GROWLERDB_REQUIRE_AUTH` | _unset_ | When truthy (`1`/`true`/`yes`/`on`), the gateway refuses to start unless authentication is configured (`--oidc-issuer` or `--builtin-auth`). Use it in production so a missing auth flag fails fast instead of serving open. |
| `GROWLERDB_DEFAULT_INDEX` | _unset_ | The index the console selects by default — its front door — advertised via `/v1/config`. Point it at a `VECTOR` index (the demo uses `movies`) so a fresh visitor lands where semantic/hybrid search is one click away. Unset ⇒ the console uses the first index. |

Running the gateway without `--oidc-issuer` or `--builtin-auth` leaves it open (no authentication).
That is fine for local use and prints a warning at startup; set `GROWLERDB_REQUIRE_AUTH` to turn the
warning into a hard startup failure.

### Scale limit & licensing {#scale-limit}

The open-source tier runs up to 3 index nodes per deployment at no cost. Beyond that, the control
plane refuses to admit new nodes until an Enterprise license raises the cap. Existing nodes and
data are never disrupted: a re-registering node always passes, and only genuinely new capacity is
gated. Set the signed license via `GROWLERDB_LICENSE` on the control plane; an invalid token is
ignored with a warning and falls back to the free tier. Licenses are verified offline, with no
phone-home. See [`COMM-LICENSE.md`](https://github.com/GrowlerDB/growlerdb/blob/main/COMM-LICENSE.md)
for how to obtain one.

## The index definition

You define an index with a small YAML document. Pass it to `growlerdb index --def file.yaml`, or
author it in the console's Indexes → Create screen, which introspects the source schema for you. With
no definition, GrowlerDB auto-maps every source field.

```yaml
name: docs
source:
  iceberg:
    catalog: growlerdb        # catalog name
    table: growlerdb.docs     # namespace.table
# key: optional, derived from the source's identifier/partition hints when omitted.
key:
  partition_fields: [region]  # co-locate a partition on a shard (partition routing)
  identifier_fields: [id]     # the per-document identity
# tenant_field: optional, enables non-widenable tenant scoping (must be a KEYWORD field).
tenant_field: tenant
mapping:
  selection: EXPLICIT         # ALL = index every source field; EXPLICIT = only those listed
  fields:
    - { path: id,     type: KEYWORD }
    - { path: title,  type: TEXT }
    - { path: body,   type: TEXT }
    - { path: region, type: KEYWORD }
    - { path: ts,     format: epoch_ms }   # a timestamp from an int64 epoch-millis column
```

### Field types

| Type | Use |
|---|---|
| `TEXT` | Analyzed full-text (BM25-searchable). |
| `KEYWORD` | Exact-match token (filters, facets, `tenant_field`). |
| `LONG` | 64-bit integer. Range, sort, numeric facets. |
| `DOUBLE` | 64-bit float. Range and sort. |
| `BOOL` | Boolean. |
| `DATE` | Date or timestamp. Range, date-histogram, time pruning. |
| `IP` | IP address, for CIDR/range match. Never auto-derived (declare it explicitly; it arrives as a string). |
| `VECTOR` | Dense embedding for semantic or hybrid search. Never auto-derived: declare it with a `vector:` config, and the embedding is produced from a text `source_field` at ingest (see below). |

### Vector fields (semantic search) {#vector-fields}

A `VECTOR` field is opt-in and derived. Rather than mapping a source column, it declares a
`vector:` config naming a text `source_field`, and GrowlerDB embeds that field's value into a dense
vector at ingest. That powers `POST /v1/search:semantic` and `/v1/search:hybrid` (and the console's
Search Semantic / Hybrid modes). Embedding runs locally by default, in-process, with no egress and no
API key, so a vector field adds semantic retrieval without an external service.

```yaml
    - { path: body_vec, type: VECTOR,
        vector: { source_field: body, model: bge-small-en-v1.5, dims: 384, metric: COSINE, provider: LOCAL } }
```

| `vector:` key | Default | Notes |
|---|---|---|
| `source_field` | none (required) | The mapped text field whose value is embedded. |
| `model` | `bge-small-en-v1.5` | Embedding model id; changing it is a re-embedding reindex. |
| `dims` | `384` | Vector dimensionality; must match the model's output width. |
| `metric` | `COSINE` | Distance metric: `COSINE`, `DOT`, or `L2`. |
| `provider` | `LOCAL` | Where embeddings run. `LOCAL` = the in-process embedder (the only provider today). |

The local provider loads the model from `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model>/`
(three files: `config.json`, `tokenizer.json`, `model.safetensors`). If the model isn't present the
field still builds, falling back to a deterministic dev embedder; provision the model for real
semantic quality (the demo's `just stack` does this for you). A vector field carries no inverted index
or columnar store, so the scalar knobs (`fast`, `cached`, `analyzer`, `record`, …) don't apply and are
rejected on it.

### Declaring timestamps {#declaring-timestamps}

A `DATE` is stored internally as epoch microseconds, the one canonical scale that range queries,
sort, the console time filter, and window pruning all use. A source column rarely *is* micros, though.
It may be an `int64` of epoch millis (very common), or an ISO-8601 string. Set a `format`
on the field and GrowlerDB normalizes it to canonical micros at ingest. A field with a `format` is
a `DATE` regardless of its source type, so a plain integer or string column becomes a real
timestamp. You don't also write `type: DATE`; the two together is rejected unless the type *is*
`DATE`.

| `format` | Source value | Example |
|---|---|---|
| `epoch_seconds` (`epoch_s`) | integer/digit-string seconds | `1782691200` |
| `epoch_millis` (`epoch_ms`) | …milliseconds | `1782691200000` |
| `epoch_micros` (`epoch_us`) | …microseconds (already canonical) | `1782691200000000` |
| `epoch_nanos` (`epoch_ns`) | …nanoseconds (truncated to micros) | `1782691200000000000` |
| `rfc3339` (`iso8601`) | an offset-aware datetime **string** | `2026-06-29T12:30:00Z`, `…+02:00` |
| `date_only` (`date`) | a `YYYY-MM-DD` **string** (UTC midnight) | `2026-06-29` |

```yaml
    - { path: ts,        format: epoch_ms }   # int64 epoch-millis  → timestamp
    - { path: created,   format: rfc3339 }    # "2026-06-29T12:30:00Z" → timestamp
    - { path: birthday,  format: date_only }  # "2026-06-29" → UTC midnight
    - { path: native_ts, type: DATE }         # a native Iceberg `timestamp` needs no format
```

Parsing fails loudly rather than silently. A value that doesn't match its declared `format` is
skipped for that document (the rest of the document still indexes), so it is never written as an
off-by-1000 or off-by-timezone date. To change a field's `format`/unit on an existing index, re-run
the build (`growlerdb alter` / reindex), and the new unit applies as documents are re-ingested.

> **Windowing.** A time-windowed index (`windowing:`) buckets on the same canonical micros scale,
> so its `field` (and optional `event_time_field`) must be a `DATE`. Declare a `format` on the
> source column (the demo's `ingest`/`event` are `epoch_ms`) or use a native Iceberg timestamp. A
> raw `LONG` window field is rejected because its unit is ambiguous. *Migration:* a
> windowed index that used a millis-`LONG` window field must re-declare it with a `format` and
> reindex; the stored window ids/zone-maps were millis and are rebuilt on the canonical micros scale.

### The composite key

Documents are identified by a composite, partition-aware key, which is `partition_fields` plus
`identifier_fields`. It drives sharding (hash on the key by default, or partition routing when
`partition_fields` are set, co-locating a partition on a shard), and it is what a search returns and
`/v1/keys:get` hydrates against.

## Authentication & tenancy {#authentication--tenancy}

The gateway is open unless you enable AuthN. Your options:

- OIDC/JWT: `growlerdb gateway --oidc-issuer <url> --oidc-audience <aud>`. Tokens are validated
  against the issuer's JWKS, and the verified `sub`/tenant/roles claims replace any caller-asserted
  headers at the trust boundary.
- API keys and mTLS between services are also supported (see the TLS flags on `serve`/
  `gateway` and the security model in [SECURITY.md](https://github.com/GrowlerDB/growlerdb/blob/main/SECURITY.md)).

**Tenant scoping.** When an index sets `tenant_field`, every read gets a mandatory, non-scoring
`tenant_field = <verified claim>` filter ANDed in. No query (`OR`, nested bool) can widen past it,
and a request with no verified claim is denied. RBAC maps verified roles to operation scopes
(viewer / index-admin / operator / service).

### Service credentials & internal transport {#service-credentials}

The control plane serves the internal, service-to-service RPCs: index registration, shard-map
reads, and window placement (`RegisterServedIndex`, `RegisterNode`, `ResolveWindowOwner`, `GetIndex`,
…). These sit in a separate layer from the user-facing gateway auth above. They authenticate cluster
services (node, gateway, connector), not end users.

- `GROWLERDB_SERVICE_TOKEN` (or `growlerdb control-plane --service-token <token>`) is a shared
  secret gating every control-plane RPC. When set, the control plane rejects any call whose
  `x-growlerdb-service-token` metadata doesn't match (a constant-time comparison) with
  `UNAUTHENTICATED`, so only services holding the token can reach the internal RPCs. When unset, the
  control plane is open, the bare local-dev default, so `just` and a loopback control plane work
  with no configuration. This is enforced regardless of the user-auth mode, so it closes the internal
  RPCs even under `--login-secret` (where user authorization is intentionally open).

  Every service that dials the control plane reads the same `GROWLERDB_SERVICE_TOKEN` and attaches it
  automatically: the node and gateway (their control-plane clients) and the connector
  (`ResolveWindowOwner` / `GetIndex`). Set the same value everywhere in the mesh. The `just stack`
  demo sets a shared `-change-me` token so its control plane is closed by default.

- Control-plane TLS: the control plane can serve over TLS (and mTLS) with `growlerdb control-plane
  --tls-cert <pem> --tls-key <pem> --tls-client-ca <pem>` (the same TLS flags as `serve`/`gateway`).
  It is optional and off by default (the loopback demo doesn't need it). When enabled, clients
  dial it over TLS by setting `GROWLERDB_CP_TLS_CA` (PEM CA verifying the control-plane's server
  certificate); add `GROWLERDB_CP_TLS_CERT` / `GROWLERDB_CP_TLS_KEY` for a client identity (mTLS) and
  `GROWLERDB_CP_TLS_DOMAIN` (default `localhost`) for the expected server SAN. Unset ⇒ plaintext.
