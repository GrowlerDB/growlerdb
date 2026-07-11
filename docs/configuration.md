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

The Iceberg source + object store are configured by environment (these override the
local-dev defaults):

| Variable | Default | Notes |
|---|---|---|
| `GROWLERDB_CATALOG_URI` | `http://localhost:8181/api/catalog` | Iceberg REST catalog. |
| `GROWLERDB_WAREHOUSE` | `growlerdb` | Catalog warehouse name. |
| `GROWLERDB_CATALOG_CREDENTIAL` | — | Catalog OAuth `client:secret` (Polaris). |
| `GROWLERDB_CATALOG_SCOPE` | — | Optional catalog OAuth scope. |
| `GROWLERDB_S3_ENDPOINT` | `http://localhost:9000` | Object-store endpoint. |
| `GROWLERDB_S3_ACCESS_KEY` | `minioadmin` | Object-store access key. |
| `GROWLERDB_S3_SECRET_KEY` | `minioadmin` | Object-store secret key. |
| `GROWLERDB_S3_REGION` | `us-east-1` | Object-store region. |
| `GROWLERDB_BACKUP_BUCKET` | — | Bucket for `backup`/`restore` (reuses the `GROWLERDB_S3_*` credentials/endpoint). |

In Kubernetes these are wired from a ConfigMap (non-secret) + a Secret (credentials) by the
[Helm chart](deployment#kubernetes-helm); the credentials should come from a `Secret`, never inline.

## The index definition

An index is defined by a small YAML document (pass it to `growlerdb index --def file.yaml`, or
author it in the console's **Indexes → Create**, which introspects the source schema for you). With
no definition, GrowlerDB **auto-maps** every source field.

```yaml
name: docs
source:
  iceberg:
    catalog: growlerdb        # catalog name
    table: growlerdb.docs     # namespace.table
# key: optional — derived from the source's identifier/partition hints when omitted.
key:
  partition_fields: [region]  # co-locate a partition on a shard (partition routing)
  identifier_fields: [id]     # the per-document identity
# tenant_field: optional — enables non-widenable tenant scoping (must be a KEYWORD field).
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
| `LONG` | 64-bit integer — range, sort, numeric facets. |
| `DOUBLE` | 64-bit float — range, sort. |
| `BOOL` | Boolean. |
| `DATE` | Date / timestamp — range, date-histogram, time pruning. |
| `IP` | IP address — CIDR/range match. **Never auto-derived** (declare it explicitly; it arrives as a string). |

### Declaring timestamps {#declaring-timestamps}

A `DATE` is stored internally as **epoch microseconds** — the one canonical scale the range queries,
sort, the console time filter, and window pruning all use. A source rarely *is* micros, though: a
column may be an `int64` of epoch **millis** (very common), or an ISO-8601 **string**. Set a `format`
on the field and GrowlerDB normalizes it to canonical micros at ingest. **A field with a `format` is
a `DATE` regardless of its source type** — so a plain integer or string column becomes a real
timestamp (you don't also write `type: DATE`; the two together is rejected unless the type *is*
`DATE`).

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

Parsing is **loud, not silent**: a value that doesn't match its declared `format` is skipped for that
document (the rest of the document still indexes) rather than being written as an off-by-1000 or
off-by-timezone date. To change a field's `format`/unit on an existing index, re-run the build
(`growlerdb alter` / reindex) — the new unit applies as documents are re-ingested.

> **Windowing.** A time-**windowed** index (`windowing:`) buckets on the same canonical micros scale,
> so its `field` (and optional `event_time_field`) **must be a `DATE`** — declare a `format` on the
> source column (the demo's `ingest`/`event` are `epoch_ms`) or use a native Iceberg timestamp. A
> raw `LONG` window field is **rejected** (its unit is ambiguous). *Migration:* a
> windowed index that used a millis-`LONG` window field must re-declare it with a `format` and
> reindex — the stored window ids/zone-maps were millis and are rebuilt on the canonical micros scale.

### The composite key

Documents are identified by a **composite, partition-aware key** = `partition_fields` +
`identifier_fields`. It drives sharding (hash on the key by default; **partition routing** when
`partition_fields` are set, co-locating a partition on a shard) and is what a search returns and
`/v1/keys:get` hydrates against.

## Authentication & tenancy {#authentication--tenancy}

The gateway is **open** unless you enable AuthN. Options:

- **OIDC/JWT** — `growlerdb gateway --oidc-issuer <url> --oidc-audience <aud>`. Tokens are validated
  against the issuer's JWKS; the verified `sub`/tenant/roles claims replace any caller-asserted
  headers at the trust boundary.
- **API keys** and **mTLS between services** are also supported (see the TLS flags on `serve`/
  `gateway` and the security model in [SECURITY.md](https://github.com/GrowlerDB/growlerdb/blob/main/SECURITY.md)).

**Tenant scoping.** When an index sets `tenant_field`, every read gets a mandatory, non-scoring
`tenant_field = <verified claim>` filter ANDed in — no query (`OR`, nested bool) can widen past it,
and a request with no verified claim is denied. RBAC maps verified roles to operation scopes
(viewer / index-admin / operator / service).
