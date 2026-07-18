---
title: Comparison & positioning
layout: default
nav_order: 8
---

# Comparison & positioning
{: .no_toc }

Where GrowlerDB fits — and where it doesn't — next to the tools you're probably already running.
For the numbers behind the claims here, see [Performance (directional)](performance); for the
mechanics of moving over, see [Migrating from Elasticsearch/OpenSearch](migration-from-elasticsearch).

1. TOC
{:toc}

## The one-line frame

GrowlerDB is a **derived full-text index over your Apache Iceberg lakehouse**. Iceberg stays the
system of record; a search returns document **keys** that hydrate back to the authoritative rows. That
single design choice — *don't own a second copy of the data* — is what separates it from both search
engines and query engines.

## vs. Elasticsearch / OpenSearch

| | Elasticsearch / OpenSearch | GrowlerDB |
|---|---|---|
| System of record | the engine's own `_source` (a **second copy**) | **Apache Iceberg** (your lake) |
| A search returns | full documents | document **keys** + score → hydrate on demand |
| Ingestion | `_bulk` / index API you operate | a **changelog connector** tracks the source table |
| Staying in sync | your job (dual-writes, reindex) | derived from the Iceberg changelog |
| Governance | search-time copy, separately secured | hydration returns the **catalog-governed** live row |
| Rebuild / re-shard | reindex the world | drop and rebuild the derived index from source |

**Choose GrowlerDB when** your data already lives in (or can land in) Iceberg and you don't want a
parallel datastore to provision, secure, and keep from drifting — especially for logs, telemetry, and
event/time-series data where the lake is already the archive of record.

**Elasticsearch/OpenSearch is the better fit when** you need capabilities GrowlerDB doesn't ship yet:
sub-10 ms **authoritative** single-document retrieval (ES serves `_source` from its own store; GrowlerDB
pays an Iceberg hydration round-trip — cache display fields to close the gap for the display case),
**vector / hybrid search and reranking** (designed but [not shipped](roadmap)), a **write API**
(GrowlerDB ingests from the changelog, not `_bulk`), or the full breadth of the aggregation/scripting/
ingest-pipeline surface. GrowlerDB's [OpenSearch `_search` adapter](opensearch-adapter) covers a
documented **read-path subset** and returns `501` on anything unsupported — no silent wrong answers.

## vs. Trino / Spark full-text on Iceberg

You can already run `LIKE`/`regexp` or a full-text UDF over an Iceberg table with Trino or Spark. The
difference is **scan vs. index**:

| | Trino / Spark on Iceberg | GrowlerDB |
|---|---|---|
| How a text query runs | **scans** the table (grows with rows) | **inverted-index** lookup (flat in dataset size) |
| Typical filtered-search latency | ~150–300 ms at 1M rows | **single-digit ms** |
| Ranking | none / bolt-on | BM25 relevance, per-hit explain |
| Returning rows | reads columns during the scan | hydrates **only** the K matching rows by key |
| Same source of truth | ✅ the Iceberg table | ✅ the same Iceberg table |

On the [directional benchmark](performance) GrowlerDB is **~50–170× faster than a Trino scan** on
filtered search, and the gap widens with data size because the index lookup stays flat while the scan
grows. Both read the same Iceberg table — GrowlerDB doesn't replace your query engine, it adds the
**search** access path the query engine lacks. (For authoritative full-row retrieval, GrowlerDB's
locator-targeted hydration is still ~2× faster than a Trino `SELECT *`, because it reads only the
matching rows rather than scanning.)

## When GrowlerDB is a good fit

- Your data is in (or can land in) **Apache Iceberg**, and you'd rather not run a separate search
  cluster that owns a second copy.
- **Logs, telemetry, events, catalogs** — text + filters + ranges + time windows, where fast filtered
  search and cheap cold storage matter.
- You want search results that resolve to the **live, governed** lakehouse row, not a search-time
  snapshot.
- You value **operational honesty**: rebuild-from-source recovery, an explicit compatibility subset,
  and no silent wrong answers.

## When to reach for something else (today)

- You need **vector / hybrid retrieval or reranking** (the RAG path) — designed but not shipped; see
  the [roadmap](roadmap).
- You need **sub-10 ms authoritative single-document** retrieval and can't cache display fields — ES
  `_source` wins on raw latency there.
- You need a **write/ingest API** into the search engine itself — GrowlerDB ingests from the Iceberg
  changelog, not a `_bulk` endpoint.
- Your data **isn't and won't be in a lakehouse table format** — GrowlerDB's whole model is the
  derived-index-over-Iceberg one.

See [Migrating from Elasticsearch/OpenSearch](migration-from-elasticsearch) for the two integration
paths (native API or the `_search` adapter) and a cutover checklist.
