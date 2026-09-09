---
title: Performance
description: GrowlerDB performance: index-lookup search latency that stays flat as the table grows, with directional benchmarks against table scans on Iceberg.
layout: default
nav_order: 9
---

# Performance
{: .no_toc }

1. TOC
{:toc}

GrowlerDB runs a head-to-head benchmark against OpenSearch on the same cluster and dataset. This page
reports the current results: ingestion throughput, freshness, index size, and query latency.

## Test setup

| | |
|---|---|
| Dataset | `http_logs`, 25.9M rows (~10 GB raw), non-windowed Iceberg (copy-on-write) |
| Hardware | 6× ccx43 (16 vCPU / 64 GB) on Hetzner Cloud, nbg1 |
| GrowlerDB | current build; Spark connector streams the Iceberg changelog |
| OpenSearch | 2.19.1, fed by Data Prepper 2.15.1 CDC (Iceberg source) |
| Object store | Hetzner Object Storage (S3-compatible) |

GrowlerDB indexes the Iceberg table and hydrates matching rows back from it. OpenSearch ingests a
second copy of the data through CDC and serves it from its own store. Both engines index the same
rows and answer the same query set.

## Ingestion, freshness, and size

| Metric | GrowlerDB | OpenSearch |
|---|---:|---:|
| Ingest throughput (cold-sync backfill) | ~16.8k docs/s | ~17.5k docs/s |
| Freshness: a new row becomes searchable (p50) | ~2.5 s | ~58 s |
| Index size (primary) | ~3.9 GB | 14.3 GB |
| Index size vs the 10 GB raw corpus | ~0.39× | ~1.43× |

GrowlerDB and OpenSearch backfill at comparable rates. During the GrowlerDB backfill the serving
nodes sat at 6% CPU, so the connector pipeline sets the pace and headroom remains.

GrowlerDB makes a new row searchable about 23× faster. It commits streamed changes continuously,
while the CDC path polls Iceberg snapshots on an interval.

GrowlerDB's index holds keys and search structures, not a copy of the source rows, so it stays
smaller than the raw data. OpenSearch stores the documents (`_source`) plus a completion structure,
so its index runs larger than the raw data. GrowlerDB ran a single primary per shard; OpenSearch ran
one replica (29.3 GB on disk), so the table compares primaries.

## Query latency

Median server-side latency, measured at rest.

| Query group | GrowlerDB | OpenSearch |
|---|---:|---:|
| Selective lookups and counts (point-lookup, exact term, CIDR, `match_all`) | ~7 ms | ~5 ms |
| Broad filters, ranges, and text (high-cardinality term, range, phrase, boolean) | ~33 ms | ~5 ms |
| Full-document retrieval (top-20, hydrated) | ~200 ms | ~10 ms |

Selective lookups and counts run about even on both engines. Broad filters cost GrowlerDB more: it
does the retrieval work over large posting lists, while OpenSearch answers many of them from warmed
counts and skip lists. Both groups stay within interactive range, and the broad-filter gap is an
active optimization target.

Full-document retrieval is where the two designs differ. OpenSearch returns a stored copy of each
document from its own index. GrowlerDB fetches the authoritative, governed row from Iceberg by key, so
retrieval reads the object store. Retrieval averaged about 200 ms (roughly 20 ms when the matching
rows cluster in the sorted layout, up to about 300 ms when they scatter), against about 10 ms for
OpenSearch. The result is the live lakehouse row, not a second copy that can drift. Under heavy
concurrent load, retrieval throughput follows the object store's capacity, while index-only latency
holds flat.

## Reading these numbers

GrowlerDB reflects the current optimization build; OpenSearch 2.19.1 is a fixed baseline measured on
the same cluster and dataset. Retrieval and broad-scan latency keep improving between rounds, so a
per-query breakdown will follow once the numbers settle. Reproduce the setup from
[`bench/`](https://github.com/GrowlerDB/growlerdb/tree/main/bench).
