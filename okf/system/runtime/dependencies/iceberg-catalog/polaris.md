---
type: Dependency
title: Apache Polaris
description: The Iceberg REST catalog that governs tables and hydration.
tags: [dependency, catalog, polaris, iceberg]
timestamp: 2026-07-04T14:22:00
---

# Apache Polaris

The **Iceberg REST catalog** GrowlerDB reads through — it governs the source tables and the
authoritative-row [hydration](/product/functional/hydration.md). GrowlerDB's
[source reader](/system/git-repo.md) resolves tables and reads data files via Polaris (the
[REST Catalog API](/system/runtime/dependencies/iceberg-catalog/rest-api.md)).

## How it's wired

- Configured via `GROWLERDB_CATALOG_URI` / warehouse / credential.
- Backed by a persistent [Postgres metastore](/system/runtime/dependencies/metastore/postgres.md) so
  the catalog survives restarts — without it, an in-memory catalog would forget tables on a bounce and
  orphan the index (the [lineage guard](/product/functional/ingestion/cdc.md) covers that case).
- A catalog outage pauses ingestion (source-side retries) while search continues; recovery is
  automatic when it returns.
