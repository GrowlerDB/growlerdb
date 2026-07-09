---
type: Interface
title: SQL UDFs (Trino / Spark)
description: Search-then-join from SQL engines — GrowlerDB returns keys+score to JOIN against Iceberg.
tags: [interface, sql, trino, spark]
resource: /connector
timestamp: 2026-07-04T14:22:00
---

# SQL UDFs (Trino / Spark)

Search from SQL engines: GrowlerDB returns matching **keys + score**, which the query then JOINs
against the authoritative Iceberg table — search-then-join, so full-text lives in GrowlerDB and the
rows stay in the lakehouse.

- **Spark** — `GrowlerDbSearch.search(spark, …) → Dataset<Row>` (module `connector/`).
- **Trino** — a `growlerdb_search` polymorphic table function (module `connector-trino/`; separate JDK
  from the Spark module).

## Notes

Both wrap the gRPC [Search](/product/interfaces/grpc.md) client and a shared column mapping. The Spark
connector also serves the [changelog ingestion](/system/runtime/components/connector.md) role
(different code path). Live engine round-trips are stack-gated in CI.
