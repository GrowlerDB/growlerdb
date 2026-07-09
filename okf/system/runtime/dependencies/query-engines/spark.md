---
type: Dependency
title: Apache Spark
description: Runs the changelog connector and the Spark SQL search UDF.
tags: [dependency, spark, jvm]
timestamp: 2026-07-04T14:22:00
---

# Apache Spark

Apache Spark hosts two JVM integrations:

- The [connector](/system/runtime/components/connector.md) — a Spark job reading the Iceberg changelog
  and streaming into a node.
- The [Spark SQL search UDF](/product/interfaces/sql-udfs.md) — `GrowlerDbSearch.search(…) →
  Dataset<Row>` for search-then-join.

## Notes

Java module `connector/` (JDK 21). Spark is a deployment/runtime dependency of the ingestion +
SQL-search paths, not of the core engine.
