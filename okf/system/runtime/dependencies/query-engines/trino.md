---
type: Dependency
title: Trino
description: Hosts the growlerdb_search polymorphic table function.
tags: [dependency, trino, jvm, sql]
timestamp: 2026-07-04T14:22:00
---

# Trino

Trino hosts the [`growlerdb_search`](/product/interfaces/sql-udfs.md) polymorphic table function
(search-then-join from SQL). Module `connector-trino/` — a separate Trino connector plugin.

## Notes

Built on a different JDK from the Spark module (Trino's SPI is JDK-23 bytecode), so it is its own
subproject. A runtime dependency only of the SQL-search path.
