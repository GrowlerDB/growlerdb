---
type: Dependency
title: PostgreSQL
description: The persistent relational metastore backing the Iceberg catalog.
tags: [dependency, metastore, postgres]
timestamp: 2026-07-04T14:22:00
---

# PostgreSQL

Postgres is the **persistent relational metastore** for
[Polaris](/system/runtime/dependencies/iceberg-catalog/polaris.md) (relational-jdbc). It makes the
catalog durable — tables survive a catalog restart, so a Polaris bounce no longer wipes the namespace
and orphans the index.

## Notes

Volume-backed in the Compose/k8s stacks; the realm is bootstrapped into it by an idempotent init step.
A GrowlerDB **node** does not use Postgres directly — only the catalog does.
