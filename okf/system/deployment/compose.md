---
type: Concept
title: Docker Compose
description: The single-host stack for dev and CI — dependencies + GrowlerDB + observability.
tags: [deployment, compose, dev]
resource: /deploy/compose
timestamp: 2026-07-04T14:22:00
---

# Docker Compose

A single-host stack (`deploy/compose`) that brings up the dependencies
([MinIO](/system/runtime/dependencies/object-storage/minio.md),
[Polaris](/system/runtime/dependencies/iceberg-catalog/polaris.md) + Postgres), and — in the `stack`
profile — GrowlerDB itself (control-plane + **two nodes** + gateway) plus the
[LGTM](/system/runtime/dependencies/lgtm.md) stack. The fastest path to a running GrowlerDB, and the
environment [CI e2e](/quality/ci-and-gates.md) runs against.

## Notes

Profiles: `seed` (sample tables), `stack` (GrowlerDB + LGTM), `pipeline` (the streaming demo with
Redpanda). Long-running services carry `restart:` policies (self-heal); chaos drills exercise recovery
([reliability](/quality/reliability.md)).

**Two demo indexes:** the `seed` profile writes `growlerdb.docs` (3 rows, the minimal
E2E table) *and* the richer `growlerdb.catalog` (10 rows — one field of every type). The `stack`
profile serves each from its own node (`node` → `docs`, `node-catalog` → `catalog`, built from
`catalog.yaml`), and the single `--all-indexes` [gateway](/system/runtime/components/gateway.md) routes
each request to its named index ([D35 multi-index routing](/system/decisions/d35-multi-index-routing.md)).
The [getting-started](/product/interfaces/website.md) **query playground** exercises the `catalog`
index through the gateway — every Lucene/KQL operator (term, phrase, keyword, set, numeric/float/date
range, CIDR, wildcard, prefix, fuzzy, boost, bool, `NOT`, match-all, regex) against known rows. With
two indexes served and no default configured, every search / `keys:get` request must name its index.
