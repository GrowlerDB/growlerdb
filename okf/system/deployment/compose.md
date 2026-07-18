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

Profiles: `seed` (sample tables), `stack` (GrowlerDB + LGTM), `catalog` (the second `node-catalog`),
`pipeline` (the streaming demo with Redpanda). Long-running services carry `restart:` policies
(self-heal); chaos drills exercise recovery ([reliability](/quality/reliability.md)).

**External lakehouse (`external.yml`):** a companion file (`deploy/compose/external.yml` + `.env`) runs
only GrowlerDB (control-plane + node + gateway, off the published image) against a user's **own**
external Iceberg REST catalog + S3 store — no bundled MinIO/Polaris/seed. It's the "day 2" step after
the demo; see the [getting-started site](/product/interfaces/website.md) *Connecting your own Iceberg
table* page for the walkthrough and limitations (REST-only catalog, static S3 keys, forced path-style).

**Two demo indexes:** the `seed` profile writes `growlerdb.docs` (3 rows, the minimal
E2E table) *and* the richer `growlerdb.catalog` (10 rows — one field of every type). Each is served from
its own node (`node` → `docs`, `node-catalog` → `catalog`, built from `catalog.yaml`), and the single
`--all-indexes` [gateway](/system/runtime/components/gateway.md) routes each request to its named index
([D35 multi-index routing](/system/decisions/d35-multi-index-routing.md)). `node-catalog` lives in its
own `catalog` profile — `just stack` co-activates `stack`+`catalog`, but the streaming demo
(`just pipeline`, `stack`+`pipeline`) deliberately excludes it (no seeded `growlerdb.catalog` source
there), and the gateway resolves indexes lazily rather than hard-depending on it.
The [getting-started](/product/interfaces/website.md) **query playground** exercises the `catalog`
index through the gateway — every Lucene/KQL operator (term, phrase, keyword, set, numeric/float/date
range, CIDR, wildcard, prefix, fuzzy, boost, bool, `NOT`, match-all, regex) against known rows. With
two indexes served and no default configured, every search / `keys:get` request must name its index.
