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
profile — GrowlerDB itself (control-plane + node + gateway) plus the
[LGTM](/system/runtime/dependencies/lgtm.md) stack. The fastest path to a running GrowlerDB, and the
environment [CI e2e](/quality/ci-and-gates.md) runs against.

## Notes

Profiles: `seed` (sample table), `stack` (GrowlerDB + LGTM), `pipeline` (the streaming demo with
Redpanda). Long-running services carry `restart:` policies (self-heal); chaos drills exercise recovery
([reliability](/quality/reliability.md)).
