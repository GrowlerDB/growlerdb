---
type: Actor
title: Platform admin / operator
description: Creates and operates GrowlerDB indexes, configures access, and runs the cluster.
tags: [actor, admin, ops]
timestamp: 2026-07-04T14:22:00
---

# Platform admin / operator

The person who **stands up and operates** GrowlerDB for their organization.

## Goals

- Create/alter/drop/reindex indexes over Iceberg tables; run
  [index management](/product/functional/index-management/index.md) (compaction, backup, aliases/ILM).
- Configure [auth, RBAC, and tenant isolation](/product/functional/auth/index.md); manage
  [users](/product/functional/user-management.md) and API tokens.
- Deploy and scale the cluster ([deployment](/system/deployment/index.md), shards, windows,
  cold tiering, replicas) and keep it healthy via
  [observability](/product/functional/observability.md).

## Reaches it through

The [CLI](/product/interfaces/cli.md), the [console UI](/product/interfaces/ui.md) (Indexes /
Settings), and the admin [REST](/product/interfaces/rest.md)/[gRPC](/product/interfaces/grpc.md) API.
