---
type: Actor
title: Application developer
description: Embeds GrowlerDB search into an application or retrieval pipeline over Iceberg data.
tags: [actor, developer, integration]
timestamp: 2026-07-04T14:22:00
---

# Application developer

The engineer who **builds on** GrowlerDB — adding a search feature to an app, or a retrieval step to a
pipeline, over data the company already keeps in Iceberg.

## Goals

- Integrate search + hydration via the [client SDKs](/product/interfaces/client-sdks.md),
  [gRPC](/product/interfaces/grpc.md)/[REST](/product/interfaces/rest.md) API, or
  [SQL UDFs](/product/interfaces/sql-udfs.md).
- Render paginated, sortable result tables from cached/fast fields with no hydration, then hydrate on
  row-open (see the [search-backed app](/product/use-cases/search-backed-app.md) use case).
- Authenticate as a [service account](/product/actors/service-account.md) (API token).

## Reaches it through

The programmatic interfaces (SDKs, gRPC/REST, SQL UDFs).
