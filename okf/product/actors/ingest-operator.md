---
type: Actor
title: Ingest operator
description: Configures and runs streaming ingestion and keeps indexes in sync with their sources.
tags: [actor, ingestion, ops]
timestamp: 2026-07-04T14:22:00
---

# Ingest operator

The person responsible for **keeping the index in sync with the source** — often the same team as the
[platform admin](/product/actors/platform-admin.md), focused on the ingestion path.

## Goals

- Configure and run the [streaming connector](/product/functional/ingestion/streaming.md) (Spark
  changelog reader / CDC) against an Iceberg table or Kafka topic.
- Ensure [exactly-once](/product/functional/ingestion/checkpoints-exactly-once.md) progress — resume
  from checkpoint after a restart, no loss or duplicates.
- Monitor **lag** (snapshots behind the source head) and throughput via the Ingestion screen; respond
  to catalog outages / backlogs (bounded catch-up).

## Reaches it through

The [console UI](/product/interfaces/ui.md) (Ingestion) and `/v1/ingestion` on the
[REST](/product/interfaces/rest.md) API; the connector itself is a
[component](/system/runtime/components/connector.md) deployed alongside.
