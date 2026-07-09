---
type: Feature
title: Streaming ingestion
description: Continuously keep an index in sync with its Iceberg source via the connector.
tags: [feature, ingestion, streaming]
timestamp: 2026-07-04T14:22:00
---

# Streaming ingestion

Keep an index **continuously in sync** with its source — no hand-built pipeline. The
[connector](/system/runtime/components/connector.md) reads the Iceberg table (append fast path, or
[changelog/CDC](/product/functional/ingestion/cdc.md)) and streams document batches to a
[node](/system/runtime/components/node.md)'s Write service.

## Behavior

- **Append fast path** for append-mostly sources (telemetry/logs); the index snapshot advances toward
  the source head as batches commit.
- **Bounded catch-up**: a large backlog is cut into sub-batches at snapshot boundaries so a big
  window drains without exceeding gRPC/memory limits.
- The connector reconnects on a node roll instead of wedging; progress is
  [checkpointed](/product/functional/ingestion/checkpoints-exactly-once.md).
- **Scale-out** ([D32](/system/decisions/d32-parallel-ingest.md)): a set of W connector workers
  partitions one table by shard group — each worker syncs only the shards it owns, so ingest
  throughput grows with workers, with no coordination between them.

## Notes

Sources: Iceberg (Spark/Flink), a Kafka topic, or CDC. Lag (snapshots behind source) is visible on the
Ingestion screen; the [ingest operator](/product/actors/ingest-operator.md) monitors it.
