---
type: Component
title: Connector
description: The Spark worker(s) that stream the Iceberg changelog into the nodes' Write services — one process for low-scale syncing, or a shard-group set of W workers for horizontal ingest scale-out.
tags: [component, connector, ingestion, spark, scale]
resource: /connector
timestamp: 2026-07-04T14:22:00
---

# Connector

**Stateless Spark workers** (JVM) that read an Iceberg table's changelog/appends and stream document
batches to the [nodes](/system/runtime/components/node.md)' Write gRPC services — the
[ingestion](/product/functional/ingestion/streaming.md) engine. Two deployment modes
([D32](/system/decisions/d32-parallel-ingest.md)), one code path:

- **Single connector** (`connector.yaml`, a `replicas:1` Deployment) — the simple low-scale mode:
  one process writes all shards.
- **Connector set** (`connector-set.yaml`, a StatefulSet of `W` workers) — horizontal scale-out:
  worker `i` (its pod ordinal) owns shards `{s : s % W == i}`, filters the changelog
  **executor-side** to its owned rows (~1/W of the window per driver), writes only its shards
  (empty lockstep sub-batches included), and resumes from its own group's lineage-min checkpoint.
  One shard, one writer — the continuity guard holds with no coordination; scaling `W` is a
  plain StatefulSet roll (regrouping self-heals via the window-covering guard). Never run both
  modes on one table at once (two writers on a shard fail fast: `CHECKPOINT_GAP`).

## Responsibilities

- **Stream** the changelog read→map→commit in bounded chunks: pull one partition at a time
  (`toLocalIterator`) and flush a sub-batch capped at `maxCommitRows`, cut only at snapshot boundaries,
  so driver memory is **O(chunk), not O(window)** — a large post-outage backlog no longer OOMs the
  driver (was `collectAsList` of the whole window → exit 52). The per-trigger under-read gate
  ([D31](/system/decisions/d31-ingest-loss-guards.md)) runs first as a **distributed `count()`** (not a
  driver collect), and its `Σ added-records` metadata walk is **bounded to the window's snapshots**
  (`committed_at ≥` the resume point, with a full-scan fallback under clock skew) rather than scanning
  all table history each trigger.
- Apply insert/update/delete with idempotent batch ids →
  [exactly-once](/product/functional/ingestion/checkpoints-exactly-once.md) resume via `GetCheckpoint`.
- Reconnect on a node roll (new pod IP) instead of wedging.

## Notes

Java module `connector/`; also hosts the Spark SQL search UDF (a different code path). The engine ↔
connector boundary is [gRPC](/product/interfaces/grpc.md). Authenticates as a
[service account](/product/actors/service-account.md).
