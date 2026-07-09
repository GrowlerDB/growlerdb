---
type: Concept
title: Windowed index k8s deployment topology (resolved)
description: A windowed (time-sharded) index is now deployable on k8s via a control-plane-driven windowed node topology (D33) — nodes serve CP-assigned time windows, the connector streams each row to its window's owner, and the gateway hot-reloads windows. Documents the delivered design + residual follow-ups.
tags: [quality, scale, deployment, windowing]
timestamp: 2026-07-04T14:22:00
---

# Windowed index k8s deployment topology (resolved)

**Resolved (task-219 / [D33](/system/decisions/d33-windowed-topology.md)).** A **windowed** index is
now deployable on Kubernetes. Previously the [sharded Helm chart](/system/deployment/helm-k8s.md)
(Design 14) deployed **hash-sharded** indexes only — every node ran `growlerdb serve --shards N
--shard-ordinal K`, which a windowed index refuses in-engine (`ShardingWindowedUnsupported`) — so the
temporal workload `http_logs_windowed` (the [windowed sweet spot](/product/functional/windowing-time.md):
windowed sharding, [cold-tier park/revive](/product/functional/cold-tiering.md), event-time query
pruning; the shape of the [IoT-telemetry use case](/product/use-cases/iot-telemetry.md)) could not run
on-cluster.

## The delivered topology

When the chart's `index.windowed=true` (and `WORKLOAD=<windowed> deploy/k8s/scale-up.sh` sets it from
the workload's `index.yaml`), the node StatefulSet serves a **control-plane-driven windowed** index
instead of hash ordinals:

- **Nodes start empty, no fixed ordinal.** Each node writes `index.json` (the resolved def) once from
  the source (empty at deploy) — **no `--shards`/`--shard-ordinal`** — then `serve` auto-detects
  windowing and serves an empty windowed index. It heartbeats into the control plane's **placement
  pool** (`RegisterNode`).
- **The control plane places windows.** The connector computes each row's window
  (`WindowRouter`, byte-identical to the engine) and asks `ResolveWindowOwner`, which assigns the
  window to the least-loaded live node on first ask; the connector streams that window's rows there.
- **Nodes create windows on first write** (`WindowedWriteService`) and publish them live (shared
  search/suggest mux + `Gateway::swap_windowed`); they re-announce served windows + zone-maps each
  tick, so the **cluster gateway hot-reloads** the window set and prunes time-filtered queries.

So the scale test can now exercise both ends of the temporal/non-temporal fork on-cluster: the
hash-routed `http_logs` and the windowed `http_logs_windowed`.

## Residual follow-ups (not blocking deployment)

- **Window read-HA / replicas** — placement is primary-only; a dead node's windows are unavailable
  until it returns (its data is rebuildable from source). See
  [windowed / multi-shard replicas](/quality/known-limitations/windowed-replica-gap.md).
- **Resume bounding** — the connector resumes from the min committed checkpoint across committed
  windows; a cold restart can re-read from the oldest active window (correct but not minimal).
- **Connector worker parallelism** for windowed ingest (single connector today; [D32](/system/decisions/d32-parallel-ingest.md)
  worker sets are hash-only) and **distributed batch-build** with placement (streaming-only today).
- **Window-partition source maintenance** — the scale-run Iceberg maintenance CronJob is hash-tuned
  (`ts, request_id`); a window-aware sort key is a convenience, and maintenance is the user's concern.
- A **live on-cluster convergence run** of `http_logs_windowed` still validates the end-to-end path.
