---
type: Decision
title: 'D39. Automatic cold-tiering is node-local, not a scheduled job'
description: Each node parks its own aged windows to cold read-through in-process on a timer (the hot→cold counterpart of the existing pre-warm loop), rather than an external CronJob — because parking needs the window's bytes on the node's own local volume.
tags: [decision, adr, cold-tier, storage]
timestamp: 2026-07-16T00:00:00
---

# D39. Automatic cold-tiering is node-local, not a scheduled job

**Decision.** A windowed node **parks its own aged windows in-process**, on a background timer
(`GROWLERDB_PARK_INTERVAL_SECS`, opt-in), demoting each window past the index's `hot_windows` policy to
[cold read-through](/product/functional/cold-tiering.md). This is the hot→cold counterpart of the
already-existing access-driven **pre-warm** loop (cold→hot), and it reuses the same machinery: back the
window up through the live serving handle (no second writer), atomically swap the handle to a
read-through shard, then evict the local Tantivy bulk (keeping the tiny locator + `aux.redb` local).
A parked window immediately gets a pre-warm watcher, so a re-heated window promotes itself back to hot
— the pair is a self-managing hot/cold set.

**Why not a scheduled (CronJob) orchestrator, like the reconcile backstop?** Reconcile only *triggers*
work over gRPC — the owning node does the read + repair. Parking is different: it needs the window's
**bytes**, which live on the node's `ReadWriteOnce` local volume, already mounted by the running node
pod. A separate job pod cannot mount that volume (and generally won't even land on the same host), so
the orchestrator pattern does not transfer. In-process parking is the only option that touches the data
where it lives, and it also makes operation automatic for free by mirroring pre-warm.

**Safety.** Victims are aged windows outside the hot horizon — immutable, not receiving writes. Only
one node owns the current (actively written) window and it is that node's most-recent window, so a
per-node "keep the most recent N hot" policy never parks a window still being written. The backup +
durable marker land **before** any local eviction, so a crash mid-park always leaves a fully-serving
hot shard, never a markerless empty window. The two background writers (auto-compaction, locator
re-map) stand down the moment a window becomes read-only (no writer), so an in-place swap is race-free.

**Deployment.** The Helm chart wires this on the node StatefulSet (`coldTier.*`): the backup bucket,
park cadence, and read-through cache size — the object store is the same `GROWLERDB_S3_*` target as the
Iceberg source. Enabling parking with no backup bucket configured is a startup error, never a silent
no-op.

**Status.** Accepted.
