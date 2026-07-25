---
type: Feature
title: Replicas
description: Read-only replicas of a shard kept in sync by segment shipping.
tags: [feature, replicas, ha]
timestamp: 2026-07-04T14:22:00
---

# Replicas

A **read-only replica** serves queries for a shard while staying in sync with the primary — for read
throughput and availability.

## Behavior

- `growlerdb serve --replica` exposes a read-only surface and polls for snapshot advances, hot-swapping
  the shard on a new snapshot (segment shipping via the backup store); a lost node can be rebuilt from
  [backup](/product/functional/index-management/backup-restore.md).
- Replicas never [compact](/product/functional/index-management/compact.md).

## Notes

Single-shard replica today; a windowed/multi-shard zero-downtime replica set is future work — see
[known limitations](/quality/known-limitations/index.md). HA today = shards spread + PDBs + PV
self-heal + honest partial results during a shard restart.

**Designed successor.** [D53](/system/decisions/d53-unit-replication.md) replaces this single-shard
`serve --replica` model with a **per-unit replication factor** over the
[placement pool](/system/decisions/d52-placement-pool.md): the control plane assigns R holders per
`(index, shard|window)` unit (one primary writer + R−1 read replicas), reads scatter to any live
holder, and replicas hydrate **read-through from shared object storage** (cold-tier-led) so failover
is metadata-bound. See [high availability](/system/high-availability.md). Not yet built.
