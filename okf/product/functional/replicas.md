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
