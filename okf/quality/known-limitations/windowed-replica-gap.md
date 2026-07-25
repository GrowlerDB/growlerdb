---
type: Concept
title: Windowed / multi-shard replicas
description: Read replicas are single-shard today; zero-downtime windowed or multi-shard replica sets are future work.
tags: [quality]
timestamp: 2026-07-04T14:22:00
---

# Windowed / multi-shard replicas

Read replicas are single-shard today (`serve --replica`, [D14](/system/decisions/d14-replica-sync.md));
windowed placement is **primary-only** ([D33](/system/decisions/d33-windowed-topology.md)), so a dead
node's windows are unavailable until it returns. Zero-downtime windowed or multi-shard replica sets
were future work.

**Now designed ([D53](/system/decisions/d53-unit-replication.md)).** A **per-unit replication factor**
over the [placement pool](/system/decisions/d52-placement-pool.md) gives R holders per
`(index, shard|window)` unit — one primary writer + R−1 read replicas, reads to any live holder, dead
units re-placed onto survivors with a warm replica serving through. Replicas hydrate read-through from
shared object storage (cold-tier-led), so failover is metadata-bound. See
[high availability](/system/high-availability.md). Designed, not yet built (the `true-ha` epic).
