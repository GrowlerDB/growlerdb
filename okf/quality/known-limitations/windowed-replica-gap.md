---
type: Concept
title: Windowed / multi-shard replicas
description: Per-unit replication (D53) is built on the true-ha branch, but replica read-through failover covers parked (cold) windows only — a hot window has exactly one live copy until hot-tail shipping lands.
tags: [quality]
timestamp: 2026-07-26T18:00:00
---

# Windowed / multi-shard replicas

Read replicas were single-shard (`serve --replica`, [D14](/system/decisions/d14-replica-sync.md))
and windowed placement was **primary-only** ([D33](/system/decisions/d33-windowed-topology.md)), so a
dead node's windows were unavailable until it returned.

**Now built for the cold tier ([D53](/system/decisions/d53-unit-replication.md), the `true-ha`
epic).** A **per-unit replication factor** over the
[placement pool](/system/decisions/d52-placement-pool.md) gives R holders per
`(index, shard|window)` unit — one primary writer + R−1 read replicas. Replicas hydrate
**read-through from shared object storage** (`open_cold_replica`), the gateway routes each read
through a failover node across the holder set (health-aware: a transport-dead holder is down-marked
and skipped for a short cooldown, then re-probed), and dead units re-place onto survivors. See
[high availability](/system/high-availability.md).

## The remaining gap: hot windows have one live copy

A replica can only serve what exists in object storage, so replica read-through failover exists
**only for parked (cold) windows** today. A **hot** window's tail lives solely on its primary's
local disk until the window parks — killing that node loses the only live copy, and reads on that
window degrade to an honest `Unavailable`/`partial` until the CP re-places the unit and the new
owner revives it from the last checkpoint (audit finding
[HA-B7](/quality/audits/2026-07-26-true-ha-adversarial-review.md)). The "zero-gap node kill"
guarantee therefore holds for the cold tier only; **continuous hot-tail shipping** (streaming the
hot window's segments/WAL to object storage so a replica can pick up the tail) is the missing piece
and is future work. Hash-shard (ordinal) replica serving likewise follows later, via the pool hash
path.
