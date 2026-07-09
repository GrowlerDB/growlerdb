---
type: Requirement
title: Durability & recovery
description: No data loss; recovery bounded by rebuild time, never by data loss.
tags: [nfr, durability, recovery, rpo, rto]
timestamp: 2026-07-04T14:22:00
---

# Durability & recovery

- **No data loss** — Iceberg is the system of record; the index is derived and rebuildable.
- **RPO ≈ last checkpoint** (seconds) for acknowledged writes — RPO = 0 for committed data, via
  [exactly-once checkpoints](/product/functional/ingestion/checkpoints-exactly-once.md).
- **RTO = shard restore in minutes** — restore from the object-storage
  [backup](/product/functional/index-management/backup-restore.md), or rebuild from Iceberg if there
  is none. Disaster recovery is bounded by rebuild time, never by data loss.

**Status.** v1 **design target**; recovery is exercised under
[reliability/chaos](/quality/reliability.md).
