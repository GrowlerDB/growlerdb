---
type: Feature
title: Backup & restore
description: Durably back an index up to object storage and restore/rebuild it; recovery is bounded by rebuild time, never data loss.
tags: [feature, index, backup, restore, durability]
timestamp: 2026-07-04T14:22:00
---

# Backup & restore

Back an index up to object storage and restore it. `POST /v1/index:backup` (+ `:backup-status`) uploads
the index's segments, snapshot/checkpoint, and definition.

## Behavior

- **Backup** the served index to a configured object-storage target; status reports the last snapshot
  and file count. Transient object-store errors are retried.
- **Restore / rebuild**: a lost node or shard restores from the object-storage backup; with no backup,
  it **rebuilds from Iceberg**. Because the index is derived, disaster recovery is bounded by rebuild
  time, never by data loss.

## Notes

A node without a backup target reports `configured: false` (rendered "Off"). Backup format and
restore mechanics: [system/storage](/system/storage/backup-format.md).
