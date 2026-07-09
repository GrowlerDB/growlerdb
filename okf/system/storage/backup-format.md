---
type: Concept
title: Backup format
description: What an index backup contains, and how restore / replica segment-shipping use it.
tags: [system, storage, backup, replica]
resource: /crates/growlerdb-backup
timestamp: 2026-07-04T14:22:00
---

# Backup format

A [backup](/product/functional/index-management/backup-restore.md) uploads an index's segment files
plus a manifest recording the **snapshot**, ingestion **checkpoint**, and index **definition** to
object storage, durably (temp + fsync + rename; transient object-store errors retried).

## Format version

The manifest carries a **format version** (`format`; manifests without the field deserialize
as 1). Every manifest consumer (restore, revive, replica refresh, status) refuses a format
**newer** than the binary supports with a clear `UnsupportedFormat` error telling the operator to
use a matching GrowlerDB version — old binaries fail loudly instead of mis-restoring.

- **Format 1** — the current (and only) format: a
  [D30 layered-locator](/system/storage/locators-segments.md) shard, whose file list carries the
  segments, `location.arr`, and `aux.redb`. (GrowlerDB shipped unreleased through the D30 work, so
  format 1 was reset to mean the layered format — no earlier format exists in the wild.)
- A future incompatible layout bumps the version; the refuse-newer check is the hygiene that makes
  that bump safe.

## Uses

- **Restore** — a lost node/shard reopens from the backup (`refresh_and_reopen`); with no backup it
  rebuilds from Iceberg.
- **Replica segment shipping** — a [replica](/product/functional/replicas.md) advances by pulling
  shipped segments from the same store and hot-swapping on a snapshot advance.

## Notes

Because the manifest carries snapshot + checkpoint, a restored index resumes ingestion exactly-once.
Implemented in `growlerdb-backup`.
