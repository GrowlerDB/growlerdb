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

## Consistency invariants

- **The manifest is the commit point** — written last on every backup, and every mutation keeps
  the durable manifest consistent with the object set at every crash point. In particular, cold
  [bundling](/system/storage/cold-bundles.md) commits the `bundled` manifest **before** deleting
  the per-file objects it supersedes, so a crash never leaves a manifest naming deleted objects
  (a `restore` gets the clean `Bundled` refusal, never a mid-download 404).
- **Replica refresh is torn-proof against concurrent backups.** A refresh pass fetches the
  mutable objects (`index/meta.json`, `aux.redb`, `location.arr`) live while segment files come
  from the manifest's list, so a primary backup landing mid-pass could pair a newer meta with an
  older segment set. After each pass the replica re-reads the manifest and retries (bounded)
  whenever the snapshot advanced during the pass; persistent contention surfaces as a transient
  `RefreshContention` the poll loop simply retries.
- **Cold-park verifies its snapshot post-swap.** A write committing between a window's backup
  and its cold swap would advance the kept `aux.redb` checkpoint past the served cold copy —
  silent loss. The park pass compares the live snapshot to the backed-up one *after* the swap
  (when the window is read-only, so the check can't itself race); on a mismatch it swaps the
  intact hot shard back and re-parks next tick.

## Notes

Because the manifest carries snapshot + checkpoint, a restored index resumes ingestion exactly-once.
Implemented in `growlerdb-backup`.
