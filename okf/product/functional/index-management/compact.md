---
type: Feature
title: Compact
description: Merge segments to keep query performance and disk in check; can run automatically.
tags: [feature, index, compact, maintenance]
timestamp: 2026-07-04T14:22:00
---

# Compact

Merge an index's [segments](/glossary.md) to bound segment count (query cost) and reclaim space from
deleted/updated documents. `POST /v1/index:compact` returns the before/after segment counts.

## Behavior

- Manual compaction on demand, or **health-driven auto-compaction**: a background loop compacts when a
  compaction-health policy trips, on both the single-shard and per-hot-window serving paths
  (`--compact-interval-secs`). Replicas never compact; cold read-through windows have no writer.
- **Cold GC is snapshot-gated.** Compaction fuses segments into new, differently-named objects, so the
  superseded objects in object storage must eventually be reclaimed — but a
  [read-through replica](/product/functional/replicas.md) ([D53](/system/decisions/d53-unit-replication.md))
  opens at a published layout and only reopens when the **source snapshot** advances. Because
  compaction re-lays-out segments *within* one source snapshot, the backup's GC **retains** superseded
  objects until the snapshot advances (and then keeps the previous generation one more cycle), so a
  reader pinned to the old layout never 404s on a lazily-fetched segment. Reclamation is therefore
  bounded, not immediate: same-snapshot compaction garbage clears on the next source commit.

## Notes

Segment merging is the [maintenance](/system/storage/index-store.md) mechanism; the product surface is
the trigger + the auto-compaction policy.
