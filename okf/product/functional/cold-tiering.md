---
type: Feature
title: Cold tiering
description: Serve old windows directly from object storage via a read-through cache — cheap long retention.
tags: [feature, cold-tier, storage]
timestamp: 2026-07-04T14:22:00
---

# Cold tiering

Old [windows](/product/functional/windowing-time.md) can be served **directly from object storage**
instead of local disk — so long retention is cheap without giving up searchability.

## Behavior

- A cold window is read through a byte-bounded range-cached object directory: **queries always
  complete**, cold is just slower, and window pruning means most queries never touch cold at all.
- A small hot cache keeps a cold window's structural bytes warm; a cold-cache hit-rate SLI is exposed.
- `/v1/cold` + the console Storage-tiers panel show what's parked.
- **Automatic, in both directions.** Each node parks its own windows past the `hot_windows` policy on
  a background timer, and a cold window that gets hot traffic again **pre-warms** itself back to local
  NVMe. Parking is node-local (the data lives on the node's own volume), opt-in per deployment, and
  never interrupts serving — the window keeps answering queries across the hot→cold swap. Backup +
  restore stay available via the `growlerdb` CLI regardless. See
  [D39](/system/decisions/d39-automatic-cold-tiering.md).

## Notes

Chosen as read-through (vs park-and-evict) so cold data stays queryable. Bundle/format details:
[system/storage](/system/storage/cold-bundles.md).
