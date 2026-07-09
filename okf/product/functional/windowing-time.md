---
type: Feature
title: Windowing & time
description: Partition an index by ingest time so old windows are immutable and query-prunable.
tags: [feature, windowing, time]
timestamp: 2026-07-04T14:22:00
---

# Windowing & time

For append-mostly time-series data, an index can be **partitioned into time windows** by ingest time.
Old windows become immutable (and [parkable](/product/functional/cold-tiering.md)); late events land
in the current window.

## Behavior

- Declare a **timestamp field** (a DATE column, canonical epoch micros) and a window granularity.
- Event-time queries are **pruned** by per-window event-time zone-maps — a time-bounded query skips
  windows it can prove won't match, so most of the corpus is never scanned.
- Distributed per-window routing selects/aggregates across windows.

## Notes

Windowing is append-mostly only; mutating [CDC](/product/functional/ingestion/cdc.md) tables stay
hash-sharded + hot. Window mechanics: [system/storage](/system/storage/index-store.md).
