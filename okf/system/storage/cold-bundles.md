---
type: Concept
title: Cold bundles
description: The split-bundle format for cold windows served read-through from object storage.
tags: [system, storage, cold-tier, bundle]
timestamp: 2026-07-04T14:22:00
---

# Cold bundles

The on-object-storage format for a parked [cold window](/product/functional/cold-tiering.md). When a
window is parked, its local segment files are streamed into a **split bundle** (a concatenated object
plus a manifest of file offsets) — built from the **local** files still on disk at park time (no
re-download), one file in RAM at a time (multipart).

## Read-through

A byte-bounded **range cache** serves reads directly from the bundle in object storage, so a cold
window stays queryable (just slower); a small **hotcache** keeps a window's structural bytes warm.
Versioned sidecars + pinned hotcache ranges keep it crash-consistent and cache-efficient.

## Notes

Read-through (vs park-and-evict) was chosen so cold data never becomes unqueryable. Cold-cache hit
rate is an [SLI](/system/observability.md). In `growlerdb-index`.
