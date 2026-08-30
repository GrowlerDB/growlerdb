---
type: Decision
title: 'D30. Layered locator — identity / reference / location'
description: The layered locator (a locator-ID fast field + a dense location array) with per-index location strategies. Superseded by D54 — the stored locator and its compaction re-map are removed; store-less predicate hydration is the only path.
tags: [decision, adr, hydration, storage]
timestamp: 2026-07-04T14:22:00
---

# D30. Layered locator — identity / reference / location

**Status.** **Superseded by [D54](/system/decisions/d54-store-less-hydration.md).**

This ADR split hydration's stored locator into three layers by mutability — identity (key terms),
reference (an internal `_locid` fast field), and location (a dense `location.arr` array patched in
place) — with a per-index `location_strategy` (`coordinates` default | `predicate` | future
`row_id`) and a background compaction **re-map** + live-file bitmap to heal locators after
`rewrite_data_files`.

The whole apparatus is **removed**. Two forces killed it: an Iceberg compaction stales *every*
stored locator at once, and the O(table) re-map that heals them scales with the source, not the
query load, and never demonstrably converged. Store-less **predicate** hydration — a key-equality
scan pruned by the row's own stored partition/sort-key stats, byte-budget-bounded and key-verified —
has nothing to stale and nothing to heal, and it drops the O(rows) hot location array too. It is now
the **only** hydration path. See [D54](/system/decisions/d54-store-less-hydration.md) and the
[segments & aux store](/system/storage/locators-segments.md) doc.
