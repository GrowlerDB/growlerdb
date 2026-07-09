---
type: Feature
title: Export
description: Export a result set to JSON or CSV from the console.
tags: [feature, search, export]
timestamp: 2026-07-04T14:22:00
---

# Export

Export the current result set to **JSON** or **CSV** from the [console](/product/interfaces/ui.md).
CSV output is guarded against formula injection.

## Notes

For a large, consistent export, pin a [point-in-time](/product/functional/search/sort-paging.md) and
page with the keyset cursor so the set doesn't shift mid-export.
