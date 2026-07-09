---
type: Feature
title: Drop index
description: Remove an index; the source Iceberg data is untouched.
tags: [feature, index, drop]
timestamp: 2026-07-04T14:22:00
---

# Drop index

Delete an index and its served state. Because the index is a **derived, rebuildable** view, dropping
it never touches the authoritative Iceberg data — it can be recreated by
[building](/product/functional/index-management/create.md) again from the source.

## Notes

Drop also prunes the index from any [aliases](/product/functional/index-management/aliases-ilm.md) that
point at it, so an alias never dangles. Registered/served state is removed from the
[control plane](/system/runtime/components/control-plane.md).
