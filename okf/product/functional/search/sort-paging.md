---
type: Feature
title: Sorting, paging & collapsing
description: Sort by relevance or fast fields, page by offset or keyset cursor, collapse to top-per-value, and pin a point-in-time.
tags: [feature, search, sort, pagination]
timestamp: 2026-07-04T14:22:00
---

# Sorting, paging & collapsing

Controls on [`/v1/search`](/product/interfaces/rest.md) beyond the query itself.

- **Sort** — by relevance (`_score` desc, the default) or by numeric/date/KEYWORD **fast fields**.
  `_score` may sort alone or as a tiebreaker among fields. A composite-key tiebreaker is applied
  implicitly for a deterministic total order. Sorting on a non-fast (or text) field is rejected —
  `index:describe` publishes the index's **`sort_fields`** (the valid set) so a client like the
  [console](/product/interfaces/ui.md) only ever offers a sortable field, never one the engine 400s on.
- **Offset paging** — `offset` + `limit` (`from`/`size`), bounded by a page-fetch ceiling. Required
  for a `_score` sort (a score isn't a stable keyset key).
- **Keyset paging** — `search_after` with the opaque `next_cursor` from the prior response: stable
  deep pagination; requires a sort over fast fields.
- **Collapse** — collapse to the top hit per distinct value of a fast field, with a per-hit group
  count.
- **Point-in-time (`pit_id`)** — read against a frozen snapshot for a consistent scroll/export.

## Notes

The composite key as the stable tiebreaker is what makes deep paging deterministic — central to the
[search-backed app](/product/use-cases/search-backed-app.md) use case.
