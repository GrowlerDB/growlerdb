---
type: Feature
title: Highlighting
description: Query-term-highlighted snippets rendered in the results list.
tags: [feature, search, highlight]
timestamp: 2026-07-04T14:22:00
---

# Highlighting

Renders **query-term-highlighted snippets** in search results so a user sees *why* a hit matched. The
console highlights matched terms in each result row's snippet (picked from a named or the longest text
field among the hit's cached fields).

## Notes

Highlighting works from the query terms + the hit's cached text fields — no extra round trip. Terms
are derived from the parsed [query](/product/functional/search/query.md).
