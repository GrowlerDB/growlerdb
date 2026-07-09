---
type: Feature
title: Suggest (autocomplete)
description: Type-ahead value suggestions for the field:prefix token being typed.
tags: [feature, search, suggest, autocomplete]
timestamp: 2026-07-04T14:22:00
---

# Suggest (autocomplete)

Type-ahead completion — as a user types a `field:prefix` token, `POST /v1/suggest` (gRPC `Suggest`)
returns matching values so they can complete the query without knowing exact terms.

## Behavior

- Suggests values for the field token under the cursor; the console wires it into the search box.
- In a windowed index, suggest fans out across all windows (no time pruning) to stay complete.

## Notes

Backed by the indexed terms of the target field. See [query](/product/functional/search/query.md) for
the syntax the suggestions complete.
