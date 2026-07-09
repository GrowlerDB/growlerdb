---
type: Decision
title: D13. Locator vs PK-clustering
description: Use a locator by default; prefer Iceberg pruning when the source is primary-key-clustered.
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D13. Locator vs PK-clustering

**Decision.** Use a locator by default; prefer Iceberg pruning when the source is primary-key-clustered.

**Status.** Accepted; refined by [D30](/system/decisions/d30-layered-locator.md) — the pruning
preference becomes D30's store-less `predicate` location strategy, selectable per index.
