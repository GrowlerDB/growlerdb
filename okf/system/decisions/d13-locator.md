---
type: Decision
title: D13. Locator vs PK-clustering
description: Use a locator by default; prefer Iceberg pruning when the source is primary-key-clustered.
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D13. Locator vs PK-clustering

**Decision.** Use a locator by default; prefer Iceberg pruning when the source is primary-key-clustered.

**Status.** **Superseded by [D54](/system/decisions/d54-store-less-hydration.md).** The locator
default is removed entirely: GrowlerDB stores no per-row location and hydration is only the
store-less pruned key scan. What survives from this ADR is its pruning preference, now the *sole*
path rather than the PK-clustered special case.
