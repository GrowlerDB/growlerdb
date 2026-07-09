---
type: Feature
title: Aliases & ILM
description: Named pointers to one or more indexes enabling atomic reindex-and-swap and lifecycle retention.
tags: [feature, index, alias, ilm]
timestamp: 2026-07-04T14:22:00
---

# Aliases & ILM

An **alias** is a named pointer to one or more indexes. Clients query the alias; operators re-point it
atomically. `/v1/aliases` (list / point / re-point / drop).

## Behavior

- **Atomic reindex-and-swap** — build a new index, then re-point the alias in one durable write: the
  zero-downtime cutover story (with [reindex](/product/functional/index-management/reindex.md)).
- **Search-and-merge** across an alias's member indexes.
- **Index lifecycle management** — glob patterns (`events-*`) and keep-last-N retention over a set of
  time-rolled indexes.

## Notes

Aliases never dangle: [dropping](/product/functional/index-management/drop.md) a member prunes it from
the alias. The console exposes an Aliases panel.
