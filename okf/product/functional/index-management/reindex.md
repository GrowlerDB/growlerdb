---
type: Feature
title: Reindex
description: Rebuild an index from its source, fenced and single-flight; pairs with alias-swap for zero downtime.
tags: [feature, index, reindex]
timestamp: 2026-07-04T14:22:00
---

# Reindex

Rebuild an index from its Iceberg source — after a definition change, a source recreation, or to move
to a new shard layout. `POST /v1/index:reindex` (CLI `reindex`).

## Behavior

- **Write-fenced + single-flight**: a reindex fences writes and rejects a concurrent reindex (412);
  no-source → 501; wrong-index → 404.
- For a resharded layout, a filtered reindex keeps only the shard's owned documents.
- Pair with an [alias swap](/product/functional/index-management/aliases-ilm.md) to reindex into a new
  index and cut over atomically — the zero-downtime story.

## Notes

Single-shard trigger over REST today (multi-shard → honest Unimplemented). The source-streaming read
path keeps memory bounded.
