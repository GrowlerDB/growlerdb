---
type: Concept
title: Quality overview
description: How GrowlerDB maintains quality — the correctness guarantees and the methods that uphold them.
tags: [quality, overview, guarantees]
timestamp: 2026-07-04T14:22:00
---

# Quality overview

How GrowlerDB stays trustworthy — the guarantees it makes and the methods that keep them true. This
area is **process knowledge**: an agent should learn *how we work to keep quality*, not just that
quality exists.

## Correctness guarantees

- **Exactly-once ingestion** — no committed-data loss (RPO = 0 for acknowledged writes) and no
  duplicates across restarts ([checkpoints](/product/functional/ingestion/checkpoints-exactly-once.md)).
- **Crash consistency** — the durable Tantivy commit lands before the redb locator checkpoint, so a
  crash never leaves a half-committed or corrupt index
  ([locators & segments](/system/storage/locators-segments.md)).
- **No silent under-count** — a true cross-shard `total`, an honest `partial` flag when a shard is
  down, and dedup on cross-shard merge.
- **No stale/orphaned reads** — the source-lineage guard serves degraded on a recreated source rather
  than returning ghost rows.

## Methods

[Tests](/quality/tests/index.md) · [scalability](/quality/scalability.md) ·
[reliability](/quality/reliability.md) · [security](/quality/security/index.md) ·
[CI & gates](/quality/ci-and-gates.md) · [release readiness](/quality/release-readiness.md) ·
[how issues are handled](/quality/issues.md) · [known limitations](/quality/known-limitations/index.md).
