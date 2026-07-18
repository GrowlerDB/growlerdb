---
type: Decision
title: D21. Reranker
description: A pluggable reranker hook, off by default, for the deferred vector capability.
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D21. Reranker

**Decision.** A pluggable `Reranker` hook that reorders the fused top-K for relevance-critical cases —
**off by default**, opt-in per query/index. Suggested local model `bge-reranker-base`, or a provider API
via the seam. It sits **outside** the index and never changes what is stored.

**Status.** Accepted; in active build (M5). Ships **open**
([D41](/system/decisions/d41-vector-open-core.md)) as the optional final stage of retrieval-first
([D42](/system/decisions/d42-retrieval-first.md)); provider reranker keys are server-side only.
