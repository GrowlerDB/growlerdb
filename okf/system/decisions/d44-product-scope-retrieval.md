---
type: Decision
title: 'D44. Product scope: full-text, vector & hybrid retrieval over your data'
description: GrowlerDB is a retrieval engine — full-text, vector, and hybrid search over your data, keyed back to an authoritative source. Supersedes D24 (pure text search over Iceberg); the source-agnostic thesis and vector capability were already implicit in D42/D41.
tags: [decision, adr, positioning, scope]
timestamp: 2026-07-19T00:00:00
---

# D44. Product scope: full-text, vector & hybrid retrieval over your data

**Decision.** GrowlerDB is a **retrieval engine** — **full-text, vector, and hybrid search over your
data** — that keeps a fast, derived index and resolves matches back to the authoritative row in the
underlying source. This **supersedes [D24](/system/decisions/d24-product-scope.md)** ("a pure text
search engine over Iceberg"), which was written before the vector/hybrid capability shipped
([D19](/system/decisions/d19-ann-library.md)–[D21](/system/decisions/d21-reranker.md),
[D41](/system/decisions/d41-vector-open-core.md), [D42](/system/decisions/d42-retrieval-first.md)) and
narrowed the product to one modality and one source.

## The positioning

- **Headline:** *Full-text, vector, and hybrid search over your data.*
- **Sub-line:** *GrowlerDB keeps a fast, derived index over your lakehouse and resolves matches back
  to the authoritative row.*

Three pillars carry the copy:

1. **Full-text + vector + hybrid** retrieval — plus reranking and a read-only
   [MCP server](/product/interfaces/mcp-server.md) for AI agents. *Shipped today.*
2. **No second copy.** The index — inverted terms, per-doc vectors, and any
   [cached fields](/system/decisions/d23-cached-field-policy.md) — is **derived** from your source,
   which stays authoritative; matches resolve back to the live row via
   [hydration](/product/functional/hydration.md).
3. **Any source, any catalog.** Apache Iceberg via *any* Iceberg REST catalog today; the
   [SourceConnector seam](/system/decisions/d37-extension-seams.md) is built to take more —
   Delta Lake read ([a stretch item](/quality/known-limitations/index.md)), then CDC/Debezium and
   Kafka — growing toward **federated retrieval across lakehouse and operational data**.

## What changed vs. D24

- **Modality:** "pure *text* search" → **full-text, vector, and hybrid retrieval**. Text is one
  modality, no longer the whole product.
- **Source:** "over *Iceberg*" → **over your data**. Iceberg stays the flagship (and keeps the name
  story — a *growler* is a berg calved off an iceberg), but the differentiator — a derived index that
  hydrates back to an authoritative source with no second copy — is **source-agnostic**, and other
  sources are on the roadmap.

## What did not change (non-goals retained)

- **Not a system of record / datastore.** The source owns the truth; the index holds only what it
  needs to search and to point back.
- **Not an analytics / OLAP engine**, and **not detection / alerting** — that is the app layer above
  GrowlerDB.
- **Not a RAG framework and no LLM generation** — [D42](/system/decisions/d42-retrieval-first.md)
  still governs the retrieval-first boundary; GrowlerDB never calls out to an LLM.

## Why

The product surface already outgrew "pure text search over Iceberg": vector/hybrid retrieval, rerank,
and the MCP server ship in the AGPL core today, and the value proposition (a derived index that keeps
no second source of truth) never depended on the modality or on Iceberg specifically. Aligning the
headline with what the engine actually does — and with the accepted retrieval-first thesis in D42 —
removes an internal contradiction and states the broader, still-honest scope. Maturity gating keeps it
truthful: full-text/vector/hybrid are claimed as shipped; additional sources are named as roadmap, not
present capability.

**Status.** Accepted. **Supersedes [D24](/system/decisions/d24-product-scope.md).** Builds on
[D42](/system/decisions/d42-retrieval-first.md) (retrieval-first) and
[D41](/system/decisions/d41-vector-open-core.md) (vector is open-core).
