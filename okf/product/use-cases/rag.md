---
type: Use Case
title: RAG retrieval over governed enterprise data
description: Hybrid lexical + semantic retrieval that returns governed, fresh chunks keyed into Iceberg.
tags: [use-case, rag, retrieval, vector]
timestamp: 2026-07-04T14:22:00
---

# RAG retrieval over governed enterprise data

**Persona.** An [application developer](/product/actors/app-developer.md) building an internal copilot
or customer-facing assistant.

**Context.** Enterprise knowledge — docs, tickets, wikis, chunked for retrieval — lives in Iceberg.
The assistant needs **hybrid lexical + semantic** retrieval that returns **governed** chunks, stays
**fresh**, and respects **per-user access**.

**How GrowlerDB is used.**

- Index the chunk table; store embeddings as a column alongside the text.
- Query runs **hybrid** lexical (BM25) + vector KNN, fused → top-K coordinates →
  [hydrate](/product/functional/hydration.md) chunk + metadata from Iceberg, governed so a user only
  retrieves what they may read.
- Fresh via [changelog ingestion](/product/functional/ingestion/cdc.md) — edits/deletes to source
  docs propagate.

**Why it fits.** Hybrid retrieval on **one governed store**; keys into authoritative data; freshness;
no separate vector store to sync; access control enforced at hydration.

> **Status.** The **vector / hybrid** capability (embeddings + KNN + fusion) is a **planned**
> capability, not yet shipped — see [known limitations](/quality/known-limitations/index.md). The
> lexical retrieval, governance, and freshness pieces are available today.

**Requirements exercised.** Hybrid search · vector + lexical · governed retrieval · low-latency top-K ·
freshness.
