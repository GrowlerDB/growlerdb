---
type: Decision
title: 'D42. Retrieval-first, not a RAG framework'
description: GrowlerDB is the governed retrieval layer; generation belongs to the app/agent. Chunking is supported at ingest (simple, configurable); LLM generation exists only as a demo/playground convenience.
tags: [decision, adr]
timestamp: 2026-07-18T00:00:00
---

# D42. Retrieval-first, not a RAG framework

**Decision.** GrowlerDB is the **governed retrieval layer** for AI over the lakehouse — it owns
hybrid lexical + semantic recall, freshness, tenant-scoped governance, and coordinates that
[hydrate](/product/functional/hydration.md) authoritative Iceberg rows. It is **not** a RAG framework:
prompt orchestration, agent loops, and the LLM answer belong to the app / agent / framework
(LangChain, LlamaIndex, a custom agent, an [MCP](/product/functional/search/index.md) client).

The product surface is a great **retrieval API + an MCP server**. Two deliberate boundaries:

- **Chunking is supported at ingest, kept simple.** RAG needs chunked text, and chunking is
  *indexing*, not *generation*: the connector / build can split a text field into chunk-documents that
  share a parent key, with a simple, configurable policy. Sophisticated / semantic chunking stays in
  the caller's pipeline.
- **Generation is a demo-only convenience.** A thin RAG *playground* (retrieve → optionally call a
  configured LLM → show a cited answer) is worth shipping as a showcase in the console/demo, but it is
  explicitly **not** the product surface. GrowlerDB configures no LLM by default and ships none.

**Why.** The one thing only a lakehouse-native engine can do is *governed, fresh retrieval keyed to
authoritative data with no second copy of the truth*. Competing with RAG frameworks on orchestration
would dilute that and duplicate a crowded, fast-moving layer. Retrieval-first keeps GrowlerDB
composable underneath whatever generation stack a team already uses.

This decision governs scope; the licensing of the capability is
[D41](/system/decisions/d41-vector-open-core.md), and the retrieval mechanics are
[D19](/system/decisions/d19-ann-library.md) / [D20](/system/decisions/d20-embedding-model.md) /
[D21](/system/decisions/d21-reranker.md).

**Status.** Accepted.
