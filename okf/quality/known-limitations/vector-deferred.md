---
type: Concept
title: Vector / hybrid search not yet shipped
description: Embeddings, ANN/KNN, and reranking (the RAG hybrid-retrieval path) are decided and in active build (M5), not yet shipped.
tags: [quality]
timestamp: 2026-07-04T14:22:00
---

# Vector / hybrid search not yet shipped

Embeddings, ANN/KNN, and reranking (the RAG hybrid-retrieval path) are **in active build** (M5) — no
longer merely deferred, but not yet shipped. The capability is **open-core**
([D41](/system/decisions/d41-vector-open-core.md)) and **retrieval-first**
([D42](/system/decisions/d42-retrieval-first.md)): local-default embeddings
([D20](/system/decisions/d20-embedding-model.md)), Tantivy native KNN
([D19](/system/decisions/d19-ann-library.md)), RRF fusion + filtered KNN, and an off-by-default
reranker ([D21](/system/decisions/d21-reranker.md)). Lexical retrieval, governance, and freshness are
available today.
