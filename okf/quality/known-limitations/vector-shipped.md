---
type: Concept
title: Vector / hybrid search (shipped)
description: RESOLVED — embeddings, ANN/KNN, hybrid (RRF) fusion, and an optional reranker are shipped in the AGPL core; no longer a limitation.
tags: [quality]
timestamp: 2026-07-19T00:00:00
---

# Vector / hybrid search (shipped)

**RESOLVED.** Vector and hybrid retrieval are **shipped** in the AGPL core — no longer deferred.
Local-default embeddings ([D20](/system/decisions/d20-embedding-model.md) — Candle BGE, keyless),
per-segment ANN/KNN ([D19](/system/decisions/d19-ann-library.md)), RRF fusion + filtered KNN, and an
off-by-default reranker ([D21](/system/decisions/d21-reranker.md)), with node-local query embedding in
the distributed path ([D43](/system/decisions/d43-node-local-query-embedding.md)). The capability is
**open-core** ([D41](/system/decisions/d41-vector-open-core.md)) and **retrieval-first**
([D42](/system/decisions/d42-retrieval-first.md)) — see product scope
[D44](/system/decisions/d44-product-scope-retrieval.md). Optional **external** embedding/rerank
providers attach via a server-side-only key; the local defaults need none.
