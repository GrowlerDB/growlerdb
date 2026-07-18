---
type: Decision
title: D20. Default embedding model
description: Configurable, provider-agnostic BGE-family embedding models for the deferred vector capability.
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D20. Default embedding model

**Decision.** Configurable, provider-agnostic embedding, **local by default**: BGE-family models
(BGE-small-en-v1.5, 384-dim; bge-m3 multilingual) embedded in-process via ONNX / `fastembed` — no
external dependency and **no data egress**. External embedding APIs (Voyage, Cohere, OpenAI, a
self-hosted server) attach via the `Embedder` seam and are strictly **opt-in**. The embedding config
(model id, dimensions, provider) is recorded in the index metadata so vector search is reproducible and
a model change is a tracked re-embedding reindex.

**Status.** Accepted; in active build (M5). Local-default embedding is what lets the vector capability
be **open** with zero required keys ([D41](/system/decisions/d41-vector-open-core.md)); provider keys
are server-side only.
