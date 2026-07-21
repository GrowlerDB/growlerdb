---
type: Decision
title: D20. Default embedding model
description: Configurable, provider-agnostic BGE-family embedding models (local-default, keyless) for the shipped vector capability.
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

**Status.** Accepted; **shipped**. The local-default embedder runs **bge-small-en-v1.5 on ONNX
Runtime** (the `ort` crate, int8-quantized) — realigning with this decision's ONNX intent after an
initial pure-Rust Candle implementation proved too slow (~10 docs/s on a laptop). ONNX Runtime links a
**native `libonnxruntime`** (fetched at build time; offline at runtime) for **~an order of magnitude**
more CPU throughput, which is what makes local embed-at-ingest viable at demo/low-volume scale (the
high-throughput paths are SOURCE/EXTERNAL — [D46](/system/decisions/d46-embed-write-path-stage.md)). The
cross-encoder **reranker** ([D21](/system/decisions/d21-reranker.md)) still runs on Candle pending its own
ONNX move. Local-default embedding is what lets the vector capability be **open** with zero required keys
([D41](/system/decisions/d41-vector-open-core.md)). The opt-in **external** path is implemented: a
`provider: EXTERNAL` field calls a hosted service over HTTP with a **server-side-only** key
(`GROWLERDB_EMBEDDING_API_KEY`, redacted, rotatable, never browser-exposed), failing closed without one —
provider keys are never client-side.
