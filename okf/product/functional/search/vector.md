---
type: Feature
title: Vector fields & embeddings
description: Declare a VECTOR field over a text column; GrowlerDB embeds it (local by default) and stores the vector per document for semantic / hybrid retrieval.
tags: [feature, search, vector, embedding, rag]
timestamp: 2026-07-18T00:00:00
---

# Vector fields & embeddings

A **VECTOR field** gives a document a dense embedding alongside its lexical fields, so the same index
serves both **lexical (BM25)** and **semantic** retrieval, fused into one ranking that returns the
[same coordinates](/product/functional/hydration.md) resolving back to governed Iceberg rows. This is the
foundation of GrowlerDB as the [retrieval layer for RAG](/product/use-cases/rag.md) —
**retrieval-first**, [open-core](/system/decisions/d41-vector-open-core.md).

## Declaring a vector field

A vector field is declared explicitly (never auto-derived) with a `vector` config on the field mapping:

```yaml
mapping:
  fields:
    - path: body
      type: TEXT
      analyzer: english
    - path: body_vec
      type: VECTOR
      vector:
        source_field: body        # the text field to embed
        model: bge-small-en-v1.5  # default; recorded for reproducibility
        dims: 384                 # default for the model
        metric: COSINE            # COSINE (default) | DOT | L2
        provider: LOCAL           # LOCAL (default, in-process) | EXTERNAL (opt-in, later)
```

- **Opt-in, always** — a field is only a vector if the author declares it. The majority of indexes carry
  no vectors and pay nothing.
- **Local by default** ([D20](/system/decisions/d20-embedding-model.md)) — embeddings are generated
  in-process from an open model; **no data leaves** the deployment and **no API key** is required. The
  `provider` seam allows an external embedding service as a conscious opt-in.
- **Reproducible** — `model`, `dims`, `metric`, and `provider` are recorded in the
  [index metadata](/system/storage/data-model.md); changing the model is a tracked re-embedding reindex.
- A vector field takes no analyzer, and is never `fast`, `cached`, or inverted-indexed — it stores only
  its embedding.

## How the embedding is produced

At **ingest**, for each `LOCAL` vector field GrowlerDB embeds the `source_field` text of every document
through the [`Embedder`](/system/decisions/d20-embedding-model.md) seam and stores the resulting vector
in the document's segment (backed up and restored with the lexical segment). The `Embedder` trait is the
integration point ([D41](/system/decisions/d41-vector-open-core.md) keeps it open; external providers
attach here).

The default local embedder is **bge-small-en-v1.5** run **in-process on [Candle](https://github.com/huggingface/candle)**
— pure Rust, no native/C dependency, no network. The model (`config.json`, `tokenizer.json`,
`model.safetensors`) is provisioned out of band into `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model-id>/`;
when it isn't present, embedding transparently falls back to a deterministic dev embedder (so ingest and
offline CI keep working) with a one-time warning. The BGE runtime is behind a default-on build feature, so
a slim build can drop the ML dependency entirely. Automatic model download is intentionally not part of the
runtime — provisioning stays explicit, keeping the default deployment offline.

## Semantic (KNN) retrieval

Each segment's vectors are indexed into a GrowlerDB-owned **ANN sidecar**
([D19](/system/decisions/d19-ann-library.md)) — one `<segment>.ann` beside the Tantivy segment, built
after commit and each compaction, and backed up / restored with the lexical segment. A **top-level KNN**
query embeds the query text through the same `Embedder` used at ingest, finds the nearest vectors per
segment, keeps only live docs, and resolves each to its composite **coordinate** — exactly like a lexical
hit, so [hydration](/product/functional/hydration.md) is unchanged.

**Filtered KNN.** A KNN query carries an optional **filter** — a lexical / fast-field sub-query whose
matching documents constrain the candidate set (the nearest vectors *where* `lang = en`, a numeric
range, etc.). The filter's per-segment doc set is intersected with the neighbors, so semantic retrieval
still respects metadata.

**Hybrid search (RRF).** A hybrid query runs both modalities — lexical **BM25** and vector **KNN** — and
fuses their rankings with **Reciprocal Rank Fusion** (`RRF_K = 60`) into one ranked list of coordinates.
This is where semantic recall complements exact-term precision: on a real-model eval over paraphrase
queries (zero lexical term overlap), hybrid strictly beats lexical-only.

**Reranking (opt-in).** A semantic/hybrid query may set `rerank` to reorder its retrieved top-K by a
cross-encoder relevance pass over `(query, passage)` ([D21](/system/decisions/d21-reranker.md), local
**bge-reranker-base** on Candle). It **sits outside the index** — a post-retrieval reorder — and is
**off by default** (retrieval-first); the local model is keyless, falling back to a deterministic dev
reranker when unprovisioned.

**On the authenticated gateway.** Semantic and hybrid search are exposed on the multi-shard gateway
(gRPC `SemanticSearch` + REST `/v1/search:semantic` and `/v1/search:hybrid`), not just the embedded
engine. The **query is embedded on each node**, not the gateway ([D43](/system/decisions/d43-node-local-query-embedding.md)):
a node already holds the embedding model and the field's config for ingest, and embedding is
deterministic, so per-shard embedding yields the same vector — and the gateway stays free of the ML
dependency. The gateway is pure orchestration: it scatters the query, gathers each shard's top-K,
merges by score (semantic), and — for hybrid — also runs the lexical fan-out and **RRF-fuses** the two
merged lists.

**Tenant safety.** The mandatory, non-widenable [`tenant = <claim>` filter](/product/functional/rbac-and-tenancy.md)
is enforced on the vector path too, **at the node**: the verified tenant `Term` rides **inside** the KNN
as a filter (the lexical arm gets the usual `and_filter`), so neighbors are intersected with the caller's
tenant docs and cannot cross tenants — a query-supplied filter cannot widen past the claim. A
tenant-scoped index with **no** verified claim **fails closed** (refuses) rather than returning nearest
neighbors unscoped. (Tenancy stays opt-in — a single-tenant index carries no `tenant_field` and this is
a no-op; see [RBAC & tenancy](/product/functional/rbac-and-tenancy.md).)

> **Status.** In active build (M5). Shipped: the **field type, local BGE embedding at ingest (Candle),
> the `Embedder` seam, per-document vector storage, the per-segment ANN sidecar (backed up), top-level
> KNN, filtered / tenant-scoped KNN, RRF hybrid fusion, the authenticated multi-shard gateway
> surface** (gRPC + REST, node-local embedding), the **console** search-mode UX + grounded Ask screen,
> and the **opt-in reranker**. **Distributed windowed** semantic search and the approximate (HNSW) index
> (a scale optimization) follow — see
> [known limitations](/quality/known-limitations/index.md). The interface and stored format are stable.
