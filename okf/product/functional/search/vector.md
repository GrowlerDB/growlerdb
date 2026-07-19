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
queries (zero lexical term overlap), hybrid strictly beats lexical-only. The optional
[reranker](/system/decisions/d21-reranker.md) is the remaining refinement.

**Tenant safety.** The mandatory, non-widenable [`tenant = <claim>` filter](/product/functional/rbac-and-tenancy.md)
is now enforced on the vector path too: the tenant `Term` rides **inside** the KNN as a filter (the
lexical arm gets the usual `and_filter`), so neighbors are intersected with the caller's tenant docs and
cannot cross tenants. A tenant-scoped index with **no** verified claim still **fails closed** (refuses)
rather than returning nearest neighbors unscoped. Semantic / hybrid search remain on the native engine
API for now — exposing them on the authenticated gateway is a later surface, but the tenant enforcement
that makes that safe is in place.

> **Status.** In active build (M5). Shipped: the **field type, local BGE embedding at ingest (Candle),
> the `Embedder` seam, per-document vector storage, the per-segment ANN sidecar (backed up), top-level
> KNN, filtered / tenant-scoped KNN, and RRF hybrid fusion**. The optional **reranker** follows — see
> [known limitations](/quality/known-limitations/index.md). The interface and stored format are stable.
