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
attach here). The embedding is stored per document; the **ANN index**
([D19](/system/decisions/d19-ann-library.md)), **RRF fusion + filtered KNN**, and the optional
[reranker](/system/decisions/d21-reranker.md) are the retrieval half.

The default local embedder is **bge-small-en-v1.5** run **in-process on [Candle](https://github.com/huggingface/candle)**
— pure Rust, no native/C dependency, no network. The model (`config.json`, `tokenizer.json`,
`model.safetensors`) is provisioned out of band into `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model-id>/`;
when it isn't present, embedding transparently falls back to a deterministic dev embedder (so ingest and
offline CI keep working) with a one-time warning. The BGE runtime is behind a default-on build feature, so
a slim build can drop the ML dependency entirely. Automatic model download is intentionally not part of the
runtime — provisioning stays explicit, keeping the default deployment offline.

> **Status.** In active build (M5). The **field type, local BGE embedding at ingest (Candle), the
> `Embedder` seam, and per-document vector storage** are shipped. Query-time semantic / hybrid KNN,
> fusion, and reranking are the retrieval half — see
> [known limitations](/quality/known-limitations/index.md). The interface and stored format are stable.
