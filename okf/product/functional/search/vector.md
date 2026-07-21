---
type: Feature
title: Vector fields & embeddings
description: Declare a VECTOR field over a text column; GrowlerDB embeds it (local by default) and stores the vector per document for semantic / hybrid retrieval.
tags: [feature, search, vector, embedding, rag]
timestamp: 2026-07-20T00:00:00
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
        provider: LOCAL           # provenance: LOCAL (default, in-process) | SOURCE (bring-your-own column) | EXTERNAL (opt-in)
```

- **Opt-in, always** — a field is only a vector if the author declares it. The majority of indexes carry
  no vectors and pay nothing.
- **Local by default** ([D20](/system/decisions/d20-embedding-model.md)) — embeddings are generated
  in-process from an open model; **no data leaves** the deployment and **no API key** is required. The
  `provider` seam allows an external embedding service as a conscious opt-in.
- **Provenance-typed** ([D46](/system/decisions/d46-embed-write-path-stage.md)) — where the vector
  comes from is a property of the field: **LOCAL** embeds in-process, **SOURCE** maps a vector column
  the source table already carries straight through (bring-your-own — zero embed cost, the
  high-throughput path), **EXTERNAL** calls a remote service. Same field, same query path.
- **Reproducible** — `model`, `dims`, `metric`, and `provider` are recorded in the
  [index metadata](/system/storage/data-model.md); changing the model is a tracked re-embedding reindex.
- A vector field takes no analyzer, and is never `fast`, `cached`, or inverted-indexed — it stores only
  its embedding.

## How the embedding is produced

Embedding is a **write-path stage** ([D46](/system/decisions/d46-embed-write-path-stage.md)): for each
`LOCAL` vector field GrowlerDB embeds the `source_field` text of every document through the
[`Embedder`](/system/decisions/d20-embedding-model.md) seam and stores the resulting vector in the
document's segment (backed up and restored with the lexical segment); a `SOURCE` field skips embedding
and maps its vector column through. The stage is shared by every source→index path — build, reindex,
sync, reconcile — so coverage is a property of the write pipeline rather than of one code path, and it
runs **pipelined and pooled** (embed overlaps read/write and fans across cores) so a vector field's
embedding is not a serial bottleneck on ingest throughput. Progress is a gauge emitted from the stage
(embedded / total), **decoupled from commit granularity** — the build is observable without shrinking
commit chunks. *(Direction: [D46](/system/decisions/d46-embed-write-path-stage.md). Today LOCAL
embedding runs only on the **cold build**; reindex / sync / reconcile do not yet re-embed — TASK-326 —
so a rebuilt or appended-to LOCAL vector index loses coverage until the shared stage lands. `SOURCE`
provenance and the pooled pipeline are the same work.)* The `Embedder` trait is the
integration point ([D41](/system/decisions/d41-vector-open-core.md) keeps it open; external providers
attach here). Forward passes are **bounded** (sub-batches of 32 inputs): attention memory scales with
`batch × seq²`, so an unbounded whole-table pass OOMs a node on real corpora (a 20k-abstract arXiv
build killed a 4 GB node at batch 400) — sub-batching caps peak memory regardless of build size, with
identical vectors (no cross-sequence attention). Inputs are **truncated to the model's sequence
window** (512 positions for BGE): an over-long text embeds its head rather than failing the forward
pass. A batch whose embed still fails is retried **per text**, skipping only true failures — and the
skip count is logged loudly per call. Both are load-bearing: pre-fix, one over-long abstract voided an
entire build chunk's vectors with a log-free skip, leaving 20k docs silently invisible to KNN
(TASK-323). Loaded models are **cached per model directory** for the process lifetime — the factory
runs on every semantic query, and per-call loading made each query re-read 133 MB of weights.

**Coverage is observable.** `describe_index` reports each vector field's `docs_with_vector` next to
`num_docs`: a shortfall means documents were ingested **without** an embedding and are invisible to
semantic search — a gap lexical search and `num_docs` both mask. Agents and the console read this
before trusting semantic/hybrid results ([D45](/system/decisions/d45-degraded-vs-error.md)).

The default local embedder is **bge-small-en-v1.5** run **in-process on [ONNX Runtime](https://onnxruntime.ai/)**
(via the `ort` crate, int8-quantized), CPU, no network at runtime ([D20](/system/decisions/d20-embedding-model.md)).
This links a **native `libonnxruntime`** (fetched at build time) — a deliberate trade of the former pure-Rust
Candle path for **~an order of magnitude** more CPU throughput, which is what makes local embed-at-ingest
viable on a laptop (the cross-encoder reranker still runs on Candle pending its own ONNX move). The model
(`config.json`, `tokenizer.json`, `model.onnx`) is provisioned out of band into
`${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model-id>/`; when it isn't present, embedding transparently
falls back to a deterministic dev embedder (so ingest and offline CI keep working) with a one-time warning. The
BGE runtime is behind a default-on build feature, so a slim build can drop the ML dependency entirely. Automatic
model download is intentionally not part of the runtime — provisioning stays explicit, keeping the default
deployment offline.

**External providers (opt-in).** A field declared `provider: EXTERNAL` (and, for reranking,
`GROWLERDB_RERANK_PROVIDER=external`) instead calls a hosted embedding / rerank service over HTTP. The
API key is **server-side only** (`GROWLERDB_EMBEDDING_API_KEY` / `GROWLERDB_RERANK_API_KEY`, from the
engine's env — a k8s Secret / Vault mount), **cached with a 5-min TTL so a rotated key is picked up
within the window** (no per-call env read on the hot path, no restart),
**redacted** in all output, and **never** sent to the browser or surfaced on `/v1/config`. Selecting
`EXTERNAL` with no key **fails closed** (a clear error, never a silent local fallback). The local default
needs **zero** keys. There are **no LLM keys** — GrowlerDB never calls an LLM
([D42](/system/decisions/d42-retrieval-first.md)); the external path is embedding + reranking only.

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

**Hybrid search (RRF).** A hybrid query runs both modalities **concurrently** — lexical **BM25** and
vector **KNN** — and fuses their rankings with **Reciprocal Rank Fusion** (`RRF_K = 60`) into one
ranked list of coordinates. This is where semantic recall complements exact-term precision: on a
real-model eval over paraphrase queries (zero lexical term overlap), hybrid strictly beats
lexical-only. The semantic arm **defines** the request (its vector field drives resolution and
authz), so its failure fails the query; a failed **lexical** arm degrades to the semantic ranking —
**flagged** via `partial` + a `warnings` entry, never silently ([D45](/system/decisions/d45-degraded-vs-error.md)),
and refused outright when the request set `require_complete`. Hybrid `total` is the lexical arm's
true match count when that arm succeeded (KNN has no match count), else the fused page size.

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
> the **opt-in reranker** (local + external providers), and the **approximate HNSW index** (auto-selected
> at scale, filtered KNN stays exact). Remaining: **distributed windowed** semantic search, the
> **write-path embed stage** (LOCAL embedding on reindex/sync/reconcile, pooled/pipelined ingest, and
> `SOURCE` bring-your-own vectors — [D46](/system/decisions/d46-embed-write-path-stage.md), TASK-326),
> and the deferred **EXTERNAL pooling** / faster-runtime bake-off — see
> [known limitations](/quality/known-limitations/index.md). The interface and stored format are stable.
