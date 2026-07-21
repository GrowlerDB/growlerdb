---
type: Decision
title: 'D46. Embedding is a write-path stage over provenance-typed vectors'
description: Vectors are provenance-typed (SOURCE / LOCAL / EXTERNAL); embedding is a pipelined, pooled stage shared by every write path — not a call inside the cold build — so coverage can't regress per-path and ingest throughput isn't gated by an inline embed. EXTERNAL pooling and a faster-runtime bake-off are deferred.
tags: [decision, adr]
timestamp: 2026-07-20T00:00:00
---

# D46. Embedding is a write-path stage over provenance-typed vectors

**Context.** Local embed-at-ingest was implemented as a single call — `embed_located_docs` — inside
the cold `build_from_source` path and nowhere else. Two problems follow. (1) **Coverage regresses
per-path:** `reindex` rebuilds a vector index with an empty ANN sidecar, and incremental `sync` /
drift `reconcile` write re-read docs un-embedded — so any rebuild or append silently drops or decays
vectors (TASK-326; same silent-coverage-loss family as [D45](/system/decisions/d45-degraded-vs-error.md)).
(2) **It doesn't scale:** the embed runs inline and blocking, upstream of the commit, so for a vector
field embedding throughput *is* ingest throughput — the fast lexical/structured ingest and the slow
CPU embedding are conflated into one serial stage. The demo's minutes-long first-run wait and the
ingest throughput ceiling are the same design choice.

**Decision.**

1. **Vectors are provenance-typed.** A vector field's `provider` generalizes to three provenances —
   same field, same query path, different origin of the vector:
   - **SOURCE (bring-your-own):** the vector already exists as a column in the source table; ingest
     maps it straight through. Zero hot-path embed cost — the scalable, high-throughput default.
   - **LOCAL:** embedded in-process by the bundled model ([D20](/system/decisions/d20-embedding-model.md)).
     Batteries-included; for demos and low volume.
   - **EXTERNAL:** embedded by a remote / pooled service. Throughput scales with the backend.
2. **Embedding is a write-path stage, not a build-only call.** One embed stage sits between
   source-read and index-write and is shared by **every** source→index path — build, `reindex`,
   `sync`, `reconcile`. Coverage becomes a property of the write pipeline, so it cannot regress on a
   per-path basis (this is the structural fix for TASK-326 — a shared seam, not three patched call
   sites).
3. **The stage is pipelined and pooled.** Read chunk *N+1* while embedding *N* while writing *N-1*,
   so ingest throughput ≈ `max(stage)`, not `Σ(stages)` — the reader no longer idles during embed.
   LOCAL embedding fans its sub-batches across cores (superseding today's sequential loop of 32-input
   forwards); SOURCE is a no-op pass-through. Per-key order is preserved (a later upsert to a key
   never overtakes an earlier one on a faster worker), so the keyed-upsert and checkpoint contracts
   are unchanged.
4. **Progress is observed from the stage, never by handicapping ingest.** Coverage/throughput
   progress (embedded / total, docs·s⁻¹) is a gauge emitted from inside the embed stage, **decoupled
   from commit granularity**. We do **not** shrink the streamed-read chunk or the commit chunk to make
   a build observable — that inflates segment/commit count and cripples throughput to buy a progress
   bar. The commit path keeps its throughput-optimal chunk size; the durable coverage number stays
   `docs_with_vector`.

**Scope / deferrals.**

- **EXTERNAL embedding pooling is deferred** until a workload needs it. The stage's shape (a
  bounded-concurrency worker pool) accommodates a remote embedding fleet when the time comes — no
  redesign, just a pooled `Embedder` behind the same seam (ties to the server-side embedding key,
  TASK-299).
- **The LOCAL runtime stays Candle BGE**; a faster-runtime bake-off (ONNX / int8-quantized / a
  smaller model) is **deferred**. The pipeline + core-level pooling make the runtime choice
  non-load-bearing — LOCAL only has to be fast enough that a right-sized demo embeds in seconds while
  overlapped with I/O, which the pooled stage delivers without a new dependency.

**Consequences.**

- The scaling story is honest and provenance-shaped: high-throughput vector ingest is **SOURCE**
  (embed upstream, bring vectors) or **EXTERNAL** (pooled service); **LOCAL** is the convenience floor.
  GrowlerDB is the retrieval/indexing engine, not an embedding farm.
- The demo uses LOCAL over the pooled stage on a right-sized corpus — genuinely fast embed-on-ingest,
  no chunk-size tricks — and "does it scale?" is answered by the same stack running SOURCE vectors
  over a large corpus (see the demo first-run and BYO-vector work).
- Extends [D20](/system/decisions/d20-embedding-model.md) (model) and
  [D43](/system/decisions/d43-node-local-query-embedding.md) (node-local query embedding); relates to
  [D10](/system/decisions/d10-ingestion-runtime.md) (ingestion runtime),
  [D31](/system/decisions/d31-ingest-loss-guards.md) / [D32](/system/decisions/d32-parallel-ingest.md)
  (ingest guards & parallelism), and [D45](/system/decisions/d45-degraded-vs-error.md) (coverage is a
  flagged, observable property).

**Status.** Accepted (direction); **not yet built**. Today embedding runs only on the cold build —
the per-path coverage gap (TASK-326) stands until the stage lands. Sequencing: (1) embed stage +
SOURCE provenance seam (closes TASK-326, adds pooling + the progress gauge); (2) demo first-run over
the stage. EXTERNAL pooling and the runtime bake-off wait for need.
