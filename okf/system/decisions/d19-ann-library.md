---
type: Decision
title: 'D19. ANN index: owned per-segment HNSW artifact'
description: A GrowlerDB-owned HNSW ANN artifact per segment (open Rust ANN crate) behind an AnnIndex trait, carried through the one Tantivy segment lifecycle — because the pinned Tantivy has no native KNN.
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D19. ANN index: owned per-segment HNSW artifact

**Decision.** Build the approximate-nearest-neighbor index as a **GrowlerDB-owned artifact, one per
segment**, behind an `AnnIndex` trait, and carry it through the **one existing segment lifecycle** —
built alongside the Tantivy segment, sealed, backed up, and merged like a lexical segment (a versioned
sidecar, registered in the segment's files so backup/restore covers it). Query-time KNN runs over these
per-segment artifacts and its results [fuse](/product/functional/search/index.md) with the BM25 set.

**Correction to the original framing.** The initial decision named "Tantivy native KNN." The pinned
Tantivy (`0.26`) has **no native vector/KNN support**, so the graph cannot be a Tantivy field — hence the
GrowlerDB-owned artifact above. The HNSW graph itself comes from an **open Rust ANN crate**
(`hnsw_rs` / `instant-distance` are the candidates); the concrete crate is chosen and validated in the
ANN-index build task (TASK-42). Brute-force KNN over a stored-vector fast field is the small-N fallback
behind the same trait. Either way it stays **one segment lifecycle** for both modalities.

**As built (TASK-42).** The `VectorIndex` trait (`build` / `knn` / serialize) is realized by the
**brute-force exact** `BruteForceIndex` (`growlerdb-index/src/vector.rs`) as the shipped
implementation: at the current per-segment N it is exact (no recall loss), stably serializable
(postcard), expresses all three `VectorMetric`s (Cosine/Dot/L2) from one stored representation, and
adds **no new dependency** (so no `deny.toml` change). An HNSW crate can replace it behind the same
trait later with no change to callers — the sidecar stores each field's index as opaque bytes and each
index self-describes its `dims`/`metric`. The artifact is a **versioned per-segment sidecar**
(`<segment-uuid>.ann`, magic `GDBa` + `u16` version, like the cold-tier `sidecar.rs`) holding one
`VectorIndex` per vector field. It is built after commit (and rebuilt after each compaction merge over
the newly-sealed segment), registered in `sealed_segments()` so backup/restore carries it, and read at
query time by a top-level `Query::Knn { field, vector, k }`: each Tantivy segment's sidecar is loaded,
`knn` runs, live docs are kept via the segment's alive-bitset, and each segment-local docid resolves to
its stored composite key exactly as a lexical hit does. Query **text** → embedding happens at the
search-service layer (`Engine::semantic_search` via `default_embedder`), so the core AST carries the
resolved vector. KNN is a **top-level** clause (KNN-only ranking); fusing it with the BM25 set (RRF) is
TASK-43, so a `Knn` nested inside a lexical `Bool`/`Boost`, or carried on the query-string / OpenSearch
/ gRPC surfaces, returns a clear "unsupported" error rather than silently mis-ranking.

**Status.** Accepted; in active build (M5). The capability ships **open**
([D41](/system/decisions/d41-vector-open-core.md)) as the ANN half of retrieval-first
([D42](/system/decisions/d42-retrieval-first.md)).
