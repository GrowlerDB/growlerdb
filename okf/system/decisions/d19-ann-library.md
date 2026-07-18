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

**Status.** Accepted; in active build (M5). The capability ships **open**
([D41](/system/decisions/d41-vector-open-core.md)) as the ANN half of retrieval-first
([D42](/system/decisions/d42-retrieval-first.md)).
