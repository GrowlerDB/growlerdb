---
type: Decision
title: 'D43. Query embedding is node-local in the distributed search path'
description: In the distributed (gateway) semantic/hybrid path each node embeds the query text; the gateway stays pure orchestration with no embedding model dependency.
tags: [decision, adr]
timestamp: 2026-07-18T00:00:00
---

# D43. Query embedding is node-local in the distributed search path

**Decision.** For distributed semantic / hybrid search, the **node** (not the gateway) embeds the query
text. The gateway's `SemanticSearch` fan-out sends each shard node the query **text**; the node embeds it
with the vector field's configured embedder (the same [`Embedder`](/system/decisions/d20-embedding-model.md)
factory it uses at ingest), runs its top-K KNN, and returns hits. The gateway remains **pure
orchestration** — scatter, gather, score-merge, and (for hybrid) RRF-fuse — and links **no** embedding
model.

**Why not embed once at the gateway.** Embedding at the gateway would save N−1 embeddings per query, but
would force the **gateway process to carry the ML runtime + model + each index's `VectorSpec`** — a real
dependency-footprint and provisioning cost on a component whose job is routing. Node-local embedding
avoids that:

- Each node **already** holds the model and the resolved field config (it embeds at ingest), so nothing
  new is provisioned.
- Embedding is **deterministic** for a given model, so every shard produces the **same** query vector —
  there is no cross-shard inconsistency to reconcile.
- The redundant work is a single short embed per shard on the query path (cheap relative to the search),
  and it keeps the gateway ML-free and horizontally simple.

The tenant `= <claim>` filter is likewise injected **at the node** (inside the KNN via `with_knn_filter`),
so tenant isolation holds on the vector path exactly as for lexical search.

**Deferred.** Distributed **windowed** (time-sharded) semantic search is not yet wired — window-routing
nodes return `Unimplemented` for `SemanticSearch`; the embedded and ordinal-sharded paths are complete.

**Status.** Accepted.
