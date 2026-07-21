---
type: Concept
title: LOCAL embedding runs only on the cold build
description: For a LOCAL-embed vector field, embedding runs only in the initial build — reindex, incremental sync, and drift reconcile write re-read docs un-embedded, so a rebuilt or appended-to index loses vector coverage. The write-path embed stage (D46) is the fix.
tags: [quality]
timestamp: 2026-07-20T00:00:00
---

# LOCAL embedding runs only on the cold build

**Limitation.** Local embed-at-ingest is wired into exactly one write path — the cold
`build_from_source`. Every other source→index path writes docs **un-embedded**:

- **`reindex`** rebuilds a vector index with an empty ANN sidecar (all vectors dropped);
- **incremental `sync`** (append fast-path) writes appended docs without vectors (coverage decays as
  the table grows);
- **drift `reconcile`** writes re-read docs without vectors.

So a **LOCAL**-embed vector index that is rebuilt, appended to, or drift-repaired silently loses
semantic coverage — the same silent-coverage-loss family as
[D45](/system/decisions/d45-degraded-vs-error.md), on the rebuild/append axis. **SOURCE**
(bring-your-own vector column) indexes are unaffected — those paths copy the column through. Coverage
is observable — `describe_index` reports `docs_with_vector` vs `num_docs` — so the gap is visible, not
hidden.

**Fix.** [D46](/system/decisions/d46-embed-write-path-stage.md): make embedding a **write-path stage**
shared by every path, so coverage is a property of the pipeline and can't regress per-path (TASK-326).
Interim guard worth considering until the stage lands: warn / refuse on a reindex or sync of a
LOCAL-embed index. **EXTERNAL** embedding pooling and a faster-runtime bake-off are separately deferred.
