---
type: Decision
title: 'D45. Degraded results are flagged, never silent — and callers can refuse them'
description: The cutoff between a degraded 200 (partial/warnings) and an error, plus the per-request require_complete flag that opts a caller out of the third (partial) state entirely.
tags: [decision, adr]
timestamp: 2026-07-20T00:00:00
---

# D45. Degraded results are flagged, never silent — and callers can refuse them

**Decision.** A search response may degrade only when what is returned remains a **true, on-question
answer with reduced coverage**, and every such degradation is **flagged in-band** — the coarse
`partial` bool plus human-readable `warnings` naming what was lost (a failed hybrid arm, a
dev-fallback query embed). Anything else is an **error**:

- a request that can't be interpreted or authorized (4xx — never "degraded around");
- loss of the part that **defines the request's semantics** (the semantic arm of hybrid — its
  vector field drives resolution and authz; hybrid-minus-semantic would answer a different
  question than the one asked);
- **total** coverage loss (every shard failed) — zero coverage is "unanswered", not "degraded";
- capacity refusal before any work (admission) — a complete answer exists, retry.

A **failed lexical arm** in hybrid degrades (semantic-only RRF is still a valid ranking of real
candidates, flagged so the caller can re-issue), as does a down shard among many and a per-hit
hydration failure.

**`require_complete` (per request, wire-default false).** Some callers don't want the third
(partial) state at all — a flagged subset is still a subset, and a pipeline that can retry would
rather get an honest UNAVAILABLE than de-duplicate degraded pages. Setting `require_complete`
turns any coverage degradation (failed shards, a dropped hybrid arm) into a retryable
UNAVAILABLE naming the loss. **Advisory** warnings that don't reduce coverage (the dev-embedder
notice) never trip it, so dev/CI setups stay usable.

**Why in-band flags and not just logs.** The motivating incident (2026-07-20, demo stack): the
hybrid path silently swallowed every lexical-arm failure — no log, no flag — so hybrid responses
were byte-identical to semantic-only ones (scores exactly `1/(rrf_k + rank)`), and an agent
consumer confidently mis-described the corpus. A consumer can only compensate for degradation it
can see; server logs are invisible to it.

**Consequences.** `SearchResponse` carries `warnings` (proto field 7; REST omits when empty);
every gateway merge point unions shard warnings and enforces `require_complete`; the MCP `search`
tool passes both through and its instructions tell agents to read them. `total` is honest per
mode: true corpus-wide count for lexical, top-k page size for semantic (KNN has no match count),
and the lexical arm's count for hybrid when that arm succeeded.
