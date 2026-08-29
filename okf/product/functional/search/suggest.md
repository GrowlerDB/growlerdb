---
type: Feature
title: Suggest (autocomplete)
description: Type-ahead value suggestions for the field:prefix token being typed.
tags: [feature, search, suggest, autocomplete]
timestamp: 2026-07-04T14:22:00
---

# Suggest (autocomplete)

Type-ahead completion — as a user types a `field:prefix` token, `POST /v1/suggest` (gRPC `Suggest`)
returns matching values so they can complete the query without knowing exact terms.

## Behavior

- Suggests values for the field token under the cursor; the console wires it into the search box.
- In a windowed index, suggest fans out across all windows (no time pruning) to stay complete.
- Ranked by descending document frequency (ties by term ascending); frequencies are approximate (not
  merge-on-read liveness-filtered) — the accepted contract for a suggester hint.

## Prefix backing: term dictionary vs. completion sidecar

Two paths, transparent to the caller:

- **Default (live term dictionary).** Prefix suggest seeks the field's FST term dictionary per segment
  and ranks the matches. No extra storage; cost grows with the per-segment fan-out and the number of
  terms under the prefix.
- **Opt-in completion sidecar** (`suggest: true` on the [field mapping](/product/functional/index-management/index.md)).
  Building the segment also writes a per-segment `<segment>.cmp` sidecar — a precomputed
  `prefix → top-K-by-frequency` table (bounded prefix depth) — so a short prefix answers from a lookup
  plus a bounded cross-segment merge instead of a term-dictionary scan, for single-digit-ms latency
  parity with a dedicated completion structure. The sidecar follows the ANN-sidecar lifecycle (written
  after commit, rebuilt on compaction, carried by backup/replica shipping). Any case the table can't
  answer — a prefix longer than the built depth, a segment without the sidecar, or an unflagged field —
  falls back to the live term-dictionary path, so results are identical within the approximate-suggester
  contract. Storage is the disclosed cost, the direct counterpart to an external engine's completion FST.

Fuzzy ("did-you-mean") suggest always uses the live dictionary scan.

## Notes

Backed by the indexed terms of the target field. See [query](/product/functional/search/query.md) for
the syntax the suggestions complete.
