---
type: Decision
title: D30. Layered locator — identity / reference / location
description: Split the locator into immutable identity (key terms) + an internal locator-ID fast field + a small mutable dense-array location store, with per-index location strategies; GrowlerDB imposes nothing on the source table.
tags: [decision, adr, locator, hydration, storage]
timestamp: 2026-07-04T14:22:00
---

# D30. Layered locator — identity / reference / location

**Decision.** Replace the keyed redb locator table with three layers split by mutability:
**identity** (key → document; the Tantivy key-term dictionary, already paid for), **reference**
(document → an internal dense **locator ID**, an immutable Tantivy fast field), and **location**
(locator ID → interned `file_id` + row position, a small **dense array file** patched in place).
Hydration behavior is a per-index, user-selectable **location strategy** with an auto-detected
default — `coordinates` (universal default; background compaction re-map + live-file bitmap +
lazy refresh), `row_id` (Iceberg v3 row lineage; gated on ecosystem support), or `predicate`
(store-less partition/PK-pruned lookup, per D13). Key verification and the predicate fallback stay
on under every strategy. **GrowlerDB imposes nothing on the source table** — no required sort
order, format version, writer, or maintenance regime; source-side properties are optimizations a
table earns, never requirements. The composite key encoding (D5) is unchanged.

**Why.** Measured at scale (http_logs): the keyed locator was ~84% of index bytes (~2× source) and
went stale wholesale on every Iceberg compaction (lazy refresh ≈ 1 per hydrated key). Any per-row
*keyed* map pays PK + tree overhead per row; the layered split stores keys once (in Tantivy) and
pointers in ~12 B/row. Spikes (2026-07-04): dense array **12.0 B/entry exact** vs redb-u64
**53.9 B/entry** (~3× container tax — would have kept the locator at ~1.9× source); re-map key
lookups ~1M/s warm, so compaction re-map is parquet-read-bound, not index-bound. Projected total
index:source ≈ 0.85–0.9× on worst-case http_logs (from 2.4×), ~0.45–0.55× on realistic rows.
Because the location array is tiny and the pointer data lives in segments, parked cold windows
offload ~97%+ of index bytes to object storage (previously ~16% — `aux.redb` stayed local).

**Consequences.** The Tantivy-commit-then-redb-checkpoint ordering is retained (checkpoint + batch
idempotency still depend on it); the location array fsyncs before the Tantivy commit; `aux.redb`
shrinks to meta/checkpoint/batch-idempotency. Live-key-set consumers (drift, key-count, reconcile)
move to live-doc enumeration (raw term enumeration over-reports under delete debt). An adversarial
design review (2026-07-04) rejected the earlier v3-first draft: v3 ecosystem gaps (deletion-vector
reads in iceberg-rust, changelog `_row_id`, a `rewrite_manifests` row-id bug) gate only the
`row_id` strategy, which ships later as a strategy flip. Refines **D13** (its pruning preference
becomes the `predicate` strategy); **D28** stays scoped to v3 *types* — row-lineage adoption is
tracked here. Full plan, review findings, and spike numbers: task-184's plan document (backlog).

**Status.** Accepted. Implementation staged (foundations → layers → re-map → predicate strategy →
`row_id` when the v3 stack is ready). The layers are **live** and the **keyed redb table is
deleted** — GrowlerDB being unreleased made the planned dual-write/migration phase (and its
layout marker, dual backup formats, and reindex-migration trigger) moot, so the layered store is
the only layout and the live-key-set consumers (drift, key-count, reconcile) read live-term
enumeration directly. The **compaction re-map + live-file bitmap are live** (slice 3): source
compaction is healed by a background poll-diff-and-patch instead of a per-hydration refresh tax,
asserted stale-rate ≈ 0 by the compaction-under-hydration regression test — see
[locators & segments](/system/storage/locators-segments.md). The **`predicate` strategy is live**
(slice 4): a per-index `location_strategy` definition option (`COORDINATES` default |
`PREDICATE`), store-less hydration through the pruned key scan, an honest-scope warning at create
(unclustered high-cardinality keys degrade to broad scans — stated, not detected), and
**duplicate-PK detection** on the shared key-scan path (`growlerdb_duplicate_pks_total` +
rate-limited warning; deterministic highest-`(file, position)` winner). Remaining: `row_id`
(gated on the v3 ecosystem) and **strategy auto-detection** from table inspection, deferred until
`row_id` exists so the detector chooses among all three.
