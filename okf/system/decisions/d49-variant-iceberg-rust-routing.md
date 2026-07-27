---
type: Decision
title: D49. Variant-table routing around released iceberg-rust — per-call-site plan
description: Released iceberg-rust cannot parse a v3 schema containing a variant column (it fails in Catalog::load_table), so every Rust source call site over a variant table breaks — not just hydration. This records the routing decision for each call site: Trino for hydration + schema introspection, connector for ingest, raw REST metadata or a D45-loud scope-out for the rest, until the native path lands (TASK-353).
tags: [decision, adr, variant, ingestion, hydration, iceberg-rust]
timestamp: 2026-07-25T12:00:00
---

# D49. Variant-table routing around released iceberg-rust — per-call-site plan

**Decision.** Released iceberg-rust (our `0.9.1` pin; also `0.10.0`) fails to parse **any** v3 table
schema that contains a `variant` column — the error is raised inside `Catalog::load_table`, before
any scan. Variant schema support merged upstream 2026-07-16, after `0.10.0`
([D48](/system/decisions/d48-variant-delivery.md)). Because every Rust source method in
`growlerdb-source` funnels through `catalog.load_table(&ident)`, the break is **not confined to
hydration** — schema introspection, cold build, source stats, snapshot polling, and reconciliation
all fail on a variant table. Each call site is therefore routed per the table below, keyed off the
per-index fork [`ResolvedIndex::has_variant_field`](/product/functional/index-management/variant.md).
A non-variant index keeps the **native path untouched** — every routing below is gated on the fork.

| Concern (call site) | Non-variant (unchanged) | Variant-table routing |
| --- | --- | --- |
| **Schema introspection** at create — `read_source_schema` | iceberg-rust `load_table` + Arrow schema | **Trino** `information_schema.columns` → `SourceSchema` (`TrinoHydrator::read_source_schema`); key hints from the definition's explicit `key:`. Variant column → `SourceType::Other` (declared `type: VARIANT`). |
| **Hydration** — `hydrate` / `load_and_plan` / `resolve_pass1` / `point_read` | native direct-parquet point read | **Trino** key-predicated point `SELECT`, variant column `CAST(... AS JSON)` (`TrinoHydrator::hydrate`). Locators ignored (Trino re-finds by key); rows still key-verified. |
| **Ingest** — cold build / backfill / streamed read (`read_documents*`, `read_appended_since`) | native Rust scan | **Connector only** (Spark reads variant today, incl. shredded). No Rust cold build for a variant table; the node is fed exclusively by the connector's bootstrap + changelog ([connector](/system/runtime/components/connector.md)). A Rust build request for a variant index is a **D45-loud error**, not a silent empty index. |
| **Source stats** — `source_health` / `current_snapshot_records` / `partition_skew` | `load_table` metadata summary | **Scoped out (interim)**: a variant table returns no native stats gauges — best-effort via raw catalog REST metadata (`total-files-size` etc.) is a follow-up. Absence is surfaced, never a fabricated zero. |
| **Snapshot polling / lineage** — `current_snapshot(_ordered)` / `snapshot_timestamps` / `table_uuid` | `load_table` metadata | **Raw REST metadata** (the catalog's `GET .../table` JSON carries snapshots + `table-uuid` without needing schema parse) — a thin metadata read that skips iceberg-rust's schema deserialization. Interim; folds into the native path at TASK-353. |
| **Reconciliation / compaction re-map** — `scan_stale_index` / `read_documents_in_partition` / `current_plan` / `read_file_key_rows` | native scan + key scan | **Scoped out (interim)** for variant tables: reconciliation and the compaction re-map poller are disabled with a D45-loud degradation flag; the connector's changelog is the source of truth until the native path lands. |

**Status.** **Implemented for the create + hydrate + ingest lanes; live-verified.** Shipped: the
fork predicates (`IndexDefinition::declares_variant` / `ResolvedIndex::has_variant_field`), the Trino
seam (`growlerdb-source::trino`, `shared_hydrator()`), and the wiring — **create** introspects a
variant table's schema via Trino (`index_shard`/`define_index`/`create_index`) and **skips the
native cold build** (loudly; connector-fed), and **hydration** forks in `hydrate::get_by_key` + the
node `LookupService` onto the Trino lane. Ingest is the connector (`--variant-spec`). Verified
against the running stack: creating `events` over the live v3 table resolves its schema + shapes via
Trino, and `TrinoHydrator` hydrates keys returning `payload` as JSON. Still scoped-out (interim,
D45-loud where hit): source **stats**/**snapshot-poll**/**reconcile** and **alter**/**describe_source**
on a variant table (no def to fork `describe_source`; raw-REST metadata reads are the follow-up).
Because the ingestion-status source-head probe is native-only, a variant index reports
`source_probeable = false` in its ingestion status; the console **health roll-up excludes it** — a null
source head + `unknown` lag is *expected* for a variant source (read via the connector's Trino lane),
not a cluster outage, so a healthy variant index no longer flags the header Down (only its structural
failures — `no_primary` / `unreachable` — still degrade). The
native path (TASK-353) collapses every "Trino / scoped-out" cell back to native once iceberg-rust
ships variant, leaving Trino only as the permanent slow lane (D48).

**Why.**

- The parse failure is at `load_table`, the one chokepoint every source method shares, so the blast
  radius is the whole crate — deciding routing per call site up front (rather than discovering it
  mid-implementation) is what keeps a variant index from silently half-working.
- Trino is already in the deployment surface for SQL-side comparison, reads v3 variant, and needs no
  schema parse on our side — the cheapest interim planner for both hydration and introspection.
- Ingest has no gap to bridge: the connector's Spark line reads variant today (D48 step 1), so the
  native Rust build simply doesn't run for a variant table — cleaner than a half-native scan.

**Alternatives rejected.**

- **Pin iceberg-rust to the unreleased variant revision** — breaking-API churn in a load-bearing
  dependency for ~one release cycle, to bridge a gap Trino covers with released code (also D48).
- **Silently skip variant columns and index the scalars only** — violates the loud-degradation
  posture ([D45](/system/decisions/d45-degraded-vs-error.md)); a variant index that quietly dropped
  its variant would look healthy while serving nothing for the column.
- **A JVM hydration sidecar** — a new runtime component in the point-read path for a temporary gap;
  heavier than reusing Trino (also D48).
