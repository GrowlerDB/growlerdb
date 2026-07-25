---
type: Decision
title: D48. Variant delivery — connector-first ingest, Trino interim hydration, native on iceberg-rust
description: Variant support ships connector-first (Spark reads variant today, bootstrap + changelog); hydration for variant tables routes through Trino until iceberg-rust ships variant, then the native path takes over and Trino remains the slow lane for delete-bearing files and the stale fallback.
tags: [decision, adr, variant, ingestion, hydration]
timestamp: 2026-07-25T09:00:00
---

# D48. Variant delivery — connector-first ingest, Trino interim hydration, native on iceberg-rust

**Decision.** The [variant mapping model (D47)](/system/decisions/d47-variant-mapping.md) is
delivered in this order:

1. **Ingest via the connector.** The [connector](/system/runtime/components/connector.md) already
   covers both bootstrap (full scan) and changelog, and its Spark/Iceberg line reads variant —
   including shredded files — today. Variant extraction (flatten leaves, discriminator,
   `variant_get` for shape paths) lands there first; nodes receive scalar leaves over the
   existing wire model. This extends [D10](/system/decisions/d10-ingestion-runtime.md) (Spark
   until iceberg-rust matures) to the variant read path.
2. **Hydration for variant tables routes through Trino in the interim.** Released iceberg-rust
   cannot parse a v3 schema containing variant, which breaks scan planning — and with it every
   Rust path, including the (already iceberg-free) direct-parquet pass-1 point read. A per-index
   hydration fork sends variant-table hydration to Trino as key-predicated point queries
   returning the variant as JSON; non-variant indexes keep the native path untouched. Trino
   unavailability degrades loudly per [D45](/system/decisions/d45-degraded-vs-error.md).
3. **The native path takes over when iceberg-rust ships variant** (merged upstream 2026-07-16;
   expected in the next release). Batch/backfill decodes variant natively, and hydration pass-1
   decodes the variant column inside the existing direct parquet point read (the Arrow/parquet
   line we already pin carries the decode machinery). Trino is then demoted to the **permanent
   slow lane**: delete-bearing files and the pass-2 stale-locator fallback.

**Status.** Steps 1–2 **implemented + live-verified**; step 3 pending the next iceberg-rust release.
The connector extracts the variant column (step 1: `VariantExtractor` + `--variant-spec`; bootstrap +
changelog, `create_changelog_view` handles variant), and the interim Trino lane (step 2:
`growlerdb-source::trino` — key-predicated point `SELECT`s with the variant as JSON, `nextUri` poll,
`information_schema` introspection, D45-loud errors) is wired into the engine's create + hydrate call
sites behind the per-index fork (`declares_variant`/`has_variant_field`). Verified against the running
stack: creating `events` over the live v3 table resolves via Trino, and keys hydrate returning
`payload` as JSON. The full per-call-site routing around released iceberg-rust is recorded in
[D49](/system/decisions/d49-variant-iceberg-rust-routing.md). The seam is designed so step 3 swaps
the primary implementation without touching callers.

**Why.**

- Every component in step 1 is released and stable; nothing about ingest needs to wait.
- The hydration hot path is only blocked by *planning*, not reading — but with no usable
  external planner (below), the interim fork moves whole variant-table hydration to Trino, which
  is already in the deployment surface for SQL-side comparison. Sub-optimal latency is accepted
  as temporary and scoped to variant tables only.

**Alternatives rejected.**

- **Catalog-side (REST) scan planning as the planner seam** — would have kept pass-1 point reads
  native (planning is snapshot-cached, so an external planner amortizes to ~zero per-hydration).
  Rejected because the catalog does not implement the scan-planning endpoints
  (spec-only; upstream apache/polaris#966 open). Worth revisiting if that lands.
- **Pinning iceberg-rust to an unreleased git revision** — carries breaking-API churn in a
  load-bearing dependency for roughly one upstream release cycle, to bridge a gap Trino covers
  with released code.
- **A JVM hydration sidecar** — a new runtime component in the point-read latency path for a
  temporary gap; heavier than reusing Trino, permanent cost for interim benefit.
