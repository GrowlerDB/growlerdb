---
type: Decision
title: D47. Variant mapping — untyped flatten + discriminator-selected shapes
description: Iceberg v3 variant columns are indexed via two composable modes — an untyped flattened catch-all (path terms + text) and declared sub-schemas (shapes) selected per row by a discriminator — with no dynamic typed mapping and no stored whole-value blob.
tags: [decision, adr, variant, mapping]
timestamp: 2026-07-24T12:00:00
---

# D47. Variant mapping — untyped flatten + discriminator-selected shapes

**Decision.** An Iceberg v3 `variant` column is mapped through two composable modes, defined in
[variant fields](/product/functional/index-management/variant.md):

1. **Flatten (schema-less):** the whole value is indexed untyped as one field — every leaf as an
   exact `path = value` term, plus an optional analyzed text catch-all over string leaves. No
   declaration, and no per-path types, so type conflicts cannot arise by construction.
2. **Shapes (declared sub-schemas):** named typed sub-mappings of paths, selected per row by a
   declared **discriminator** path (inside the variant or a sibling column). Shape paths get the
   full field-type/flag surface; resolved names are the dotted `column.path`, shared across
   shapes (same path + same type = same field; a type disagreement is a create-time error).

Shape selection and leaf extraction happen at extraction time (reader/connector), so nodes and
the wire model stay scalar-leaf-only. No whole-value blob is stored: declared paths may be
`cached` ([D23](/system/decisions/d23-cached-field-policy.md)); the full object is retrieved by
[hydration](/product/functional/hydration.md). Readers use Parquet-shredded subcolumns for
declared paths where available.

**Status.** Mapping model **implemented** in the Rust core (index-def resolution, the untyped
flatten node index, the dotted-path query rewrite, and create-time cross-shape type validation);
connector-side extraction and the interim hydration wiring are in progress
([D48](/system/decisions/d48-variant-delivery.md), [D49](/system/decisions/d49-variant-iceberg-rust-routing.md)).
Supersedes the variant clause of [D28](/system/decisions/d28-iceberg-v3.md)
("variant to flattened dotted paths") — flatten generalizes it and shapes add typed access; D28
continues to cover the rest of the v3 types path (nanosecond timestamps). Still gated on ecosystem
variant support for the native read path (iceberg-rust/Arrow reads, Parquet shredding); Spark
extraction is available today.

**Why.**

- The index model is schema-driven — every mapped path resolves against the source schema at
  create ([create](/product/functional/index-management/create.md)) — and the value model is
  scalar end-to-end. Variant's per-row structure breaks the first; these two modes preserve both
  invariants instead of abandoning them.
- Untyped flatten makes schema-less ingestion **total**: no document is rejected and no path is
  silently re-typed mid-stream, because nothing in flatten has a type.
- Real tables multiplex heterogeneous payloads through one variant column; a discriminator is
  the cheap, deterministic per-row lookup (one term read) that gives each kind its typed fields
  without cross-shape ambiguity.

**Alternatives rejected.**

- **Dynamic typed mapping** (auto-discover paths at ingest and assign real types on first
  sight): requires type-conflict machinery (lock/coerce/reject), grows the field set unboundedly
  with the data, and moves schema resolution from create time into the ingest path. Typed access
  is what shapes are for.
- **Stored whole-value blob** (cached JSON of the object): duplicates the authoritative row in
  the index against [D23](/system/decisions/d23-cached-field-policy.md)'s minimal-explicit
  caching; hydration already returns the original.
- **Shape selection by structural matching** (first schema whose paths fit): order-dependent,
  costlier per row, and can mis-match near-identical payloads; a discriminator is explicit and
  O(1). Revisitable as a fallback for discriminator-less tables if demand appears.
