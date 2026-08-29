---
type: Feature
title: Variant fields
description: Index Iceberg v3 variant columns — an untyped flattened catch-all (schema-less) plus declared, discriminator-selected sub-schemas (shapes) for typed access.
tags: [feature, index, variant, mapping, roadmap]
timestamp: 2026-07-24T12:00:00
resource: https://iceberg.apache.org/spec/#variant
---

# Variant fields

**Status: end-to-end via the connector + interim Trino lane; native Rust path pending.** The Rust
core — the [D47](/system/decisions/d47-variant-mapping.md) mapping model (untyped flatten +
discriminator-selected shapes), the node's flatten indexing (`<col>#terms` path terms + an optional
analyzed `<col>#text` catch-all), the dotted-path query rewrite, and create-time cross-shape type
validation — is implemented and tested. The connector-side extraction
([D48](/system/decisions/d48-variant-delivery.md) step 1) and the engine wiring of the interim Trino
lane ([D48](/system/decisions/d48-variant-delivery.md) step 2,
[D49](/system/decisions/d49-variant-iceberg-rust-routing.md)) are **wired**: index creation
introspects a variant table's schema via Trino, ingest runs through the connector (`--variant-spec`),
and hydration forks onto Trino — the create + hydrate lanes are verified live against the seeded v3
table. The native Rust path (step 3) waits on the next iceberg-rust release. Part of the Iceberg v3
adoption path ([D28](/system/decisions/d28-iceberg-v3.md)).

An Iceberg v3 **variant** column holds a semi-structured value whose structure is per-row — its
paths are not in the table schema, so the existing rule that every mapped path resolves to a
declared source leaf ([create](/product/functional/index-management/create.md)) cannot apply.
A variant column gets its own mapping surface with two **composable** modes:

## Flatten (schema-less)

No declaration needed. The whole value is indexed as one field, untyped:

- **Path terms** — every leaf is indexed as an exact `path = value` term, so
  `payload.user.login:octocat` filters work without any declared mapping. Values are indexed in
  canonical string form.
- **Text catch-all** (optional) — the value's string leaves feed one analyzed TEXT field for
  full-text (BM25) search over the whole object.

Because flatten is untyped, **type conflicts cannot exist**: a path whose values are sometimes
numbers and sometimes strings is just two terms. The trade-off is no ranges, sorts, or numeric
semantics on flattened paths — typed access requires a shape.

## Shapes (declared sub-schemas)

One variant column often multiplexes several kinds of value (e.g. an event payload per event
type). The mapping may declare several named **shapes** — typed sub-mappings of paths — and a
**discriminator**: a path inside the variant (or a sibling column) whose value selects the shape
for each row.

- A shape's paths use the full field-type surface (TEXT/KEYWORD/LONG/DOUBLE/BOOL/DATE/IP/VECTOR)
  and per-field flags (`fast`, `cached`, `indexed`, …) exactly like top-level fields; a VECTOR
  path makes a variant's text semantically searchable
  ([D46](/system/decisions/d46-embed-write-path-stage.md)).
- Resolved field names are the dotted `column.path` — the same naming as struct flattening —
  **not** namespaced per shape, so queries don't need to know which shape a document matched.
  The same path declared in two shapes with the same type is the same field; with different
  types it is a **create-time error**.
- A row whose discriminator matches no declared shape skips typed extraction (an ingest counter
  records it, per [D45](/system/decisions/d45-degraded-vs-error.md)'s loud-degradation posture);
  flatten, if enabled, still covers it. A declared path whose value fails its type is dropped for
  that document, likewise counted.

Flatten and shapes compose per column: flatten alone is fully schema-less; shapes alone is
schema-only; both gives typed declared paths plus a flattened catch-all over the whole value
(flatten always covers the whole value, including declared paths).

## Retrieval

No whole-value blob is stored in the index. Declared shape paths support `cached` like any field
(within the [cached-field policy](/system/decisions/d23-cached-field-policy.md)); the original
object comes back via [hydration](/product/functional/hydration.md) — the hit's coordinates
resolve the authoritative row, variant included.

## Extraction & delivery

Shape selection and leaf extraction happen at extraction time — in the
[reader/connector](/system/runtime/components/connector.md) — so nodes receive resolved scalar
leaves and the wire value model stays scalar. Readers prefer Parquet-**shredded** typed
subcolumns when a data file shreds a declared path, decoding the binary variant only for the
residual.

On the node, a variant field carries no single typed field: flatten builds a reserved
`<column>#terms` raw-keyword field whose tokens are `path\u{1}value` (an exact `path = value` term),
and — when `text` is enabled — a `<column>#text` analyzed catch-all over the value's string leaves.
Declared shape leaves are ordinary typed fields at their dotted `column.path`. The query layer
routes a dotted term over an **undeclared** sub-path (`payload.user.login:octocat`) to the
`#terms` token, a bare `payload:query` to the `#text` catch-all, and a **declared** shaped path to
its own typed field (so ranges/sorts work). The extracted leaves reach the node over the wire's
composite variant-leaf shape (`VariantColumn`: column-qualified path + scalar value + the row's
discriminator) — the value model stays scalar-leaf-only, no nested/JSON `Value` kind.

Delivery order is decided in [D48](/system/decisions/d48-variant-delivery.md): ingest ships
**connector-first** (bootstrap + changelog; Spark reads variant today), hydration for variant
tables routes through **Trino in the interim** (released iceberg-rust cannot yet plan over a
variant schema — see [D49](/system/decisions/d49-variant-iceberg-rust-routing.md) for the full
per-call-site routing), and the **native Rust path takes over** — batch build and in-point-read
variant decode — once iceberg-rust ships variant support, leaving Trino as the permanent slow lane
for delete-bearing files and the unprunable-scan fallback.

Phasing: **first** flatten end-to-end (connector ingest + Trino-backed hydration); **then**
shapes — discriminator, typed fields and flags, shredding-aware reads; **then** the native-path
swap.
