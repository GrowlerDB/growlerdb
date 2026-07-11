---
type: Concept
title: Data model
description: How documents are keyed and fields are typed, cached, and made fast.
tags: [system, storage, data-model, schema]
timestamp: 2026-07-04T14:22:00
---

# Data model

How an Iceberg row becomes an indexed document.

- **Composite key** — every document is keyed by `(partition fields, identifier fields)`; this key
  routes the document, tie-breaks pagination, and is the handle for
  [hydration](/product/functional/hydration.md).
- **Key value types** — key fields may be string, integer, boolean, or **temporal** (date /
  timestamp, normalized to canonical **epoch microseconds UTC** at extraction); floats
  are rejected at definition time (unstable identity, NaN diverges cross-language). The type-tagged
  key encoding is a frozen cross-language contract (Rust `CompositeKey::encode` ↔ the JVM
  connector's `ShardRouter`), locked by golden parity vectors on both sides. A temporal key also
  builds a typed Iceberg predicate on the hydration fallback scan (a DATE column only when the
  value is an exact UTC midnight — anything unsafe falls back to an unfiltered read, never a
  predicate that could exclude the row).
- **Field types** — TEXT (analyzed), KEYWORD (exact), IP (CIDR), DATE, numeric, etc.; per-field
  analyzers; nested structures are **flattened** to dotted paths. A declared **timestamp** field
  (canonical epoch micros) enables [windowing](/product/functional/windowing-time.md).
- **TEXT indexing detail** — per-field `record: BASIC | FREQ | POSITION` (what each
  posting carries; default `POSITION`, full fidelity) and `fieldnorms: bool` (BM25 length
  normalization; default on). Positions exist only for phrase queries and are usually the
  largest slice of a text field's inverted index — `FREQ` drops them while keeping full BM25;
  a phrase query on a positionless field fails with the remedy, never silently empty. Both are
  TEXT-only (rejected elsewhere) and reindex-requiring to change.
- **Cached display fields** — values stored *with* the hit so a results page renders without
  hydration; sensitive fields are never cached.
- **Fast fields** — columnar-stored fields that are sortable / filterable / facetable / collapsible.
  A fast numeric/date/IP field is **columnar-only by default**: range, exact-match,
  sort, and exists all run on the fast field, so an inverted index alongside it would be dead
  weight (a per-doc-unique timestamp is the worst case — pure term-dict + postings bloat).
  `indexed: true` alongside `fast: true` keeps both; `indexed: false` without `fast` is rejected
  (the field would be unqueryable), and TEXT/KEYWORD can never opt out (string search has no
  columnar fallback). Flipping `indexed` is a reindex-requiring alter.
- **Mapping selection** — index all parsed fields, or an explicit allowlist on a wide table.

## Notes

The definition is versioned with the index; schema evolution adds new fields to new segments in place,
while type/analyzer changes need a [reindex](/product/functional/index-management/reindex.md).
