# GrowlerDB query syntax (for the `search` tool)

Default grammar is **Lucene-style**; pass `"syntax": "kql"` for KQL (lowercase `and`/`or`/`not`).
Call `describe_index` first — it returns every field with its type + capabilities and ready-made
`example_queries` for this index.

## Lexical query forms

| Form | Example | Notes |
|---|---|---|
| Term | `status:active` | field must be `indexed`; bare `iceberg` hits the default TEXT field. **No stemming**: `hydration` ≠ `hydrate` — use hybrid mode for meaning-level matches |
| Phrase | `"iceberg search"~2` | optional slop |
| Boolean | `a AND b`, `NOT c`, `(x OR y)` | KQL: lowercase `and`/`or`/`not` |
| Field group | `category:(guide OR reference)` | distributes the field over the group |
| Range | `age:[18 TO 65]`, `published:[2024-01-01 TO *]` | field must be `fast`; `{ }` exclusive, `*` unbounded; DATE takes ISO-8601 or epoch micros |
| Wildcard | `sensor-*`, `s?nsor` | leading `*` is cost-guarded |
| Fuzzy / prefix | `jon~1` | edit distance |
| Regex | `/ab.*/` | |
| CIDR | `ip:10.0.0.0/8` | requires an `IP` field |
| Boost | `title:iceberg^2` | |
| Match-all | `*:*` | use as `query` for facet-only calls |
| Bool term | `archived:true` | BOOL field must be `indexed` |

## What a field's flags let a query do

- `indexed` — term/phrase/wildcard queryable (TEXT/KEYWORD get this by default; numeric/BOOL/DATE
  only when declared).
- `fast` — range filters, sorting, and `aggregate` facets.
- `cached` — the value returns **with the hit** (no hydration round trip). Prefer reading cached
  fields; use `hydrate: true` (or the `hydrate` tool) only when you need authoritative/uncached
  values.

## Modes and retrieval shape

- `mode: "lexical"` (default) — BM25 over the query string above.
- `mode: "semantic"` — meaning-based KNN; needs `vector_field` (see `describe_index`'s
  `vector_fields`); `query` is embedded, not parsed, and `filter` (Lucene/KQL string) constrains
  the neighbors.
- `mode: "hybrid"` — both arms fused (RRF); best default when a `vector_field` exists.
- `hydrate: true` — each hit also carries its authoritative source `row` in the same call.
- Results are always **governed**: RBAC + tenant scoping of the caller's token apply.
