---
type: Feature
title: Query
description: Full-text query via a canonical AST, with Lucene- and KQL-style string forms.
tags: [feature, search, query]
resource: /docs/query-language.md
timestamp: 2026-07-20T00:00:00
---

# Query

GrowlerDB has a **canonical structured query AST**; the string forms parse into it. Most callers use
the **Lucene-style** string (the default `query` on [`/v1/search`](/product/interfaces/rest.md)); a
**KQL** variant (lowercase operators) is selected per request — see
[syntax](/product/functional/search/syntax.md).

## String forms (Lucene)

Term (`status:active`; on a `BOOL` field an exact `archived:true`/`false`), default-field term
(`iceberg`), phrase (`"iceberg search"~2`), boolean (`a AND b`, `NOT c`, `()`), field-grouped set
(`category:(guide OR reference)` distributes the field over the group → `category:guide OR
category:reference`), range (`age:[18 TO 65]`, `{` `}` exclusive, `*` unbounded; a `DATE` bound is
epoch **micros** or an **ISO-8601 / RFC3339** date, `published:[2024-01-01 TO *]`), wildcard
(`sensor-*`, `?`; a leading `*` is cost-guarded), fuzzy (`jon~1`), prefix, regex (`/ab.*/`), CIDR
(`ip:10.0.0.0/8`, requires an `IP` field), boost (`^2`), match-all (`*:*`).

## AST clauses

`MatchAll · Term · Terms · Match · Phrase · Prefix · Wildcard · Fuzzy · Regex · Exists · Range ·
IpCidr · Bool (must/should/must_not/filter) · Boost`. The structured API and the
[OpenSearch adapter](/product/interfaces/opensearch-adapter.md) target these directly. Field existence
and type rules are validated **at execution** against the index schema.

## Returns

Ranked **coordinates** (the composite key) + a BM25 score — not documents; a true cross-shard `total`
and a `partial` flag if a shard was down. Fetch rows via [hydration](/product/functional/hydration.md) —
either the standalone `keys:get`, or **inline** (`hydrate: true` attaches each hit's authoritative
row to the same response, per-hit-degrading on failure).
Execution details: [system/query-execution](/system/query-execution.md).
