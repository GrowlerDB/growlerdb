---
type: Feature
title: Highlighting
description: Matched fragments per hit — server-side snippets of the analyzed match, with a client-side fallback.
tags: [feature, search, highlight]
timestamp: 2026-07-10T00:00:00
---

# Highlighting

Shows a user **why a hit matched** by marking the matched terms in a snippet of the document.

## Server-side highlighting (opt-in)

When a search opts in, the engine returns, **per hit**, a set of matched **fragments per field** that
reflect the **analyzed match** — the same tokenization/lowercasing the query ran through, and the
positions the inverted index recorded (so a phrase highlights the phrase, not each word). Only analyzed
**TEXT** fields whose text is available on the hit (the field is `cached`) can be highlighted; a
requested field that isn't highlightable is silently skipped. Fragments are generated from Tantivy's
snippet generator over the hit's cached text — no extra round trip, no re-hydration.

Highlighting is **off by default** because it is a per-hit cost (reading stored text + running a snippet
generator). A request opts in and may name the fields and bound the output (max fragments per field,
approximate fragment size); an empty field list defaults to the index's highlightable TEXT fields.

Highlights ride on the wire hit as a `map<field, fragments>`. Each fragment is carried as **XSS-safe
segments** (`{text, marked}` runs) rather than pre-marked HTML, so a client renders `<mark>`/`<em>`
around `marked` runs with no `innerHTML` — the wire is safe by construction, and multi-shard hits carry
their highlights through the Gateway merge unchanged. The [OpenSearch adapter](/product/interfaces/opensearch-adapter.md)
maps a `highlight` clause to the standard OpenSearch `highlight` response shape (`field → ["…<em>term</em>…"]`,
HTML-escaped).

## Client-side fallback

When a response carries **no** server highlights (the search didn't opt in, or a field had no matching
fragment), the console falls back to a best-effort client-side marker: it marks the parsed
[query](/product/functional/search/query.md) terms literally in the hit's cached/hydrated text. This is
a display convenience and does **not** reflect stemming, per-field analysis, or phrase positions — the
server highlight does. Both render identically (the same `{text, marked}` segment shape).

## Notes

Server highlighting reflects the analyzed match; the client-side fallback reflects the literal query
terms. Fragments are bounded (fragment size/count caps) so a hit can't return an unbounded payload.
