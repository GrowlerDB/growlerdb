---
type: Concept
title: Brand surfaces & assets
description: How Brand v1.0 applies to the console, website, docs, and social card, plus the canonical asset inventory.
tags: [brand, design, website, console, docs, assets]
timestamp: 2026-07-20T00:00:00
---

# Brand surfaces & assets

How [Brand v1.0](/product/brand/index.md) ([identity](/product/brand/identity.md),
[voice](/product/brand/voice.md)) applies to each surface. The design handoff mocked four surfaces as
HTML references (`*.dc.html`) — **references, not production code**; the work is to recreate them in each
codebase's established patterns.

## Website — `www/index.html`

The [apex landing page](/product/interfaces/website.md) becomes a branded homepage: lockup nav with
Docs/Performance/GitHub links and a glacier-light "Get started" button; a centred hero (maturity pill,
Archivo-800 H1, `#B9BCC2` sub, two CTAs, a `$ just stack` mono card); a 3-column "How it works" grid
with melt mono kickers (`1 — INDEX`); a "Why not Elasticsearch" stack; a footer with the small lockup and
`AGPL-3.0` in mono. Copy: **"document keys" → coordinates**, hero line "Full-text, vector, and hybrid
search over your data" ([D44](/system/decisions/d44-product-scope-retrieval.md)).

## Console — `ui/`

A **re-skin of the existing Search screen**, not a redesign — only tokens, fonts, and the brand row
change ([identity token mapping](/product/brand/identity.md)): the topbar reads **melt** (mark tile +
Archivo wordmark replacing the pixel-G, melt-tinted user chip); everything below the topbar uses
**glacier-light** for interactive accents (active nav pill, query-field focus, text cursor, Search
button, the table score column); search-hit matches wrap in an amber `<mark>`; the health dot is
ok-green and pulsing. All existing behaviours (autocomplete, facets, drawers, keyset paging) are
unchanged.

## Docs — `docs/`

Not mocked; applied by token. The `growlerdb` color scheme
(`docs/_sass/color_schemes/growlerdb.scss`) **builds on the theme's `dark` scheme** — the
`@import "./color_schemes/dark"` is load-bearing: it carries the dark Rouge syntax palette, and
without it code-block tokens (e.g. JSON punctuation) fall back to the light palette and render
dark-on-dark. Brand variables override on top: surfaces/text from the neutral scale, links
**glacier-light**, inline code on `#232529` in **Geist Mono** (typography + tweaks in
`docs/_sass/custom/custom.scss`), plus the favicon and header lockup.

## Social card — `docs/img/social-preview.png`

1200×630 on `#141517`: lockup top-left, a large melt berg motif bleeding off the right edge across a
faint waterline rule, an Archivo-800 headline + `#B9BCC2` sub (with `·` / `→` separators), and a bottom
row of the maturity pill + `growlerdb.com`. Rendered at **2× (2400×1260)** from `brand/social-preview.svg`.

## Brand guidelines

The `Brand Guidelines.dc.html` reference is the design source of truth; publish it as a companion
`BRAND.md` / internal page (logo construction, palette, type specimens, voice do/don't, terminology).

## Asset inventory

Canonical vector sources live in [`brand/`](https://github.com/GrowlerDB/growlerdb/tree/main/brand):

- **`favicon.svg`** — the waterline mark, exact construction; the source for the site/app favicon and
  icons (replaces the inline data-URI favicon in `www/index.html`).
- **`social-preview.svg`** — editable vector of the og card; render the shipped PNG at 1200×630+ (needs
  Archivo / Geist Mono installed).

## ⚠️ Maturity-badge caveat

The design mocks (social card, website pill) show **"v1.0 — generally available."** This **contradicts
the current shipped maturity**, which is **Beta / pre-1.0** ([release-readiness](/quality/release-readiness.md),
set deliberately so no GA claim stands while the security review + formal benchmarks are pending). Every
surface must render the **Beta** wording, not "GA / v1.0", until that status actually changes — see
[D40](/system/decisions/d40-brand-system.md).
