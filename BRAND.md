# GrowlerDB brand guidelines

The short, contributor-facing companion to the brand. The **source of truth** is the OKF —
[`okf/product/brand/`](okf/product/brand/index.md) ([identity](okf/product/brand/identity.md),
[voice](okf/product/brand/voice.md), [surfaces](okf/product/brand/surfaces.md)) and
[D40](okf/system/decisions/d40-brand-system.md). Canonical vector assets live in
[`brand/`](brand/). If this file and the OKF ever disagree, the OKF wins — fix both.

## In one paragraph

A *growler* is a small berg calved from an iceberg — GrowlerDB is the fast, derived index calved off
the Apache Iceberg lake. **Dark-first everywhere; soft neutral grays, never pure black/white.** Glacier
blue is primary, **melt** cyan is the identity accent, amber is reserved for warnings/highlights.

## Logo — the waterline mark

A **berg crossing a waterline** (see [`brand/favicon.svg`](brand/favicon.svg) for the exact
construction): a melt `#46B8C8` tile (corner radius 22.9% of size), a `#FCFCFC` circle 62.5% of the
tile, waterline at 44% of the berg, the below-line half at 42% opacity. **Minimum size 16px.**

**Lockup** = mark + wordmark. The wordmark is **Archivo 800, lowercase, −0.03em**: `growler` in
`#FCFCFC` on dark (`#26282C` on light) and `db` **always melt** `#46B8C8`.

**Casing:** "GrowlerDB" in prose; "growlerdb" only in the lockup, code, CLI, and package names. Never
"Growler DB", "GrowlerDb", or "Growler".

**Never:** flip the waterline, put gradients under the mark, recolour the berg, or revive the retired
pixel-G / handwritten wordmark.

## Colour

| Token | Hex | Use |
|---|---|---|
| glacier | `#4A7FD9` | Brand primary; text/links on **light**. |
| glacier-light | `#7FA9D4` | Interactive on **dark** (buttons, active nav, focus, table scores). Hover `#94B8DD`. |
| melt | `#46B8C8` | Identity: mark tile, the "db", topbar accents. Icon/identity only — never body text/links on light. |
| amber | `#D9A04A` | Warnings, search-hit `<mark>` (22% alpha), beta flags. Never decorative. |
| bg / panel / panel-2 | `#141517` / `#1C1D20` / `#232529` | Surfaces (deepest → nested). |
| line / line-strong | `#2E3035` / `#3F4147` | Borders. |
| text / text-2 / text-3 | `#E8E9EB` / `#B9BCC2` / `#8B8F97` | Primary / secondary / muted. |
| brand white | `#FCFCFC` | Headlines, wordmark, berg. **Never** `#FFFFFF`. |
| ok / danger | `#4FB87E` / `#D9604A` | Status. |

No harsh black/white anywhere; text on glacier/melt fills is `#16181C`–`#14161A`. The console's
`ui/src/app.css` tokens carry these — see [identity](okf/product/brand/identity.md) for the mapping.

## Typography

- **Archivo** — headlines, wordmark, section titles (600–800; sentence case, never all-caps headlines).
- **Instrument Sans** — body, UI labels, docs prose (400–600).
- **Geist Mono** — keys, queries, coordinates, scores, code, micro-labels. **Disable ligatures +
  contextual alternates on code** (`font-feature-settings: 'liga' 0, 'calt' 0`) — `calt` otherwise
  eats the space before a `--`.

Self-hosted on every surface (console, website, docs) — no font CDN.

## Voice & terminology

**Tagline: "Search your lake. Keep one truth."** Plain, confident, falsifiable claims; no superlatives;
no iceberg puns in product UI (the *growler* story is for the name only). Use the canonical words:

| Use | Not |
|---|---|
| **coordinates** (what a search hit returns) | doc ID, pointer, "document keys" |
| **hydrate / hydration** (resolve to the authoritative row) | fetch, lookup, resolve |
| **derived index** (secondary, rebuildable) | copy, replica, cache |
| **system of record** (Apache Iceberg, always) | backend, upstream database |
| **connector / gateway / console** | ingester, proxy, dashboard |

**Maturity:** the product is **Beta / pre-1.0** — never claim "GA" or "v1.0 / generally available"
until that status actually changes (the design mocks show GA; it is superseded — see
[D40](okf/system/decisions/d40-brand-system.md)).
