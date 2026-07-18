---
type: Concept
title: Brand identity — logo, colour, type
description: GrowlerDB's visual system — the waterline mark, the neutral dark-first palette + design tokens, the Archivo / Instrument Sans / Geist Mono type trio, and radius/spacing.
tags: [brand, design, identity, tokens, logo, colour, typography]
timestamp: 2026-07-18T00:00:00
---

# Brand identity — logo, colour, type

The visual system for [Brand v1.0](/product/brand/index.md) ([D40](/system/decisions/d40-brand-system.md)).
**Dark-first everywhere; soft neutral grays, never pure black/white.** The source-of-truth vector
assets live in [`brand/`](https://github.com/GrowlerDB/growlerdb/tree/main/brand) (`favicon.svg`,
`social-preview.svg`).

## The mark — a berg crossing the waterline

The name story: a *growler* is a small berg calved from an iceberg — GrowlerDB is the fast, derived
index calved off the Apache Iceberg lake ([glossary](/glossary.md)). The mark is a **berg crossing a
waterline**, constructed exactly (see `brand/favicon.svg`):

- **Tile:** melt `#46B8C8`, corner radius **22.9%** of tile size (7.3 px at 32 px).
- **Berg:** a circle, diameter **62.5%** of the tile, centred; `#FCFCFC` above the waterline.
- **Waterline** at **44%** of the berg's height; the below-line half is `#FCFCFC` at **42% opacity**
  over the melt tile.
- **Minimum size 16 px.** Never: flip the waterline, gradients under the mark, recolour the berg, or
  revive the retired pixel-G / handwritten wordmark.
- **Variants:** a glacier `#4A7FD9` tile *only* inside product UI where melt would collide with status
  colours; a tile-less berg on brand-owned dark surfaces.

**Lockup:** mark + wordmark. The wordmark is **Archivo 800, lowercase, letter-spacing −0.03em**:
`growler` in `#FCFCFC` on dark (`#26282C` on light) and `db` **always melt** `#46B8C8`. Mark height ≈
1.3× cap height; gap ≈ half the mark width. **Casing:** "GrowlerDB" in prose; "growlerdb" only in the
lockup, code, CLI, and package names. Never "Growler DB", "GrowlerDb", or "Growler".

## Colour palette & design tokens

No harsh black or white anywhere; text on glacier/melt fills is `#16181C`–`#14161A`.

| Token | Hex | Use |
|---|---|---|
| glacier | `#4A7FD9` | Brand primary. Text/links on **light** surfaces. |
| glacier-light | `#7FA9D4` | Interactive on **dark**: buttons, active nav, focus rings, query cursor, table scores. Hover `#94B8DD`; text on it `#14161A`. |
| melt | `#46B8C8` | Identity: mark tile, the "db", topbar accents, data highlights. Icon-only on white — never text/links on light. |
| amber | `#D9A04A` | Warnings, search-hit `<mark>` highlight (22% alpha bg), beta flags. Never decorative. |
| bg | `#141517` | Deepest surface / page background. |
| panel | `#1C1D20` | Cards, topbar, rail. |
| panel-2 | `#232529` | Nested surfaces, buttons. |
| field | `#17181B` | Input backgrounds (console). |
| line | `#2E3035` | Default borders. |
| line-strong | `#3F4147` | Emphasised borders (inputs, table header rule). |
| text | `#E8E9EB` | Primary text. |
| text-2 | `#B9BCC2` | Secondary text. |
| text-3 | `#8B8F97` | Muted / labels. |
| brand white | `#FCFCFC` | Headlines, wordmark, berg. **Never** `#FFFFFF`. |
| ok | `#4FB87E` | Status ok (health dot). |
| danger | `#D9604A` | Errors. |

### Token mapping → the console

The [console](/product/interfaces/ui.md) is tokenised via `ui/src/app.css`; the brand values become the
`[data-theme='dark']` set (**dark is the default**):

```
--bg:#141517  --panel:#1c1d20  --panel2:#232529  --field:#17181b
--line:#2e3035  --line-strong:#3f4147
--text:#e8e9eb  --text-2:#b9bcc2  --text-3:#8b8f97
--accent:#7fa9d4  --on-accent:#14161a          /* glacier-light on dark */
--ok:#4fb87e  --warn:#d9a04a  --danger:#d9604a
```

The **light** theme keeps `--accent:#4a7fd9` (glacier). Add a `--melt:#46b8c8` token for topbar/identity
accents. Search-hit `<mark>` background is `rgba(217,160,74,0.22)`, text inherited.

## Typography — a three-family trio

| Family | Role | Notes |
|---|---|---|
| **Archivo** | Headlines, wordmark, section titles | Weights 600–800; −0.02 to −0.03em at display sizes; **sentence case, never all-caps headlines**. |
| **Instrument Sans** | Body, UI labels, docs prose | 400–600; body 14–16px/1.6; console base 13px. |
| **Geist Mono** | Keys, queries, coordinates, scores, code, micro-labels | Micro-labels (INDEX, SORT, panel heads): 9.5–11px, uppercase, +0.08em, weight 500–600. |

This trio **replaces IBM Plex Sans/Mono in the console** and the system-font stack on the website.
Fonts are Google Fonts (Archivo 600/700/800, Instrument Sans 400/500/600, Geist Mono 400/500/600) —
**self-hosted** on every surface (console, website, docs). **Disable ligatures + contextual
alternates on code/mono** (`font-variant-ligatures: none; font-feature-settings: 'liga' 0, 'calt' 0`):
Geist Mono's `calt` otherwise collapses the space before a `--` (rendering `docs --name` as
`docs--name`) and merges operators like `://` / `->`.

## Radius & spacing

- **Radius:** 6–8px controls · 7px inputs · 9–14px cards/panels · 999px pills/chips · tile = 22.9% of size.
- **Console dimensions preserved** from the current app: 54px topbar, 236px rail, 34px nav-pill height,
  row padding 8px 12px. The brand is a **re-skin, not a redesign** — structure and behaviour are unchanged.

## Interaction

- Hover lightens one step (glacier-light → `#94B8DD`; muted text → `#E8E9EB`); **no transforms or shadows**.
- Health dot: a 2s opacity pulse (1 → 0.35 → 1). Query cursor: 1.1s blink.
- Focus: a **2px glacier-light** outline, offset 2 (recolour the existing `:focus-visible` rule).
