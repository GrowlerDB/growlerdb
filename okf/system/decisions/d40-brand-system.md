---
type: Decision
title: 'D40. Unified brand system (Brand v1.0)'
description: Adopt one dark-first brand — the waterline mark, a neutral palette with glacier primary / melt identity / amber warnings, and the Archivo / Instrument Sans / Geist Mono type trio — across console, website, docs, and social, replacing the fragmented pre-GA surfaces.
tags: [decision, adr, brand, design]
timestamp: 2026-07-18T00:00:00
---

# D40. Unified brand system (Brand v1.0)

**Decision.** Adopt a single, documented **brand system** across every GrowlerDB surface, replacing the
fragmented pre-GA look (a blue IBM-Plex console, an orange-on-charcoal website, a handwritten social
card, unthemed docs). The system — captured in [Brand](/product/brand/index.md)
([identity](/product/brand/identity.md), [voice](/product/brand/voice.md),
[surfaces](/product/brand/surfaces.md)) — is:

- **Dark-first, neutral grays** (never pure black/white).
- **A waterline mark** — a berg crossing the waterline — echoing the name story (a *growler* is a small
  berg calved off the Iceberg lake). Retires the pixel-G and the handwritten wordmark.
- **Glacier `#4A7FD9` primary, melt `#46B8C8` identity, amber `#D9A04A` reserved for warnings/highlights**;
  glacier-light `#7FA9D4` for interactive accents on dark.
- **A three-family type trio:** Archivo (display), Instrument Sans (body), Geist Mono (keys/queries/code).
- **Voice:** tagline "Search your lake. Keep one truth."; plain, falsifiable claims; canonical
  terminology (coordinates / hydrate / derived index / system of record). *(The positioning line was
  later broadened by [D44](/system/decisions/d44-product-scope-retrieval.md) to "Full-text, vector, and
  hybrid search over your data.")*

**Why.** Pre-GA, each surface had drifted to its own palette and type, which reads as unfinished at a
public launch. One system makes the surfaces legibly one product and encodes the "derived index over
the Iceberg lake" story into the mark and voice. The console change is a **re-skin, not a redesign** —
tokens and fonts only, all behaviour preserved — so the cost is bounded and low-risk.

**Scope.** Console (`ui/` tokens + fonts), website (`www/index.html`), docs (just-the-docs dark
overrides), the social/OG card + favicon, and a copy/terminology sweep. Applied surface-by-surface via
the [brand backlog epic](https://github.com/GrowlerDB/growlerdb/tree/main/../backlog).

**Caveat — maturity wording.** The design mocks show **"v1.0 — generally available."** This is
**superseded by the current maturity decision** (Beta / pre-1.0 — see
[release-readiness](/quality/release-readiness.md)): no GA claim stands while the external security
review and formal benchmarks are pending. Surfaces render the **Beta** badge until that status actually
changes; the "GA" mock copy is not adopted.

**Status.** Accepted; rollout tracked in the backlog.
