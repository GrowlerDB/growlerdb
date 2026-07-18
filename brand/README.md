# GrowlerDB brand assets

Canonical vector sources for **Brand v1.0** (the waterline mark). The brand system — logo construction,
palette + design tokens, typography, voice, and per-surface application — is documented in the OKF:

- **[okf/product/brand/](../okf/product/brand/index.md)** — identity, voice, surfaces
- **[D40 — Unified brand system](../okf/system/decisions/d40-brand-system.md)** — the decision + the
  maturity-badge caveat (mocks show "GA / v1.0"; the shipped status is **Beta / pre-1.0**, which wins)

## Files

- **`favicon.svg`** — the waterline mark (melt tile `#46B8C8`, `#FCFCFC` berg, waterline at 44%, 22.9%
  corner radius). Source for the site/app favicon; replaces the inline data-URI favicon in `www/index.html`.
- **`social-preview.svg`** — editable vector of the OG/social card (1200×630). Render the shipped PNG
  at 1200×630+ (2× recommended) into `docs/img/social-preview.png`; needs Archivo + Geist Mono installed.

These are the **source of truth** — derived copies (the rendered PNG, placed favicons) are produced by
the brand-rollout tasks, not edited here.
