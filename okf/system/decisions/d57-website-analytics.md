---
type: Decision
title: D57. Website analytics — self-hosted Plausible, cookieless, first-party
description: The marketing (growlerdb.com) and docs (docs.growlerdb.com) sites use self-hosted Plausible Community Edition for visitor/engagement analytics. Cookieless, no PII, no cross-site tracking — so no consent banner. Served first-party via an.growlerdb.com (Apache reverse proxy on the apex VM → a private, self-hosted Plausible instance) so ad-blockers don't strip it. Distinct from D26 (product telemetry, unchanged).
tags: [decision, adr, website, analytics, privacy]
timestamp: 2026-09-07T00:00:00
resource: https://github.com/GrowlerDB/growlerdb/blob/main/www/README.md
---

# D57. Website analytics — self-hosted Plausible, cookieless, first-party

**Decision.** The public **websites** — the apex landing page `growlerdb.com` (`www/index.html`) and the
docs site `docs.growlerdb.com` (`docs/`) — use **self-hosted Plausible Community Edition** for visitor
and engagement analytics. It is **cookieless**, stores **no personal data**, and does **no cross-site
tracking**, so it needs **no consent banner**. The script and event API are served **first-party** from
`an.growlerdb.com` — an Apache reverse proxy on the apex VM that forwards to a private, self-hosted
Plausible instance — so ad-blockers (heavy among the launch audience) don't strip the beacon.

**Status.** Accepted.

## Why

Before announcing on Hacker News we want honest visitor + engagement numbers (sources, top pages,
entry/exit, bounce, visit duration). Two constraints shape the choice:

- **Privacy-first, on our infra.** The analytics data stays on our own infrastructure, consistent with
  the project's ethos. This is *website* analytics only; it does not touch the product.
- **Blocker resistance.** The HN audience runs ad-blockers heavily, and both Google Analytics and
  Plausible's own cloud hosts sit on common blocklists. Self-hosting behind a **first-party** endpoint on
  our own domain is what keeps the numbers from silently under-counting.

## Scope — not product telemetry (cf. D26)

This is orthogonal to **[D26](d26-telemetry.md)** (product telemetry: no phone-home). The engine and
CLI still collect and send **nothing** by default. D57 governs only anonymous, aggregate *web* analytics
on the marketing/docs sites — page visits by browsers, never product usage, queries, or data.

## How

- **Endpoint.** `an.growlerdb.com` (short, neutral subdomain — less blockable than an `analytics.*` host)
  resolves to the apex VM, which reverse-proxies the Plausible script and `/api/event` to a private,
  self-hosted Plausible instance. The Plausible **dashboard is not exposed publicly**.
- **Snippet.** Each site loads its own **per-site** Plausible script — `an.growlerdb.com/js/pa-<id>.js`
  plus the standard `plausible.init()` bootstrap. The site identity is baked into the script file (no
  `data-domain` attribute), and events post to the script's own origin (`/api/event`), so no `data-api`
  override is needed. The apex snippet is inline in `www/index.html`; the docs snippet lives in
  `docs/_includes/head_custom.html`. Engagement features (outbound links, file downloads, custom events)
  are enabled per site in the Plausible dashboard — the served script reflects them, no snippet change.
- **Real client IP.** The proxy **must** forward `X-Forwarded-For`; without it Plausible sees every
  visitor as the proxy IP (one visitor, no geography). Runbook + verification in
  [`www/README.md`](https://github.com/GrowlerDB/growlerdb/blob/main/www/README.md).

## Consequences

- No cookie/consent banner (GDPR/CCPA-friendly by design); a short privacy note still stated on-site.
- The snippet fails silent if the endpoint is down or blocked — no user-visible error, just no data — so
  it is safe to ship before/independently of the endpoint going live.
- Keep it first-party: never point the snippet at a `plausible.*` host or hot-link Plausible Cloud.

## Alternatives considered

- **Google Analytics 4** — free but privacy-hostile (needs a consent banner), heavy, and blocked by the
  target audience. Rejected.
- **Plausible Cloud / Fathom** — good tools, but hosted off our infra and served from hosts that are
  commonly blocked; the paid tiers don't buy blocker resistance. Rejected in favor of self-hosting.
