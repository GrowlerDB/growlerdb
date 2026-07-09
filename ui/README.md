# GrowlerDB console (UI)

A **Svelte SPA** (Vite + TypeScript) that is a _pure client_ of the Engine API — the human
surface over the same gRPC/REST API programmatic callers use (wiki/20-ui). It never reaches the
Index or storage directly. This is the **scaffold** (task-45); the four screens (Search,
Indexes, Observability, Connectors) are built out in tasks 46–49.

## Layout

- `src/App.svelte` — nav shell (skip link, keyboard-navigable nav, `main` landmark).
- `src/routes/` — screen components (`Search` is minimal-functional; the rest are placeholders).
- `src/lib/auth.ts` — OIDC **authorization-code + PKCE** login; forwards the bearer token.
- `src/lib/api.ts` — Engine API client; attaches `Authorization: Bearer <token>`.
- `src/lib/i18n.ts` + `src/lib/locales/` — message catalog + `t()`; **no hardcoded strings**.
- `src/lib/router.ts` — tiny path router (the Engine serves `index.html` as the SPA fallback).
- `src/lib/config.ts` — optional OIDC config from `VITE_OIDC_*` or `window.__GROWLERDB_CONFIG__`.

## Develop

```sh
just ui-install   # once
just ui-dev       # Vite dev server (HMR)
just ui-check     # svelte-check + vitest
just ui-build     # production build → dist/
```

## Test

Two layers, both run in CI (the `ui` job):

```sh
npm run check     # svelte-check (types + a11y)
npm test          # vitest unit tests for the pure lib/ modules
npm run e2e        # Playwright screen-level E2E (task-92)
```

The **E2E** lives in `e2e/` and is **fully mocked at the network layer** (`e2e/mocks.ts`
intercepts `**/v1/**`), so it needs **no live Engine or stack** — fast and deterministic. It
builds + previews the real production bundle (see `playwright.config.ts`) and covers the primary
flows (search → hydrate, create-index-from-introspection, ingestion, observability) plus the
empty / error / partial-results states.

First run, install the browser once: `npx playwright install chromium` (CI uses
`--with-deps`). To debug: `npm run e2e -- --headed` or `--ui`.

## Served by the Engine

The Engine binary serves the built SPA: `growlerdb serve … --ui-dir ui/dist` (or
`--ui-dir` on `gateway`, or `GROWLERDB_UI_DIR`). Static assets are served directly and any
non-`/v1` path falls back to `index.html` (client-side routing); the `/v1` API takes precedence.

## Config

- `VITE_ENGINE_API` — Engine API base URL (default empty = same origin; the Engine serves the UI).
- `VITE_OIDC_ISSUER` / `VITE_OIDC_CLIENT_ID` / `VITE_OIDC_REDIRECT_URI` / `VITE_OIDC_SCOPE` — OIDC.
  With no issuer the UI runs against an open Engine (mirrors the gateway, open until `--oidc-issuer`).
