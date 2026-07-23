---
type: Interface
title: Console UI
description: The web console — a Svelte SPA over the Engine API, served by the engine itself.
tags: [interface, ui, console]
resource: /ui
timestamp: 2026-07-04T14:22:00
---

# Console UI

The **GrowlerDB console** — a Svelte single-page app (in `ui/`) served by the
[gateway](/system/runtime/components/gateway.md) at the same origin as the
[REST API](/product/interfaces/rest.md), of which it is a pure client. Implemented as the
[console-ui component](/system/runtime/components/console-ui.md).

## Screens

- **Search / Explore** — query, facets, sort/paging, highlighted results, hydrate a hit, export.
  **Semantic and hybrid retrieval live here, inline** — a **Lexical / Semantic / Hybrid** mode toggle
  (shown when the index has a [vector field](/product/functional/search/vector.md); hybrid exposes the
  RRF `k`), a natural-language placeholder in the vector modes, a one-time **"Try semantic"** invitation
  on a vector-capable index, a **"more like this"** action on a hit, and a **"vectorize a field"** step
  in create-index. There is **one search box that gets smarter**, not a separate retrieval screen.
- **The console's front door** is the deployment's default index (`GROWLERDB_DEFAULT_INDEX` →
  `/v1/config`; the demo points at `movies`, which has a vector field), so a fresh visitor lands where
  semantic/hybrid is one click away rather than on a lexical-only skeleton. Unset ⇒ the first index.
- **No separate "Ask" screen.** An earlier standalone grounded-retrieval ("Ask") screen was **retired**
  ([D42](/system/decisions/d42-retrieval-first.md) still holds — GrowlerDB never sends text to an LLM;
  retrieval returns passages + their exact Iceberg coordinates as citations). It was a redundant second
  door to what Search's Semantic/Hybrid modes already do, and the "Ask" label over a retrieval-only
  feature invited the wrong expectation. The value — passages with governed provenance — is delivered
  in Search results, not a separate tab.
- **Indexes** — list/create/alter/drop/reindex/compact/backup, aliases, per-index detail.
- **Ingestion** — per-index sync status, lag, streaming charts.
- **Observability** — SLI dashboards (latency, ingest lag, shards, cold-cache), alerts.
- **Settings** — theme/locale/keyboard, identity, roles.
- **Login gate** — shown in closed mode when unauthenticated.

## Notes

The console is themed by [Brand v1.0](/product/brand/index.md): its `ui/src/app.css` tokens take the
brand's dark-first neutral palette (glacier-light interactive accents, a melt topbar) and the Archivo /
Instrument Sans / Geist Mono trio — a re-skin, not a redesign (see
[brand identity](/product/brand/identity.md) for the token mapping).

Deploy-specific config (e.g. the Grafana link) is served at runtime via `/v1/config`, not baked in —
the SPA is built once and served by every deployment.
