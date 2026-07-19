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

- **Search / Explore** — query, facets, sort/paging, highlighted results, hydrate a hit, export. A
  **Lexical / Semantic / Hybrid** mode toggle (shown when the index has a
  [vector field](/product/functional/search/vector.md); hybrid exposes the RRF `k`), a **"more like
  this"** action on a hit, and a **"vectorize a field"** step in create-index.
- **Ask** — a grounded-retrieval screen: a question is hybrid-retrieved and answered with the source
  **passages plus their exact Iceberg coordinates as citations**. There is intentionally **no answer
  generation** — GrowlerDB never sends text to an LLM ([D42](/system/decisions/d42-retrieval-first.md));
  the value is grounded retrieval an agent can build on.
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
