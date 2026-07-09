---
type: Use Case
title: Search-backed paginated application
description: A user-facing search box over a paginated, sortable results table backed by Iceberg data.
tags: [use-case, app, pagination]
timestamp: 2026-07-04T14:22:00
---

# Search-backed paginated application

**Persona.** An [application/product developer](/product/actors/app-developer.md) adding a user-facing
search feature over data already in Iceberg — a catalog, directory, document library, admin console,
or records browser.

**Context.** The primary records live in Iceberg. The UI is the universal pattern: a **search box over
a paginated results table** — type a query, see a page of ranked rows, **sort by a column**, **page
forward**, and **click a row** to open the full record — and it should feel instant without standing
up a second search store and sync pipeline beside the lakehouse.

**How GrowlerDB is used.**

- Connect the Iceberg table; choose indexed fields; specify partition fields (e.g. `org_id`) for
  routing + pruning.
- Mark result-table columns (`name`, `updated_at`, `status`, `price`) as **cached** *and* **fast**:
  cached returns each value **with the hit** so a page renders with no hydration; fast makes the column
  **sortable**. See the [data model](/system/storage/data-model.md).
- Query → ranked **coordinates + cached fields** paint the page; **sort** by any fast field (or
  `_score`); [deep pagination uses search-after](/product/functional/search/sort-paging.md) on the
  score/key cursor — the composite key is the stable tiebreaker for a deterministic total order. A row
  click [hydrates](/product/functional/hydration.md) the authoritative record.

**Why it fits.** One governed store — no second index to provision or sync; the result list renders
and sorts from cached+fast columns without touching Iceberg; stable pagination comes free from the
composite key; click-through pulls the authoritative row. Iceberg stays the source of truth; GrowlerDB
is a derived, rebuildable index.

**Requirements exercised.** Cached display + fast fields for sort · sorting & pagination · low-latency
top-K · governed hydration on row-open.
