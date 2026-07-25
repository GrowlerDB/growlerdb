# Working on GrowlerDB

GrowlerDB is an open-source retrieval engine — full-text, vector & hybrid search over your data
(Apache Iceberg today; more sources on the roadmap). The compiled source of truth for what it
is and how it works is the **OKF** in [`okf/`](okf/index.md) — start there.

## Session flow

- **Every implementation change goes on a branch and into a PR** — never commit to `main`. A single
  PR may cover several tasks.
- **The user reviews and merges the PR** before we continue. Don't start dependent follow-on work on
  `main` until the PR is merged.
- **Before opening a PR, the full gate must pass:** `just check` (mirrors CI — Rust fmt/clippy/tests
  + the lint set; the console adds eslint/prettier/svelte-check).
- **Every PR updates the OKF** in the same PR — new/changed behavior, interface, or decision updates
  the relevant `okf/` concept (a new decision → an ADR under `okf/system/decisions/`). CI enforces
  the conformance check (`just okf-check`). See [`okf/workflow.md`](okf/workflow.md).

No commit trailer. Keep PRs small and honestly scoped.

## Querying GrowlerDB data

When asked what a GrowlerDB **index** (`movies` — the default — `docs`, `catalog`, …) says or contains, use the
**growlerdb MCP tools** (`search`, `describe_index`, …) against the running demo stack — not file
search over this repo. `describe_index` first; prefer `mode: hybrid` when the index has vector
fields (lexical does no stemming).
