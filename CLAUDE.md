# Working on GrowlerDB

GrowlerDB is open-source text search over Apache Iceberg. The compiled source of truth for what it
is and how it works is the **OKF** in [`okf/`](okf/index.md) — start there.

## Session flow

- **Backlog** lives in a sibling `backlog/` directory outside this repo. Work items are its tasks.
- **Every implementation change goes on a branch and into a PR** — never commit to `main`. A single
  PR may cover several tasks.
- **The user reviews and merges the PR** before we continue. Don't start dependent follow-on work on
  `main` until the PR is merged.
- **Before opening a PR, the full gate must pass:** `just check` (mirrors CI — Rust fmt/clippy/tests
  + the lint set; the console adds eslint/prettier/svelte-check).
- **Every PR updates the OKF** in the same PR — new/changed behavior, interface, or decision updates
  the relevant `okf/` concept (a new decision → an ADR under `okf/system/decisions/`). CI enforces
  the conformance check (`just okf-check`). See [`okf/workflow.md`](okf/workflow.md).

Commit trailer: `Co-Authored-By: …`. Keep PRs small and honestly scoped.
