# Working on GrowlerDB

GrowlerDB is an open-source retrieval engine — full-text, vector & hybrid search over your data
(Apache Iceberg today; more sources on the roadmap). The compiled source of truth for what it
is and how it works is the **OKF** in [`okf/`](okf/index.md) — start there.

## Session flow

- **Feature-branch aggregation — one ask = one feature branch off `main` = one PR to `main`** (never
  commit to `main`). Incremental work (several tasks, and for a large feature its sub-PRs) accumulates
  on the feature branch; it lands on `main` as **one squash PR = the whole feature**, gate-green and
  OKF-updated. Sub-PRs go **into the feature branch** (`feat/<name>/<slice>` → `feat/<name>`) and the
  agent self-merges them once their gate is green — they are internal decomposition, not review gates.
  See [D50](okf/system/decisions/d50-branching-model.md).
- **The user reviews and merges the feature → `main` PR** before we continue — that single PR is where
  the whole feature is reviewed. Don't start dependent follow-on work on `main` until it's merged.
- **Before opening a PR, the full gate must pass:** `just check` (mirrors CI — Rust fmt/clippy/tests
  + the lint set; the console adds eslint/prettier/svelte-check).
- **Every PR updates the OKF** in the same PR — new/changed behavior, interface, or decision updates
  the relevant `okf/` concept (a new decision → an ADR under `okf/system/decisions/`). CI enforces
  the conformance check (`just okf-check`). See [`okf/workflow.md`](okf/workflow.md).

No commit trailer. Keep PRs small and honestly scoped.

## Code comments

Comments earn their place or they go. Applies to all languages (Rust, Java, Svelte/TS).

- **Concise:** most comments are ≤2 lines — targeted sentences, not paragraphs. Minimize visual clutter.
- **Why, not what:** the code says *what* it does; a comment explains *why* (a non-obvious rationale,
  invariant, trade-off, safety/correctness reason, or a decision ref like `D53`/`HA-G4`). If a comment
  only restates the code, delete it.
- **No obvious/translated comments:** don't narrate self-explanatory code (`// increment i`), and don't
  restate a name (`// Gets the username` on `username()` / a `getUsername` Javadoc).
- **Current code only:** comments describe how the code is *now*. Don't tell past stories — historical
  narratives, "originally/previously/used-to", old bug/ordering retellings, stale task IDs. Prune them.
- **Keep the load-bearing:** invariants, security/correctness rationale, decision refs, public-item doc
  contracts, and functional pragmas (`eslint-disable`, `svelte-ignore`, `@Override`) stay — trimmed to
  their essence, not deleted.

## Querying GrowlerDB data

When asked what a GrowlerDB **index** (`movies` — the default — `docs`, `catalog`, …) says or contains, use the
**growlerdb MCP tools** (`search`, `describe_index`, …) against the running demo stack — not file
search over this repo. `describe_index` first; prefer `mode: hybrid` when the index has vector
fields (lexical does no stemming).
