---
type: Process
title: CI & gates
description: The continuous-integration gates every change must pass.
tags: [quality, ci, gates, lint, process]
timestamp: 2026-07-04T14:22:00
---

# CI & gates

The automated gates a change passes before merge — the quality process encoded in
[CI](/system/build.md).

## The gate (every PR)

- **Rust** — `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`,
  `cargo test --workspace`.
- **Lint** — typos, shellcheck, actionlint, yamllint, markdownlint (repo-wide); the console adds
  eslint + prettier + `svelte-check`.
- **UI** — eslint, prettier, svelte-check, unit tests (all four also in local `just check` via
  `ui-check`, task-218 fix — the recipe used to skip eslint/prettier, letting CI catch what the
  local gate missed), plus build + mocked Playwright E2E (CI-only: they need the browser toolchain).
- **E2E** — the walking-skeleton (index → search → hydrate) against the real Compose stack.
- **License/supply-chain** — cargo-deny (licenses, advisories, bans).

## How it runs

Jobs are **path-filtered** (a docs-only PR doesn't compile Rust; the lint job runs unconditionally).
Runners split by trust ([D34](/system/decisions/d34-runner-safety.md)): **`pull_request` runs on
GitHub-hosted** runners, while **`push` (main) and nightly run on the home-lab self-hosted** runners
(sccache, persistent disk) — so untrusted fork-PR code never touches the home lab, and an org policy
requires maintainer approval for any outside collaborator's run. The full E2E + Spark integration run
nightly. See [CLI](/system/build.md) for `just check` (the local mirror).

## Notes

Every PR must also update the [OKF](/workflow.md) — enforced by the PR template (and an OKF conformance
check).
