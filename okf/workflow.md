---
type: Process
title: Workflow & OKF conventions
description: How GrowlerDB is developed — contribution, the gate, and the rule that every PR updates this OKF — plus the OKF authoring conventions.
tags: [workflow, process, contributing, okf]
timestamp: 2026-07-04T14:22:00
---

# Workflow & OKF conventions

How work ships in GrowlerDB, and how this knowledge base is kept current. An agent or contributor
reading this should be able to make a change correctly and leave the OKF up to date.

## Contribution & PR process

GrowlerDB uses **feature-branch aggregation** — **one ask = one feature branch off `main` = one PR
to `main`** ([D50](/system/decisions/d50-branching-model.md)). Incremental work accumulates on the
feature branch; `main` only ever sees complete, gate-green, reviewed features.

- **Feature branch per ask.** Branch off `main` with the usual type prefix (`feat/…`, `fix/…`,
  `chore/…`, `docs/…`, `ci/…`). A small ask is one branch, a few commits, one PR — as before. A large
  ask that would once have fragmented into many `main` PRs now fragments *inside* its feature branch.
- **Sub-PRs stay off `main`.** For a large feature, slice it into sub-branches off the feature branch
  (`feat/<name>/<slice>` → `feat/<name>`), PR'd **into the feature branch**; the implementing agent
  **self-merges each sub-PR once its gate is green**. Sub-PRs are internal decomposition, not review
  gates — the maintainer reviews the **feature → `main`** PR only, seeing the whole feature at once.
- **Never commit to `main` directly.** The feature → `main` PR is **squash-merged** (one clean commit
  per feature; `main` keeps a **linear history**), then reviewed and merged.
- **The full gate must pass before the feature → `main` PR** (`just check` mirrors CI):
  - Rust: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace`.
  - Lint gates (every PR): typos, shellcheck, actionlint, yamllint, markdownlint; the console adds
    eslint + prettier + `svelte-check`. See [CI & gates](/quality/ci-and-gates.md).
- **Commit trailer:** `Co-Authored-By: …`. **PR body trailer:** the generated-with note. Keep the
  feature honestly scoped and each internal slice self-contained.
- Small, honest slices keep review tractable and the test suite meaningful as a behavior oracle;
  aggregating them on the feature branch keeps every PR to `main` a complete, confident unit.

**`main` is protected** ([D50](/system/decisions/d50-branching-model.md)): require a PR, require the
`ci-gate` status check, require **linear history** (squash-only), no direct pushes. Releases are
unaffected — they stay **tag-derived and cut on demand from `main`**
([D29](/system/decisions/d29-release-versioning.md)); there is no permanent `develop` and no
`release/*`/`hotfix/*` branch. Repo admins apply the ruleset:

```sh
gh api -X PUT repos/GrowlerDB/growlerdb/branches/main/protection \
  -f 'required_status_checks[strict]=true' \
  -f 'required_status_checks[contexts][]=ci-gate' \
  -F 'enforce_admins=true' \
  -F 'required_pull_request_reviews[required_approving_review_count]=1' \
  -f 'restrictions=' -F 'required_linear_history=true' -F 'allow_force_pushes=false'
```

## Every PR updates the OKF

**This OKF is the living source of truth for compiled knowledge about GrowlerDB.** Any PR that changes
behavior, structure, interfaces, dependencies, or process **must update the relevant OKF concept(s)**
in the same PR — new capability → new/updated concept; changed component → update its concept and
links; new decision → an ADR under [`/system/decisions/`](/system/decisions/index.md). A change that
leaves the OKF stale is incomplete.

The **dated update history is the git log** (PR titles + commits) — *not* a shared in-repo log file.
An append-only `log.md` was retired because every PR appended to it, forcing parallel PRs into serial
rebase-and-rebuild; the pre-GA development narrative is archived in the sibling `backlog/`.

**Enforcement.** A [PR template](https://github.com/GrowlerDB/growlerdb/blob/main/.github/PULL_REQUEST_TEMPLATE.md)
carries an "OKF updated?" checklist item, `CONTRIBUTING.md` states the rule, and CI runs an **OKF
conformance check** (`okf/check.sh` / `just okf-check` — every concept carries a non-empty `type`).
Whether a change *should* have updated a concept remains a review judgment; treat the rule as binding.

## OKF authoring conventions

This bundle follows the [Open Knowledge Format](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md).

- **Every concept** is one markdown file with YAML frontmatter carrying a non-empty **`type`**.
  Recommended fields: `title`, `description` (one sentence), `tags`, `timestamp` (ISO 8601). Add
  `resource` (a canonical URI) where one exists.
- **`type` vocabulary** (keep the graph coherent): `Concept`, `Interface`, `Actor`, `Use Case`,
  `Feature`, `Requirement`, `Component`, `Dependency`, `Decision`, `Test Suite`, `Quality Attribute`,
  `Glossary`, `Process`.
- **Reserved files** (no frontmatter): `index.md` (a curated listing of a directory's concepts).
- **Links** use **absolute bundle-relative** paths (e.g. `/system/runtime/node.md`) so they survive
  moves. A link asserts a relationship; the kind is conveyed by the surrounding prose. Link a feature
  to the component that implements it and to the decision that shaped it — the cross-links are what
  make this a catalog rather than folders of prose.
- **Scope: strictly GrowlerDB.** No comparisons to or assessments of other products. Competitive and
  market analysis lives outside this bundle.
- **Structure:** a capability with sub-parts is a directory (with an `index.md`); a leaf is a single
  concept. See [overview](/overview.md) for the top-level map.
