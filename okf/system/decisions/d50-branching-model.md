---
type: Decision
title: D50. Branching model — feature-branch aggregation, one whole feature per PR to main
description: One ask is one feature branch off main; incremental work (tasks, agent-merged sub-PRs) accumulates on the feature branch, which lands on main as a single squash PR — the whole feature, gate-green and OKF-updated — so main only ever sees complete, reviewed features. No permanent develop, no release/hotfix branches; releases stay tag-from-main.
tags: [decision, adr, process, git, branching, release]
timestamp: 2026-07-25T00:00:00
resource: https://github.com/GrowlerDB/growlerdb/blob/main/okf/workflow.md
---

# D50. Branching model — feature-branch aggregation

**Decision.** GrowlerDB uses **feature-branch aggregation**: **one ask = one feature branch off
`main` = one PR to `main`.** All incremental work for that ask — several backlog tasks, several
commits, and for large features a set of **sub-PRs merged into the feature branch** — accumulates on
the feature branch, never on `main`. When the feature is complete (full gate green, OKF updated), the
feature branch lands on `main` as a **single squash merge** that represents the whole feature. `main`
therefore only ever sees complete, quality-guaranteed features, reviewed as a unit.

Concretely:

- **Feature branch per ask.** Branch off `main` with the usual type prefix — `feat/<name>`,
  `fix/<name>`, `chore/<name>`, `docs/<name>`, `ci/<name>`. A small ask is a single branch with a
  handful of commits and one PR — identical to before. A large ask that previously fragmented into
  many `main` PRs now fragments *inside* its feature branch.
- **Sub-PRs stay off `main`.** For a large feature, incremental slices are sub-branches off the
  feature branch (e.g. `feat/variant/introspection` → `feat/variant`), opened as PRs **into the
  feature branch**. The implementing agent **self-merges a sub-PR once its gate is green** — sub-PRs
  are internal decomposition, not review gates. The maintainer reviews the **feature → `main`** PR
  only, seeing the entire feature at once.
- **Squash into `main`.** The feature → `main` PR is **squash-merged**: one clean commit per feature
  on `main`; the slice-by-slice history lives in the feature-branch PR. `main` keeps a **linear
  history**, one commit per shipped feature.
- **Gate at the `main` boundary.** The full CI gate (the `ci-gate` required check) runs on the
  feature → `main` PR, so `main` stays green feature-by-feature. Sub-PRs may run the same gate on the
  feature branch, but only the `main` boundary is authoritative.
- **Parallel features are independent branches** off `main`, so they don't serialize on each other
  (the reason the shared `log.md` was retired — see [workflow](/workflow.md)).

**No permanent `develop`, no `release/*`/`hotfix/*` branches.** Releases are already **tag-derived**
and cut on demand from `main` ([D29](/system/decisions/d29-release-versioning.md)): `main` need only
be green, not "a release," per commit. A permanent `develop` would duplicate `main` without adding a
gate that the tag-time re-run of the full gate doesn't already provide; `release/*` and `hotfix/*`
branches stabilize and patch a running production that doesn't exist pre-GA (zero live instances).
Full git flow's ceremony would be idle machinery, so it is explicitly **not** adopted.

**Enforcement.** `main` is protected: **require a PR, require the `ci-gate` status check, require
linear history** (squash-only), and no direct pushes. The target ruleset is documented in
[workflow](/workflow.md); applying it is a repo-admin (maintainer) action.

**Why.** Agents decompose one ask into many slices; PRing each slice straight to `main` made `main`
pass through partial-feature states and forced the maintainer to review slices rather than features.
Aggregating the slices on a feature branch and squashing the whole feature into `main` makes every
PR to `main` a complete, reviewable, gate-green unit — quality and confidence per merge — while
keeping the cheap internal decomposition agents are good at. It is the lightest model that delivers
"a PR to `main` is the entire feature," and it composes with the existing tag-derived release path
([D29](/system/decisions/d29-release-versioning.md)) and public-repo runner safety
([D34](/system/decisions/d34-runner-safety.md)) unchanged.

**Status.** Accepted.
