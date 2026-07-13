---
type: Decision
title: D34. Runner safety for a public repo — all CI on GitHub-hosted runners
description: The public repo runs all CI/CD (pull_request, push/main, nightly, connector, release) on GitHub-hosted runners — free for public repos and with no owned machine in the blast radius. The org fork-PR approval gate and least-privilege CI tokens remain as defense-in-depth.
tags: [decision, adr, ci, security, ops, runners]
timestamp: 2026-07-04T14:22:00
---

# D34. Runner safety for a public repo — all CI on GitHub-hosted runners

**Decision.** All of the public repo's CI/CD runs on **GitHub-hosted runners** (`ubuntu-latest`) —
`pull_request`, `push` (`main`), `nightly`, the `connector` build, and `release`. **No self-hosted
runner is in the blast radius**, so untrusted code — whether a fork PR or a poisoned dependency
pulled during a build — can never execute on a machine we own. Two further layers remain:

1. **Approval gate.** The org Actions policy requires approval for **all outside collaborators**
   (`fork-pr-contributor-approval = all_external_contributors`), so no fork PR's workflow runs until
   a maintainer reviews the diff and clicks *Approve and run*.
2. **Least-privilege tokens.** CI `GITHUB_TOKEN` is `permissions: contents: read` on `ci.yml`,
   `connector.yml`, and `nightly.yml`; the `changes` path-gate diffs with `git` rather than the API
   for the same reason; fork PRs receive no repo secrets and no write token.

**Why.** Public + self-hosted runners = **arbitrary code execution on hardware we own**, which
inverts the private-repo trust assumption the self-hosted model relied on. An earlier version of
this decision kept a hosted/self-hosted *split* (fork PRs hosted, trusted `push`/`nightly`
self-hosted) to keep using the home-lab mini-PCs. Going public removed the reason for the split
on both sides: (a) the org self-hosted **runner groups disallow public repositories**
(`allows_public_repositories = false`), so a public repo's `self-hosted` jobs simply queue forever
against idle runners; and (b) **GitHub-hosted minutes are free and unlimited for public repos**, so
there is no cost saving to protect. Going fully hosted is therefore both the unblock *and* strictly
safer — it deletes the home-lab attack surface rather than fencing it.

**Consequences.** CI loses the self-hosted persistent sccache/disk cache, so cold builds are slower;
`Swatinem/rust-cache` (GHA cache backend) mitigates, and `CARGO_INCREMENTAL=0` / no-debuginfo keep
the hosted runners within disk/memory limits. The self-hosted runners are **no longer used by this
repo** (they remain available for private org repos). The required `ci-gate` status check is
unaffected. Prior follow-ups about hardening the self-hosted runners (`--ephemeral`, etc.) are moot
for this repo; pinning third-party actions to SHAs and tightening org `allowed_actions` still apply.

**Status.** Accepted. `ci.yml`, `connector.yml`, and `nightly.yml` run on `ubuntu-latest`;
`release.yml` was already hosted. Supersedes the earlier hosted/self-hosted split (applied when the
repo went public and its jobs could no longer reach the org self-hosted runner group).
