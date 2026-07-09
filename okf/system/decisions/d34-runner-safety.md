---
type: Decision
title: D34. Runner safety for a public repo — hosted PR CI + approval gate
description: On going public, untrusted fork-PR code never runs on the home-lab self-hosted runners — an org policy requires approval for all outside collaborators, and pull_request CI runs on disposable GitHub-hosted runners while self-hosted is reserved for push(main) and nightly; CI tokens are least-privilege.
tags: [decision, adr, ci, security, ops, runners]
timestamp: 2026-07-04T14:22:00
---

# D34. Runner safety for a public repo — hosted PR CI + approval gate

**Decision.** A public repo must never execute **untrusted fork-PR code on the self-hosted
(home-lab) runners**. Two independent layers enforce this (task-164):

1. **Approval gate.** The org Actions policy requires approval for **all outside collaborators**
   (`fork-pr-contributor-approval = all_external_contributors`), so no fork PR's workflow runs
   until a maintainer reviews the diff and clicks *Approve and run*. This is set org-wide, not
   per-repo (repo-level fork-PR approval isn't settable while private).
2. **Hosted PR CI.** `pull_request` jobs run on **GitHub-hosted** runners
   (`runs-on: ${{ github.event_name == 'pull_request' && 'ubuntu-latest' || 'self-hosted' }}`);
   the **self-hosted** runners are reserved for `push` (`main`) and `nightly` — trusted, post-merge
   work. So even an *approved* fork PR executes on disposable infra, never the home lab.

CI `GITHUB_TOKEN` is also **least-privilege** (`permissions: contents: read` on `ci.yml`,
`connector.yml`, `nightly.yml`); the `changes` path-gate already diffs with `git` rather than the
API for the same reason.

**Why.** Public + unguarded self-hosted runners = **arbitrary code execution on the home lab**
(the runners at `192.168.68.101-103`) from any fork PR — it inverts the private-repo trust
assumption the whole self-hosted model relied on. The approval gate closes *auto*-execution; the
hosted split is defense-in-depth so a maintainer approving a plausible-looking PR still can't hand
the home lab to attacker-controlled code. GitHub-hosted minutes are **free for public repos**, so
the split has no marginal cost.

**Consequences.** PR CI loses the self-hosted persistent sccache/disk cache, so cold PR builds are
slower — `Swatinem/rust-cache` (GHA cache backend) mitigates, and the heavy full-suite/Spark work
stays nightly on self-hosted. `push`/`nightly` keep the fast cached self-hosted path. Trusted
internal PRs (org members) also run hosted — a uniform per-event rule rather than fork-detection
branching, at the cost of not using the home lab for insiders' PRs. The required `ci-gate` status
check is unaffected (the job name is stable across runners). **Follow-ups (not blockers):**
re-register the self-hosted runners as **`--ephemeral`** (a poisoned job can't taint the next), pin
third-party actions to SHAs, and tighten org `allowed_actions` to a `selected` allowlist.

**Status.** Accepted (task-164). Approval policy applied org-wide; `ci.yml` and `connector.yml`
carry the per-event runner split; `nightly.yml` stays self-hosted (schedule/dispatch only — no fork
PRs reach it). Precedes going public + Pages ([task-164](/workflow.md)).
