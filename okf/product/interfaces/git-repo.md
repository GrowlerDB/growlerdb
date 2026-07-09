---
type: Interface
title: Git repository (project touchpoint)
description: How users engage the open-source project — issues, PRs, discussions, and releases.
tags: [interface, git, oss, releases]
resource: https://github.com/GrowlerDB/growlerdb
timestamp: 2026-07-04T14:22:00
---

# Git repository (project touchpoint)

The GitHub repository is how users and contributors engage the open-source project — distinct from
[`system/git-repo`](/system/git-repo.md), which describes the codebase layout.

## What you can do here

- **Consume releases** — signed multi-arch container images, the Helm chart (OCI), and release
  binaries with checksums, cut from SemVer tags.
- **Report issues** and request features via the **issue forms** (bug / feature), or open a
  **Discussion** for questions (see [how issues are handled](/quality/issues.md)). Security reports go
  privately through [GitHub Security Advisories](https://github.com/GrowlerDB/growlerdb/security/advisories/new).
- **Contribute** — open PRs following the [workflow](/workflow.md) (branch → gate → PR; every PR
  updates this OKF). `CODEOWNERS` routes reviews; a PR template encodes the checklist.

## Notes

Repository health is complete for public launch: LICENSE (Apache-2.0), CONTRIBUTING (DCO),
CODE_OF_CONDUCT, SECURITY, plus `CODEOWNERS`, issue forms + `config.yml`, a PR template, and
`dependabot.yml` (cargo / npm / actions / maven / docker). **Dependabot alerts + security updates,
secret scanning, and push protection** are enabled; branch protection guards `main`. Runner safety
for the public repo is [D34](/system/decisions/d34-runner-safety.md); the release pipeline is in
[system/build](/system/build.md).
