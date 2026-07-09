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
- **Report issues** and request features (see [how issues are handled](/quality/issues.md)).
- **Contribute** — open PRs following the [workflow](/workflow.md) (branch → gate → PR; every PR
  updates this OKF).
- **Get help / discuss** — the project's discussion and support channels.

## Notes

Repository health files (LICENSE, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY) and templates make the
on-ramp clear; the release pipeline is in [system/build](/system/build.md).
