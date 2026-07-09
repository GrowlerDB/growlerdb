---
type: Decision
title: D29. Release versioning: tag-derived, auto-incremented
description: The git tag is the source of truth for a release version; artifacts are stamped from it while the tree stays 0.0.0; releases auto-increment the patch with explicit minor/major bumps, starting at a 0.1.0 GA baseline.
tags: [decision, adr, release, versioning]
timestamp: 2026-07-04T14:22:00
resource: https://github.com/GrowlerDB/growlerdb/blob/main/RELEASING.md
---

# D29. Release versioning: tag-derived, auto-incremented

**Decision.** The **git tag (`vX.Y.Z`) is the source of truth** for a release version. Release
artifacts are *tag-derived* — the tag is stamped into the container image, the Helm chart
`appVersion`, the release binaries, and the CLI `--version` at build time — while the in-tree
workspace version stays `0.0.0` (no version-bump commit to `main`). Releases **auto-increment the
patch** from the last tag, with **explicit `minor`/`major`** bumps via `workflow_dispatch`; the
**initial GA baseline is `0.1.0`** (GA-quality, pre-1.0). Refines the SemVer policy in
[D25](/system/decisions/d25-api-stability.md); mechanics live in
[build & release](/system/build.md).

**Why.** Keeping the tree at `0.0.0` avoids a release-commit dance and honors "never commit to
`main` directly"; a local `--version` honestly reads `0.0.0`. `0.1.0` (not `1.0.0`) matches the
pre-1.0 stability promise and the open [known limitations](/quality/known-limitations/index.md). The
index format is a rebuildable derived store, so its compatibility promise is "reindex from source"
rather than in-place migration.

**Status.** Accepted.
