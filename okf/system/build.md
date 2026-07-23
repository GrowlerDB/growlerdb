---
type: Process
title: Build & release
description: The build toolchain, CI workflows, the gate, and the release pipeline.
tags: [system, build, ci, release, process]
timestamp: 2026-07-04T14:22:00
---

# Build & release

How GrowlerDB is built, gated, and released.

## Toolchain

- **mise** provisions toolchains (Rust, protoc, node, Maven); build/test via `mise exec -- cargo …`.
  **sccache** caches compilation on the runners.
- `just` recipes wrap common tasks (`just check` mirrors the CI gate; `just lint-all`).

## CI workflows (`.github/workflows/`)

- **`ci.yml`** (per PR + push to main) — a path-filtered `changes` job gates: `build-test` (fmt +
  clippy + workspace tests), `license-audit` (cargo-deny), `ui` (svelte-check + eslint + prettier +
  vitest + Playwright), `e2e` (walking-skeleton against the Compose stack), and `lint`
  (typos/shellcheck/actionlint/yamllint/markdownlint). See [CI & gates](/quality/ci-and-gates.md).
- **`nightly.yml`** — the full e2e suite + Spark integration.
- **`connector.yml`** — the JVM connector build (triggered on connector/proto changes).
- **`release.yml`** — tag-triggered publishing (below).

Runs on **self-hosted** runners.

## Release pipeline

Triggered by a `workflow_dispatch` (with a `bump: patch|minor|major`) **or** a pushed `v*` tag. It
runs the full gate, then publishes: a **signed multi-arch** (amd64+arm64) container image with an
**SBOM** (cosign keyless via OIDC), the **Helm chart** (OCI), and release **binaries + checksums**,
plus a GitHub Release. Runs on **GitHub-hosted** runners; both the container image and the standalone
binaries are built on **native per-arch runners** (amd64 + arm64, no QEMU, no `cross`) — the image
with a buildx layer cache merged into one manifest, the binaries with a plain `cargo build`. Native is
required so the local embedder links its native ONNX Runtime (`ort`) the same way the image does; a
`cross` container's older glibc can't, so the **binaries require glibc 2.38+** at runtime (the ONNX
prebuilt's floor, same as the image).

**Versioning** is **tag-derived** ([D29](/system/decisions/d29-release-versioning.md)): the git tag
is the source of truth, stamped into the image, the chart `appVersion`, the binaries, and the CLI
`--version` at build time, while the tree stays `0.0.0`. A dispatch computes the next version
(`scripts/next-version.sh` — auto-increment patch, explicit minor/major, `0.1.0` GA baseline) and
creates the tag *after* the gate passes, so a red gate leaves no orphan tag. The image carries an
immutable `X.Y.Z` plus moving `X.Y`/`X`/`latest`. See [RELEASING.md](https://github.com/GrowlerDB/growlerdb/blob/main/RELEASING.md).

## Notes

The release job publishes to GHCR with the workflow's `packages`/`id-token` permissions. Deployment
of the artifacts is [system/deployment](/system/deployment/index.md).
