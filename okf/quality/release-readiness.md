---
type: Process
title: Release readiness
description: The GA criteria, versioning, and release process that gate a release.
tags: [quality, release, ga, versioning, process]
timestamp: 2026-07-04T14:22:00
---

# Release readiness

How GrowlerDB decides it is ready to ship, and how it ships.

## GA criteria

A living **GA criteria checklist** tracks readiness across functionality, security, stability/ops,
performance, and release/docs — each item marked Met / Partial / Pending with the shipped evidence, so
the go/no-go is honest. It doubles as the pre-release checklist.

## Versioning & release

- **SemVer** ([D25](/system/decisions/d25-api-stability.md)) across the Engine API, wire protocol, and
  on-disk format, with deprecation windows.
- The release [pipeline](/system/build.md) auto-increments the patch and supports explicit major/minor
  bumps; a tag publishes a signed multi-arch image + SBOM, the Helm chart, and binaries.

## Notes

Remaining road-to-1.0 items (independent security review, the formal at-scale benchmark suite —
directional numbers are already published as a public Performance page) are tracked against the GA
criteria and surfaced in [known limitations](/quality/known-limitations/index.md).
