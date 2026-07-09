---
type: Process
title: Supply-chain gates
description: cargo-deny gates licenses, advisories, and bans in CI; releases produce an SBOM and cosign signatures; dependency and secret scanning run on the repository.
tags: [quality]
timestamp: 2026-07-04T14:22:00
---

# Supply-chain gates

cargo-deny gates licenses, advisories, and bans in CI; releases produce an SBOM and cosign signatures; dependency and secret scanning run on the repository.

**Dependabot alerts complement cargo-deny.** cargo-deny gates the **RUSTSEC** feed (Rust only) at CI
time; GitHub's Dependabot alerts cover the broader **GHSA** database across every ecosystem (Rust,
npm, Maven, Docker) and catch advisories RUSTSEC hasn't picked up (e.g. the `jsonwebtoken` and
transitive `thrift` GHSA advisories that a green cargo-deny did not flag). Both run; the pre-public
launch bumped the flagged deps (gRPC, `jsonwebtoken`, ECharts). Automatic Dependabot **version-update
PRs are paused** — alerts stay on, and fixes are applied deliberately.
