# Contributing to GrowlerDB

Thanks for your interest in GrowlerDB! Contributions are welcome.

## Contributor License Agreement (CLA)

GrowlerDB uses a **license-grant CLA** (adapted from the Apache ICLA — see [`CLA.md`](CLA.md)).
The first time you open a pull request, the CLA-assistant bot comments with a one-time signing
link; your PR can merge once you've signed.

**Why a CLA, and why this one.** GrowlerDB is open source under **AGPL-3.0** (see [`LICENSE`](LICENSE)).
The CLA lets **GrowlerDB LLC** keep the project sustainable by also offering commercial/OEM licenses.
It is a **license grant, not an assignment — you keep the copyright to your contribution.** GrowlerDB
LLC commits to keeping the core AGPL-3.0 open source.

## Dev setup

The toolchain is pinned with [mise](https://mise.jdx.dev):

```sh
mise install        # installs the Rust toolchain from mise.toml
just setup          # rustfmt + clippy components
just check          # fmt + clippy + tests — run this before opening a PR
```

`just up` / `just down` start and stop the local dev dependencies (MinIO + Polaris).

## Pull requests

GrowlerDB uses **feature-branch aggregation** ([D50](okf/system/decisions/d50-branching-model.md)):
**one feature = one branch off `main` = one PR to `main`**, squash-merged. Decompose a large feature
into sub-branches off the feature branch (`feat/<name>/<slice>` → `feat/<name>`) and merge those into
the feature branch; open the PR to `main` only when the whole feature is complete. `main` is
protected (PR + `ci-gate` check + linear history required); releases are cut on demand from tags, so
there is no `develop`/`release`/`hotfix` branch to target.

- Keep the feature focused and reviewable; reference the backlog task where relevant.
- `just check` must pass before the PR to `main` (CI enforces fmt, clippy `-D warnings`, tests, and a
  license-audit gate via `cargo-deny`).
- **Update the OKF.** Any change to behavior, interfaces, components, dependencies,
  decisions, or process must update the relevant concept(s) under [`okf/`](okf/) in
  the same PR — the OKF is the living source of truth. See [`okf/workflow.md`](okf/workflow.md).
  `just okf-check` (also in CI) verifies OKF conformance.
- Substantial changes (public APIs, on-disk format, cross-component contracts) go
  through an RFC first.

## Design & knowledge

Compiled knowledge about GrowlerDB — architecture, decisions (ADRs), and process —
lives in the [`okf/`](okf/) knowledge base (Open Knowledge Format), the living source
of truth. If a change contradicts what's documented there, update the OKF too — don't
silently diverge.

Anything touching a user-facing surface (console, website, docs, social) should follow the
**brand** — see [`BRAND.md`](BRAND.md) (logo, palette, type, voice/terminology), a companion to
[`okf/product/brand/`](okf/product/brand/index.md).
