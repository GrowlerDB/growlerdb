# Contributing to GrowlerDB

Thanks for your interest in GrowlerDB! Contributions are welcome.

## Developer Certificate of Origin (DCO)

GrowlerDB uses the [DCO](https://developercertificate.org/) — **not** a CLA. Sign off
every commit to certify you wrote it (or have the right to submit it):

```sh
git commit -s -m "your message"
```

This appends a `Signed-off-by:` trailer with your name and email.

## Dev setup

The toolchain is pinned with [mise](https://mise.jdx.dev):

```sh
mise install        # installs the Rust toolchain from mise.toml
just setup          # rustfmt + clippy components
just check          # fmt + clippy + tests — run this before opening a PR
```

`just up` / `just down` start and stop the local dev dependencies (MinIO + Polaris).

## Pull requests

- Keep changes focused and reviewable; reference the backlog task where relevant.
- `just check` must pass (CI enforces fmt, clippy `-D warnings`, tests, and a
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
