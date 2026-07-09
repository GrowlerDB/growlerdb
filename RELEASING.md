# Releasing GrowlerDB

## Versioning (SemVer)

GrowlerDB follows [Semantic Versioning 2.0](https://semver.org). **The git tag (`vX.Y.Z`) is the
source of truth for a release version.** Release artifacts are *tag-derived*: the in-tree workspace
version (`[workspace.package] version` in `Cargo.toml`) stays `0.0.0`, and the tag is stamped into
the container image, the Helm chart `appVersion`, the release binaries, and the CLI `--version` at
build time (see [ADR D29](okf/system/decisions/d29-release-versioning.md)). A local build therefore
honestly reports `0.0.0` (an unreleased build).

- **MAJOR** — breaking changes to a stable public surface (the Engine API,
  index-definition schema, wire protocol, or CLI).
- **MINOR** — backward-compatible features.
- **PATCH** — backward-compatible fixes.

Pre-1.0 (`0.x`): minor versions may carry breaking changes, called out in the changelog. **The
initial GA line is `0.1.x`** — GA-quality but pre-1.0 (some capabilities are still validating; see
`okf/quality/known-limitations/`). The **Helm chart** carries its own SemVer (`version` in
`Chart.yaml`), bumped when the chart changes, independent of `appVersion`.

## Stability & deprecation policy

- The **stable surface** is the documented REST/gRPC Engine API, the index-definition YAML, and the
  CLI. The OpenSearch adapter is **best-effort/optional** (documented subset) and may change within
  a minor.
- **Index (on-disk) format:** an index is a *derived, rebuildable* store (Tantivy segments + the
  redb locator), not a system of record — the authoritative data lives in Iceberg. The format is
  versioned; a breaking format change is allowed on a **minor** pre-1.0 (a **major** post-1.0) and
  the migration is **rebuild/reindex from the source**, which is always available. We do not promise
  in-place on-disk migration.
- A feature is deprecated with a changelog `Deprecated` entry and, where possible, a runtime warning.
  Deprecated surface is supported for **≥ 1 minor release** (≥ 1 major, post-1.0) before removal.
- Breaking changes are listed under `Changed`/`Removed` in [CHANGELOG.md](CHANGELOG.md) with a
  migration note.

## Cutting a release

Prereq: **green `main`** — CI (`fmt`, `clippy`, tests, `cargo-deny`, lint, the Compose E2E) is
passing. `release.yml` re-runs the full gate before it publishes, and (for a dispatch) before it
creates the tag, so a red gate never leaves an orphan tag.

### Preferred: auto-computed version (`workflow_dispatch`)

Run the **release** workflow from the Actions tab (or `gh workflow run release.yml -f bump=patch`)
with a `bump`:

- `patch` (default) → the next patch from the last released tag, e.g. `v0.1.3 → v0.1.4`.
- `minor` → `v0.1.3 → v0.2.0`.
- `major` → `v0.1.3 → v1.0.0`.

The workflow gates, computes the version with [`scripts/next-version.sh`](scripts/next-version.sh),
**creates and pushes the `vX.Y.Z` tag**, then builds and publishes. **The first release ignores the
bump and produces `v0.1.0`** (the GA baseline). No files are hand-edited.

### Alternative: push a tag by hand

`git tag -s vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`. The tag push triggers `release.yml`,
which uses the tag verbatim (and still runs the full gate first). Use this when you want a signed
tag or a version that doesn't follow the auto-increment.

Then update [CHANGELOG.md](CHANGELOG.md) — move `Unreleased` to the new version + date and add the
compare link — and **verify** the workflow: signed multi-arch container on GHCR, SBOM attached, Helm
chart pushed (OCI), release binaries + checksums on the GitHub Release.

### First GA release (recreated repo)

The initial GA is cut on a **freshly recreated repo** (no git history, no tags). The tree carries no
version (`0.0.0`, tag-derived) — the initial commit is version-less, and `v0.1.0` comes from the
release workflow. One-time checklist:

1. **Repo is ready** — pushed under the `GrowlerDB` org, `main` green, still **private** until the
   go-public gates are done (security review, pre-public audit, community health, and **self-hosted
   runner hardening** — public repos let fork PRs run on the runners).
2. **Re-grant GHCR package access** — a fresh repo re-links the server image (`ghcr.io/growlerdb/growlerdb`,
   auto by name) but **`growlerdb-connector` and `growlerdb-seed` need the repo granted Write again**
   (org → Packages → each package → *Manage Actions access* → add this repo, role Write), or the
   `scale-images` workflow's push 403s.
3. **Clear the old `0.1.x` dev images** from the org GHCR (they persist across the recreate and would
   collide with a fresh `v0.1.0`): org → Packages → `growlerdb` → delete the `0.1.1`…`0.1.8` versions,
   so `0.1.0` is the first *public* image. (Or, to keep them, set `INITIAL_VERSION=0.2.0` and start the
   public line there — but update the ADR/docs baseline accordingly.)
4. **Cut it** — `gh workflow run release.yml -f bump=patch`. With no tags, the workflow ignores the bump
   and produces **`v0.1.0`** (the [ADR D29](okf/system/decisions/d29-release-versioning.md) GA baseline).
5. **Date the changelog** — move `[Unreleased]` → `[0.1.0] - <date>`.
6. **Verify** — `docker buildx imagetools inspect ghcr.io/growlerdb/growlerdb:0.1.0` shows a
   linux/amd64 + linux/arm64 manifest with cosign signature + SBOM; the Helm chart is on GHCR (OCI);
   binaries + `.sha256` are on the GitHub Release.
7. **Then** enable GitHub Pages (docs site) and go public → announce.

## What the release workflow produces

`.github/workflows/release.yml` (on `v*` tags **or** `workflow_dispatch`):

- **Container image** → `ghcr.io/growlerdb/growlerdb`, multi-arch (amd64+arm64), built from
  `deploy/Dockerfile`, tagged with the immutable **`X.Y.Z`** plus the moving **`X.Y`**, **`X`**, and
  **`latest`** — pin exactly or float, as you prefer.
- **SBOM** (CycloneDX, via Syft) for the image, attached to the release and as a cosign attestation.
- **Cosign signatures** (keyless / GitHub OIDC) for the image and SBOM.
- **Helm chart** packaged with `--app-version X.Y.Z` (chart `version` from `Chart.yaml`) and pushed
  to `oci://ghcr.io/growlerdb/charts`.
- **Release binaries** (`growlerdb`) for linux x86_64/arm64, each with a `.sha256`, attached to the
  GitHub Release.

Client libraries (PyPI / crates.io) publish from their own subtrees once their versions are bumped;
add those jobs as the client packages stabilize.

## Verifying a release (consumers)

```sh
cosign verify ghcr.io/growlerdb/growlerdb:X.Y.Z \
  --certificate-identity-regexp 'https://github.com/GrowlerDB/growlerdb/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
