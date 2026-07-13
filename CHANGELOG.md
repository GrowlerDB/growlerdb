# Changelog

All notable changes to GrowlerDB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) (see [RELEASING.md](RELEASING.md)).

## [Unreleased]

## [0.2.0] - 2026-07-12

The **public-launch** release — multi-index querying, server-side highlighting, an authenticated demo
(with Trino to explore and compare against Iceberg), enterprise-license visibility, and a hardened
control and data plane.

### Added

- Multi-index querying from a single Gateway endpoint, with per-index RBAC.
- Server-side highlighting — analyzed match fragments returned with hits.
- Enterprise-license visibility: `/v1/license` endpoint + a console **Settings → Enterprise license**
  card (licensee, nodes in use vs. limit, Free/Enterprise badge).
- Control-plane service-credential auth + optional mTLS; the demo mesh is closed by default.
- Console: inline cached fields on the hit row, degenerate facets hidden, aligned results table.
- Demo & getting-started: authenticated login with per-index user scopes; a rich catalog demo index
  with a query playground; **Trino** in `just stack` to explore the Iceberg tables and compare results.

### Changed

- **Reusable gateway assembly:** the CLI's gateway wiring is now an injectable library API
  (`growlerdb_cli::gateway`) with public authenticator seams, so out-of-tree auth can attach without
  forking. The default build stays 100% AGPL. (ADR D37)
- **Open-source scale line:** the core runs free up to a node cap; beyond it, the control plane admits
  new nodes only with an offline-verified Enterprise license — existing nodes and data are never
  disrupted. Cold-tier / object-storage-served storage stays open source; scale is the gate, not code.
  (ADR D38)
- Relicensed the core from Apache-2.0 to **AGPL-3.0-only** (see [LICENSE](LICENSE)); a
  [commercial license](COMM-LICENSE.md) is available for embedding/OEM, AGPL-incompatible use, and the
  enterprise add-ons. Contributions move to a license-grant [CLA](CLA.md) (replaces the DCO). (ADR D36)

### Fixed

- Query correctness: BOOL term handling, ISO date-range bounds, and field-grouped `OR` sets.
- Console: send the index when hydrating a row (fixes a multi-index `400`).
- Observability: node-catalog is scraped so catalog-index SLIs populate.
- Getting-started / demo seed polish and the README try-it search example.

### Security

- Hardened the public data plane: caller-asserted identity headers are dropped at the trust boundary;
  `keys:get`, aggregation-cardinality, and highlight/body sizes are capped; per-shard query timeouts.
- Hardened the supply chain for public release: RUSTSEC/advisory gating in CI, SHA-pinned Actions,
  non-root and digest-pinned container images.
- The Python client no longer sends self-asserted identity headers.
- Dependency security bumps (grouped: Rust, Maven, npm, GitHub Actions).

### Docs

- Documentation is now served at <https://docs.growlerdb.com>.
- Added a README architecture diagram, the commercial/OEM license terms, the trademark + governance
  policy, and a repository social-preview card.

## [0.1.1] - 2026-07-09

### Security

- Dependency security bumps ahead of the first public release, surfaced by Dependabot alerts:
  gRPC `1.75.0` (Netty "MadeYouReset" HTTP/2 DoS — high), `jsonwebtoken` `10` (type-confusion
  authorization-bypass advisory; the pure-Rust `rust_crypto` provider is selected explicitly), and
  ECharts `6.1` (console XSS advisory). A medium transitive `thrift` advisory (via `parquet`, in the
  own-data metadata-parse path) is tracked for the arrow/parquet 59 upgrade.

## [0.1.0] - 2026-07-08

The initial GA surface.

### Added

**Core engine & query**
- Text search over Apache Iceberg: index a source table, search it, hydrate authoritative
  rows back from Iceberg by primary key (`/v1/search`, `/v1/keys:get`).
- Layered query language: a native structured AST plus a Lucene/KQL string parser
  (`field:value`, phrases, ranges, wildcards, fuzzy, CIDR, regex, boost, `AND`/`OR`/`NOT`).
  `*:*` / `*` parse to a cheap match-all.
- Composite, partition-aware document keys; field collapsing; keyset (`search_after`) paging;
  point-in-time reads; suggestions/autocomplete; aggregations.

**Distribution**
- Control plane (index registry), stateful searcher/index nodes, and a query Gateway
  (scatter-gather + top-K merge). Node self-registration with the control plane.
- Sharding (hash by key; partition routing when the source is partitioned); partial-result
  flagging when a shard is down.

**Security & multi-tenancy**
- AuthN at the Gateway: OIDC/JWT (JWKS), API keys, mTLS between services. Forged caller-asserted
  identity headers are dropped and replaced with the verified claim at the trust boundary.
- Control-plane RBAC (viewer / index-admin / operator / service roles).
- Tenant scoping: a mandatory, non-widenable `tenant_field = <verified claim>` filter on every
  read; cross-tenant isolation verified end-to-end.

**Observability**
- OpenTelemetry traces + metrics + structured JSON logs; OTLP export; Prometheus `/metrics`;
  health/readiness probes; a bundled LGTM stack and GrowlerDB SLI dashboards in Compose.

**Console UI**
- A Svelte SPA served by the Gateway: Search & Explore, Indexes (create via source
  introspection / drop), Ingestion (per-shard source-head vs. committed-checkpoint lag), and
  Observability (native ECharts SLI panels).

**Ecosystem**
- Optional OpenSearch-compatible `_search` adapter (`gateway --opensearch`): a documented DSL
  subset → native query; `_id` from the composite key, `_source` via hydration. See
  [docs/opensearch-adapter.md](docs/opensearch-adapter.md).

**Deployment**
- Docker Compose stack (GrowlerDB + MinIO + Polaris + LGTM) for local/dev/test.
- A Helm chart (`deploy/helm/growlerdb`) for the Kubernetes sharded-cluster topology.

**Release & build**
- Tag-derived release versioning: `release.yml` runs on a `workflow_dispatch` (`bump:
  patch|minor|major`, auto-computing the next version) or a pushed `v*` tag. The version is stamped
  into the image, chart `appVersion`, binaries, and CLI `--version` while the tree stays `0.0.0`;
  the image gets an immutable `X.Y.Z` plus moving `X.Y`/`X`/`latest`. See [RELEASING.md](RELEASING.md).

[Unreleased]: https://github.com/GrowlerDB/growlerdb/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/GrowlerDB/growlerdb/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/GrowlerDB/growlerdb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/GrowlerDB/growlerdb/releases/tag/v0.1.0
