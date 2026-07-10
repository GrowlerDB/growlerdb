# Changelog

All notable changes to GrowlerDB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) (see [RELEASING.md](RELEASING.md)).

## [Unreleased]

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

**Distribution (M3)**
- Control plane (index registry), stateful searcher/index nodes, and a query Gateway
  (scatter-gather + top-K merge). Node self-registration with the control plane.
- Sharding (hash by key; partition routing when the source is partitioned); partial-result
  flagging when a shard is down.

**Security & multi-tenancy (M4)**
- AuthN at the Gateway: OIDC/JWT (JWKS), API keys, mTLS between services. Forged caller-asserted
  identity headers are dropped and replaced with the verified claim at the trust boundary.
- Control-plane RBAC (viewer / index-admin / operator / service roles).
- Tenant scoping: a mandatory, non-widenable `tenant_field = <verified claim>` filter on every
  read; cross-tenant isolation verified end-to-end.

**Observability (M4)**
- OpenTelemetry traces + metrics + structured JSON logs; OTLP export; Prometheus `/metrics`;
  health/readiness probes; a bundled LGTM stack and GrowlerDB SLI dashboards in Compose.

**Console UI (M6)**
- A Svelte SPA served by the Gateway: Search & Explore, Indexes (create via source
  introspection / drop), Ingestion (per-shard source-head vs. committed-checkpoint lag), and
  Observability (native ECharts SLI panels).

**Ecosystem (M7)**
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

[Unreleased]: https://github.com/GrowlerDB/growlerdb/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/GrowlerDB/growlerdb/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/GrowlerDB/growlerdb/releases/tag/v0.1.0
