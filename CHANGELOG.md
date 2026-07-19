# Changelog

All notable changes to GrowlerDB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) (see [RELEASING.md](RELEASING.md)).

## [Unreleased]

### Added

- **Vector fields (foundation).** A `VECTOR` field type embeds a text column (local model by default,
  no egress) and stores the per-document embedding in the segment, backed up with the lexical segment —
  the base for semantic / hybrid retrieval. Opt-in per field; `model` / `dims` / `metric` / `provider`
  recorded in the index metadata for reproducibility. Embeddings are produced through a pluggable
  `Embedder` seam (external providers attach here). Query-time KNN / fusion / reranking follow.
  (ADR D19/D20/D21/D41/D42 · TASK-41)
- **Local BGE embedding runtime.** The default embedder is **bge-small-en-v1.5** run in-process on
  [Candle](https://github.com/huggingface/candle) — pure Rust, no native/C dependency, no network. The
  model is provisioned out of band into `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model-id>/`;
  when absent, embedding falls back to a deterministic dev embedder so ingest and offline CI keep
  working. Behind a default-on `bge` build feature (a slim build can drop the ML dependency). (TASK-41)
- **Per-segment ANN index + semantic (KNN) retrieval.** Each segment's vectors are indexed into a
  GrowlerDB-owned `<segment>.ann` sidecar (built after commit + compaction, backed up / restored with
  the lexical segment). A top-level KNN query embeds the query text (same embedder as ingest) and
  returns the nearest documents as coordinates that hydrate. KNN is not yet tenant-filtered, so it is
  refused **fail-closed** on tenant-scoped indexes and exposed only on the native engine API (RRF
  fusion + filtered KNN follow). (ADR D19 · TASK-42)
- **Hybrid search (RRF) + filtered / tenant-scoped KNN.** A KNN query takes an optional filter
  (lexical / fast-field sub-query) whose matching docs constrain the neighbors, and `hybrid_search`
  fuses lexical BM25 + vector KNN via Reciprocal Rank Fusion into one ranking. The mandatory
  `tenant = <claim>` filter is now enforced on the vector path (the tenant Term rides inside the KNN),
  so tenant-scoped semantic/hybrid search is filtered rather than refused — still fail-closed when a
  scoped index has no verified claim. On a real-model paraphrase eval, hybrid strictly beats
  lexical-only. Engine-facade for now (gateway exposure is a later surface). (TASK-43)
- **Semantic + hybrid search on the authenticated gateway.** Exposed multi-shard over gRPC
  (`SemanticSearch`) and REST (`/v1/search:semantic`, `/v1/search:hybrid`). The query is embedded on
  each **node** (the gateway carries no embedding model — [D43](okf/system/decisions/d43-node-local-query-embedding.md));
  the gateway scatters, merges by score, and RRF-fuses the lexical + vector arms for hybrid. The
  mandatory tenant filter is enforced at the node on the vector path (fail-closed without a claim), so
  tenant isolation holds on semantic/hybrid exactly as on lexical. (TASK-302)
- **MCP retrieval server (`growlerdb mcp`).** A read-only Model Context Protocol server (JSON-RPC over
  stdio) that exposes GrowlerDB to AI agents (Claude, any MCP client) as a governed retrieval tool —
  tools: `search` (lexical / semantic / hybrid), `hydrate`, `aggregate`, `list_indexes`,
  `describe_index`. It fronts the authenticated gateway over HTTP and forwards the caller's bearer
  token, so RBAC + the non-widenable tenant filter are reused verbatim — an agent cannot reach another
  tenant's data. Hand-rolled (no SDK dependency). (ADR D41/D42 · TASK-297)
- **Console: vector / hybrid search + a grounded "Ask" screen.** The Search screen gains a Lexical /
  Semantic / Hybrid mode toggle (with a vector-field selector and an RRF-`k` control), a "more like
  this" action, and a "vectorize a field" step in create-index. A new **Ask** screen hybrid-retrieves a
  question and shows the source passages **with their Iceberg coordinates as citations** — **no answer
  generation**; GrowlerDB never calls an LLM ([D42](okf/system/decisions/d42-retrieval-first.md)).
  `POST /v1/index:describe` now reports an index's `vector_fields`. (TASK-298)

## [0.3.0] - 2026-07-18

The **Brand v1.0 + launch-readiness** release: a unified brand across the console, website, and docs;
automatic cold-tiering; and the pre-announcement docs / API-reference / quickstart hardening.

### Added

- **Cold-tiering — automatic park/revive.** Each node parks its own aged windows to cold read-through
  from object storage on a background timer, and pre-warms a cold window back to NVMe when it gets hot
  traffic again; wired on the node StatefulSet via Helm `coldTier.*`. (ADR D39)
- **Brand v1.0** — a unified visual + verbal identity (the waterline mark, a dark-first neutral palette
  with glacier/melt accents, the Archivo / Instrument Sans / Geist Mono type trio, and the
  voice/terminology) applied across the console, website, docs, and social card; canonical vector
  assets in `brand/`. (ADR D40)
- **`sort_fields`** on `POST /v1/index:describe` — the sortable (fast numeric/date/keyword) fields, so
  a client's sort menu only offers fields the engine can actually sort on.
- **Docs**: a directional **Performance** page (GrowlerDB vs Elasticsearch vs Trino), a **Comparison &
  positioning** page, the **aggregations/facets** surface + the full **REST reference** (11
  previously-undocumented routes), a **Trino connector** README, **BRAND.md**, and a prebuilt-artifact
  (image + binaries + Helm OCI) install quickstart.

### Changed

- **Console re-skinned to Brand v1.0** — design tokens, self-hosted fonts, and the waterline lockup
  replace the previous IBM-Plex look; a re-skin, not a redesign (all behaviour preserved). **Dark is
  now the default theme.**
- **Website** (apex `growlerdb.com`) and the **docs site** themed to Brand v1.0, with social unfurl
  (OG/Twitter) cards + the brand favicon.
- **Maturity wording** standardized to **Beta (0.x) — pre-1.0**; dropped the "GA line" claim while the
  external security review and formal benchmarks are pending.
- **Spark connector** aligned to Spark 4.1.3 / Iceberg 1.11.0 with the matching
  `iceberg-spark-runtime-4.1` (was a `-4.0` runtime against 4.1.3).

### Fixed

- An **empty-but-built shard** now records the source snapshot it caught up to — it reports `in_sync`
  (green) instead of leaving the whole index on a grey `uninitialized` health pill. (TASK-121)
- The console **sort menu** no longer offers non-sortable fields, which returned a `400`. (TASK-294)
- **Geist Mono** ligatures no longer collapse the space before a `--` (or merge `://` / operators) in
  rendered code. (TASK-295)
- A shard's **client error now surfaces** from a multi-shard fan-out instead of being masked. (TASK-209)
- **Cold-tier** runtime cold tracking + temporal-search units across all fields. (TASK-272/273)
- **Getting-started streaming quickstart** repaired: `telemetry_stream` RBAC/token, `node-catalog` no
  longer blocks the gateway in pipeline mode, and the `jq` / `mise` prerequisites are documented.
  (TASK-279)

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

> Versions 0.1.0–0.1.1 were pre-public builds under Apache-2.0, not published as releases —
> retained here for history. **0.2.0 is the first public release.**

## [0.1.1] - 2026-07-09

### Security

- Dependency security bumps ahead of the first public release, surfaced by Dependabot alerts:
  gRPC `1.75.0` (Netty "MadeYouReset" HTTP/2 DoS — high), `jsonwebtoken` `10` (type-confusion
  authorization-bypass advisory; the pure-Rust `rust_crypto` provider is selected explicitly), and
  ECharts `6.1` (console XSS advisory). A medium transitive `thrift` advisory (via `parquet`, in the
  own-data metadata-parse path) is tracked for the arrow/parquet 59 upgrade.

## [0.1.0] - 2026-07-08

The initial public (Beta) surface.

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

[Unreleased]: https://github.com/GrowlerDB/growlerdb/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/GrowlerDB/growlerdb/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/GrowlerDB/growlerdb/releases/tag/v0.2.0
