# Changelog

All notable changes to GrowlerDB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) (see [RELEASING.md](RELEASING.md)).

## [Unreleased]

## [0.4.0] - 2026-07-23

The **vector, semantic & hybrid retrieval** release — GrowlerDB grows from full-text into full-text +
vector + hybrid search over your Iceberg data, with a governed **MCP** server that makes it a
first-class retrieval tool for AI agents. Embeddings are **local and keyless by default** — no egress,
and GrowlerDB never calls an LLM.

### Added

**Vector & semantic retrieval**

- **`VECTOR` field type + embed-at-ingest.** A `VECTOR` field embeds a text column and stores the
  per-document embedding in the segment (backed up / restored with the lexical segment) — the base for
  semantic / hybrid retrieval. Opt-in per field; `model` / `dims` / `metric` / `provider` are recorded
  in the index metadata for reproducibility, and embeddings flow through a pluggable `Embedder` seam
  (external providers attach here). (ADR D19/D20/D21/D41/D42/D46 · TASK-41)
- **Local embedding runtime (keyless, no egress).** The default embedder runs **bge-small-en-v1.5**
  in-process on **ONNX Runtime** — no network, no API key, ~30× the CPU throughput of the initial
  pure-Rust path. The model is provisioned out of band into
  `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model-id>/`; when it is absent a deterministic dev
  embedder keeps ingest and offline CI working. Behind a default-on build feature (a slim build can drop
  the ML dependency). (TASK-41 · #175)
- **Per-segment ANN index + semantic (KNN) retrieval.** Each segment's vectors are indexed into a
  GrowlerDB-owned `<segment>.ann` sidecar (built after commit + compaction, backed up / restored with the
  lexical segment). A top-level KNN query embeds the query text (the same embedder as ingest) and returns
  the nearest documents as coordinates that hydrate. (ADR D19 · TASK-42)
- **Approximate ANN (HNSW) at scale.** The sidecar auto-selects a pure-Rust **HNSW** index
  (`instant-distance`) once a field holds more than `HNSW_MIN_VECTORS` (4096) vectors, and stays exact
  brute-force below that — transparent, same `knn` semantics, no config change. ~2.9× faster per query at
  recall@10 ≈ 0.96 on a 10k × 128-d benchmark; **filtered / tenant-scoped KNN stays exact** on both
  tiers, so a selective filter never under-fills. (ADR D19 · TASK-301)
- **Hybrid search (RRF) + filtered, tenant-scoped KNN.** `hybrid_search` fuses lexical BM25 + vector KNN
  via Reciprocal Rank Fusion; a KNN query takes an optional lexical / fast-field filter that constrains
  its neighbors. The mandatory `tenant = <claim>` filter is enforced **inside** the vector path, so
  tenant-scoped semantic / hybrid search is filtered rather than refused — still fail-closed without a
  verified claim. On a real-model paraphrase eval, hybrid strictly beats lexical-only. (TASK-43)
- **Semantic + hybrid search on the authenticated gateway.** Exposed multi-shard over gRPC
  (`SemanticSearch`) and REST (`/v1/search:semantic`, `/v1/search:hybrid`). The query is embedded on
  each **node** (the gateway carries no embedding model —
  [D43](okf/system/decisions/d43-node-local-query-embedding.md)); the gateway scatters, merges by score,
  and RRF-fuses the lexical + vector arms. Tenant isolation holds on semantic / hybrid exactly as on
  lexical. (TASK-302)
- **Opt-in reranker.** A pluggable `Reranker` reorders a semantic / hybrid query's top-K by a
  cross-encoder pass over `(query, passage)` — set `rerank: true` (+ an optional `rerank_top_k` candidate
  pool). It sits **outside** the index (a post-retrieval reorder), is **off by default** (retrieval-first),
  and runs the local **bge-reranker-base** on ONNX Runtime (falls back to a deterministic dev reranker
  when the model isn't provisioned — offline / keyless). (ADR D21 · TASK-44)
- **External embedding / rerank providers (opt-in, server-side keys).** A vector field with
  `provider: EXTERNAL` (or `GROWLERDB_RERANK_PROVIDER=external`) calls a hosted provider over HTTP with a
  **server-side-only** API key read from the engine env (k8s Secret / Vault mount), cached with a 5-min
  TTL, **redacted** in all output, and **never** exposed to the browser or `/v1/config`. Selecting
  `EXTERNAL` without a key **fails closed**. The local default needs zero keys; there are **no LLM keys** —
  GrowlerDB never calls an LLM ([D42](okf/system/decisions/d42-retrieval-first.md)). (ADR D20/D21 · TASK-299)
- **Inline hydration.** A search can return the authoritative Iceberg rows **in the same query** instead
  of a follow-up `keys:get`, collapsing the search → hydrate round trip. (TASK-317)

**MCP for AI agents**

- **`growlerdb mcp` — governed retrieval server.** A read-only Model Context Protocol server that exposes
  GrowlerDB to AI agents (Claude, any MCP client) as a governed tool set — `search`
  (lexical / semantic / hybrid), `hydrate`, `aggregate`, `list_indexes`, `describe_index`, and
  `more_like_this`. It fronts the authenticated gateway and forwards the caller's bearer token, so RBAC +
  the non-widenable tenant filter are reused verbatim — an agent cannot reach another tenant's data.
  (ADR D41/D42 · TASK-297)
- **Streamable HTTP transport + one-command quick-connect.** Every REST front serves MCP over
  **Streamable HTTP** (not only stdio), and `just mcp-connect` hooks a local agent to the demo stack over
  HTTP in one step. A self-teaching schema, context budgets, and actionable errors steer agents to the
  live indexes. (TASK-318/319/321)

**Console & demo**

- **Console: vector / hybrid search.** The Search screen gains a **Lexical / Semantic / Hybrid** mode
  toggle (with a vector-field selector and an RRF-`k` control), a **"more like this"** action, and a
  **"vectorize a field"** step in create-index. `POST /v1/index:describe` now reports an index's
  `vector_fields`. (TASK-298)
- **Demo: keyless semantic / hybrid out of the box.** `just stack`'s `catalog` index carries a `body_vec`
  VECTOR field (local bge-small-en-v1.5), and `just demo-data` stands up a vector-enabled **movies** index
  (Wikipedia movie plots), so semantic + hybrid search and the MCP server run against real data — keyless,
  no egress. The model is fetched **once per machine** into a host-mounted cache (reused across runs and
  local `cargo` / eval); the published image is not bloated. (TASK-300 · #180)
- **Query-surface admission control.** The gateway sheds load on the query path under pressure (bounded
  concurrency / queue) so a spike degrades gracefully instead of tipping the cluster. (TASK-314)

### Changed

- **Repositioned to "full-text, vector & hybrid retrieval over your data."** The README, docs landing,
  and product messaging reflect the retrieval-first, open-core vector strategy — embedding is a
  provenance-typed write-path stage, not a bolt-on. (ADR D44/D46)
- **Console "Ask" (grounded-retrieval) screen withheld from this release.** The screen is built but its
  `/rag` route is unregistered: the default demo index (`docs`) has no vector field, so it dead-ends, and
  the "Ask" label over a retrieval-only feature (no answer generation — GrowlerDB never calls an LLM, D42)
  invites the wrong expectation. Re-exposed once the demo ships a vectorized default. (#201)
- **Online shard grow** reworked so a live `grow` actually rebalances — map adoption, map-wins routing,
  and a CAS cutover replace the previous no-op path. (TASK-309)

### Fixed

- **Schema change on a built index no longer panics.** A definition that gained, dropped, or retyped a
  mapped field previously crashed the fast-field writer; the engine now detects the derived-schema change
  and **reindexes from scratch** (logged), backed by a store-level `SchemaChanged` error that guarantees
  the mismatch can never reach a writer. (TASK-303)
- **Windowed:** a cold-window write no longer panics, and the safe resume floor is carried across
  restarts. (TASK-308)
- **Degraded results are flagged, not silently dropped.** A partial or failed arm — including missing
  embed coverage on a hybrid query — now surfaces as a degraded-result flag instead of quietly returning
  fewer hits. (#173)
- **Backup / cold-tier hardening:** a cold-park write-race check, a torn-refresh guard, and manifest-first
  bundle writes close data-loss windows in park / restore. (TASK-313)
- **Robustness batch:** UTF-8 redaction, a `from_owners` guard, the hybrid filter applied to both arms, a
  shared env-guard for embed configuration, and a window-0 warning. (TASK-315)
- **Build / site:** the ONNX release image builds on a glibc-2.38+ base, `include_str!` markdown is kept
  in the Docker context, the docs site's dark code palette is legible, and the website nav collapses to a
  hamburger on mobile. (#176/#167/#168/#178)

### Security

- **Node data plane closed.** The Node's data-plane RPCs now require the mesh **service token**, with
  trust-boundary hardening — the demo mesh is closed by default and a Node won't answer unauthenticated
  peers. (TASK-310)
- **Design-review hardening:** additional gateway limits, topology observability, and an auth guard on the
  CLI / engine surface. (#183)
- Grouped dependency security bumps across Rust, Maven, npm, GitHub Actions, and Trino.

### Docs

- An approachable README + docs landing, a scannable quickstart command block, the full OpenSearch
  response envelope in the adapter example, tenant-isolation-is-opt-in clarified (single-tenant indexes
  set no `tenant_field`), and dead design / wiki links repointed to the OKF. (D44/D46)

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

[Unreleased]: https://github.com/GrowlerDB/growlerdb/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/GrowlerDB/growlerdb/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/GrowlerDB/growlerdb/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/GrowlerDB/growlerdb/releases/tag/v0.2.0
