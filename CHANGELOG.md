# Changelog

All notable changes to GrowlerDB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) (see [RELEASING.md](RELEASING.md)).

## [Unreleased]

The **true high-availability** release — HA reaches every index type, not just windowed streams. The
control plane runs as replicas, and serving moves to a self-organizing **placement pool** of
interchangeable nodes where each index's units are replicated, so losing a node is a zero-gap read
failover. You point N identical nodes at the pool and the control plane does the placement; there is
no per-node designation.

**Upgrading:** no index rebuild and no data loss — the on-disk index format is unchanged, embedded
control-plane state (index defs, tokens, RBAC) survives an in-place upgrade, and the replicated
control plane, placement pool, and replication are all **opt-in** (defaults are unchanged). Two
things need attention: (1) upgrade the control plane, serving nodes, and the Spark connector
**together** — one RPC was generalized (below), so old↔new across that boundary won't talk; and
(2) the free-tier scale limit now counts **nodes holding a primary** and is now enforced at registration
— re-check the console's Enterprise-license panel (below). Compose users adopt the new topology.

### Added

- **Replicated control plane (D51).** The control plane can run as **N stateless replicas over an
  externalized Postgres registry** with leader/standby failover, so the cluster's registry is no
  longer a single point of failure. It stays an **optional deployment mode** — the default remains
  the embedded single-node file backend (no new hard dependency); Helm gains a `controlPlane` HA
  deploy mode (N replicas + PodDisruptionBudget). (ADR [D51](okf/system/decisions/d51-controlplane-ha.md))
- **Self-organizing placement pool (D52).** Interchangeable **`serve-pool`** nodes serve CP-assigned
  units from many indexes over a single endpoint. The control plane's placement sweep distributes
  each index's **primaries round-robin** across the pool (least-loaded, liveness-grace-aware), and a
  node assigned a primary it doesn't hold **builds that index from its Iceberg source on assignment**
  (build-on-assignment). HA becomes "run N identical nodes"; no per-node build/primary designation,
  and the classic per-index `serve` remains supported. A **cold-start fast path** places never-placed
  primaries as soon as a brief settle clears (a few seconds) instead of waiting out the full liveness
  grace, so a fresh pool (e.g. `just stack`) converges in seconds while re-placement of already-held
  units keeps the full grace for anti-flap. (ADR [D52](okf/system/decisions/d52-placement-pool.md))
- **Per-unit replication + read failover (D53).** A cluster-wide **replication factor R** places
  R holders per unit (one primary + R−1 read replicas); the gateway **fails reads over** to a live
  replica when a holder is down, replicas serve **read-through from the shared cold store** (object
  storage), and the pool **self-heals** — primaries publish hot snapshots on a loop and the control
  plane tops up replicas after the liveness grace. Node-side **primary fencing** refuses
  writes/checkpoints on non-primary units. Verified with a zero-gap failover chaos drill. (ADR
  [D53](okf/system/decisions/d53-unit-replication.md))
- **The demo runs on HA.** `just stack` now serves `docs` + `catalog` + `movies` on a **two-node
  placement pool at `GROWLERDB_REPLICATION_FACTOR=2`**, shaped like a production deploy — stop either
  pool node and reads keep answering via the survivor.

### Changed

- **The free-tier scale limit now counts nodes holding a primary — and is enforced at
  registration.** The AGPL free tier (3) counts distinct live **nodes that hold a primary of any
  index**; read replicas, additional indexes, and windows whose primaries co-locate on an
  already-counted node are **free** — so enforcement matches the marketed 3-node free tier. The cap is
  still **enforced** at `RegisterServedIndex` (it was previously fail-open), so a deploy that lit up a
  4th primary-holding node can be refused with `RESOURCE_EXHAUSTED`. **Migration:** after upgrading,
  confirm your primary-holding node count against the metric in the console's **Settings → Enterprise
  license** panel. (ADR [D38](okf/system/decisions/d38-scale-limit-entitlement.md))
- **Coordinated upgrade required across the control-plane wire.** All gRPC changes are additive
  **except** the `ResolveWindowOwner`→`ResolveUnitOwner` generalization (see Removed). **Migration:**
  upgrade the control plane, serving nodes, and the Spark connector in one step — a mixed old/new
  cluster across that RPC returns `UNIMPLEMENTED`.
- **The connector now requires `--partition <fields>` for a partition/window-routed index.** With
  hash routing added, the connector cross-checks its routing against the registry: for a
  PARTITION-routed index it needs the partition field(s) both to build composite keys and to match
  the index definition, and a bare invocation (no `--partition`) now derives HASH and aborts with
  *"routing strategy mismatch"*. **Migration:** pass `--partition <fields>` matching the index's
  `partition_fields` (e.g. `--partition site` for the streaming demo's `telemetry_stream`).
- **Compose stack rebuilt around the pool.** The demo now brings up `pool-a`/`pool-b` (`serve-pool`)
  instead of per-index node services, and adds required env for replica serving:
  `GROWLERDB_REPLICATION_FACTOR`, `GROWLERDB_BACKUP_BUCKET` (a new `growlerdb-cold` MinIO bucket), and
  `GROWLERDB_S3_ACCESS_KEY`/`GROWLERDB_S3_SECRET_KEY`. **Migration:** adopt the new
  `deploy/compose` files; a `serve-pool` node with no cold store logs "replica failover disabled" and
  the control plane won't place replicas on it.
- **Helm chart → 0.3.0.** Enabling the external-Postgres registry boots an **empty** registry (no
  automated migration) and changes the control-plane Service type. **Migration:** adopt the HA mode
  on a fresh install and let nodes re-register / re-create indexes + tokens, rather than toggling it
  in place.

### Removed

- **`ResolveWindowOwner` RPC** (and its request/response messages) — generalized to the unit-general
  **`ResolveUnitOwner`** (`window` moved under a `oneof unit`). Old and new are not wire-compatible;
  see the coordinated-upgrade note above. (ADR [D52](okf/system/decisions/d52-placement-pool.md))
- **`node-catalog` / `node-movies` compose services** (and their `node-catalog-data` /
  `node-movies-data` volumes) — replaced by the `pool-a`/`pool-b` placement pool; the `node` service
  is now used only by the streaming `pipeline` profile.

### Fixed

- **A classic `serve --index X` node's index is no longer stolen by the dead-owner sweeper.** Such a
  node announces only via `RegisterServedIndex` (never the `RegisterNode` heartbeat), so the control
  plane saw its owner as dead and re-placed the index onto a pool node — which then rejected reads
  for it. The CP now heartbeats a served-index owner into the **liveness** pool on every announce
  (keeping the sweeper off it) while keeping it **out** of the placement-eligible pool, so pool units
  are still never assigned to a node that can't build/serve them (a pool primary landing on the
  classic node would otherwise have broken failover).

## [0.6.0] - 2026-07-25

The **Iceberg v3 variant** release — GrowlerDB reaches inside semi-structured `variant` columns and
makes their leaves searchable, so a v3 lakehouse table with a JSON-shaped column is a first-class
index. Enterprise scale-limit licensing also goes live end to end.

### Added

- **Iceberg v3 `VARIANT` fields — search inside semi-structured columns.** A `variant` column is
  mapped through two composable modes: **flatten** (schema-less — every leaf indexed untyped as an
  exact `path = value` term plus an optional analyzed text catch-all, so type conflicts can't arise
  by construction) and **shapes** (declared, typed sub-schemas selected per row by a discriminator
  path, with the full field-type/flag surface). Leaf extraction happens at read time, so nodes and
  the wire model stay scalar-leaf-only; no whole-value blob is stored (declared paths may be
  `cached`, the full object comes back via hydration). Ships **connector-first**: the Spark
  connector reads variant today (bootstrap + changelog, incl. shredded files) via a
  `VariantExtractor` / `--variant-spec`, and — because released iceberg-rust cannot yet parse a v3
  schema that contains a variant column — a variant index's **create-time schema introspection and
  hydration route through Trino** in the interim (key-predicated point reads, variant as JSON),
  while every non-variant index keeps the native path untouched. The demo ships a variant `events`
  index (`just stack`) so flatten + shapes work out of the box.
  (ADR [D47](okf/system/decisions/d47-variant-mapping.md)/[D48](okf/system/decisions/d48-variant-delivery.md)/[D49](okf/system/decisions/d49-variant-iceberg-rust-routing.md)
  · TASK-348…352)
- **Enterprise scale-limit licensing is now active.** Beyond the free-tier node cap, the control
  plane admits additional nodes only with an offline-verified Enterprise license — and the issuing
  side now exists: `License::mint()` signs a token from an Ed25519 private key (held by GrowlerDB
  LLC, never in-repo), the production **signing public key ships in the binary**, and Helm wires
  `credentials.license → GROWLERDB_LICENSE` on the control plane (rendered only when set; empty ⇒
  free tier). `just mint-license` / `just verify-license` wrap the operator ceremony. Existing nodes
  and data are never disrupted — scale is the gate. (ADR [D38](okf/system/decisions/d38-scale-limit-entitlement.md) · TASK-346)

### Changed

- **Read stack advanced to Iceberg 0.10 / Arrow 58** (`iceberg-storage-opendal` 0.10) — the base the
  native variant read path will land on once iceberg-rust ships v3 variant-schema parsing (tracked;
  the interim Trino lane then demotes to the slow lane for delete-bearing files and the stale
  fallback).
- **Trino aligned to 483** across the Compose stack, the scale bench, and the k8s manifests.
- **Quieter `just stack` startup** — the bring-up moved out of the justfile into
  `deploy/compose/stack-up.sh`, which prints a handful of numbered step headers instead of echoing
  the orchestration (and its explanatory prose) line by line; `--quiet-pull` drops the layer-pull
  spam. No change to the sequence, profiles, or endpoints.

### Docs

- **SEO for the website & docs:** sitemaps, `robots.txt`, and JSON-LD structured data on both hosts,
  plus a search-engine submission runbook.
- The **`movies` vector index is featured as the demo default** across the getting-started flow and
  the OKF (following its 0.5.0 debut as the console's landing index).

## [0.5.0] - 2026-07-23

The **unified search experience** release — semantic and hybrid retrieval become one search box that
gets smarter, the demo lands on a keyless movie-plots vector index out of the box, and a deployment can
declare its front-door index.

### Added

- **Semantic & hybrid retrieval, inline in Search.** The console's **Lexical / Semantic / Hybrid** mode
  toggle is the whole story: a natural-language placeholder in the vector modes, a one-time **"Try
  semantic"** invitation on a vector-capable index, and **"more like this"** on a hit. One search box
  that gets smarter — no separate retrieval screen.
- **Deployment front-door index (`GROWLERDB_DEFAULT_INDEX`).** A new gateway env, surfaced via
  `/v1/config`, lets a deployment declare the index the console opens on. The demo points it at `movies`
  (a `VECTOR` index) so a fresh visitor lands where semantic/hybrid is one click away; unset ⇒ the first
  index.
- **Demo: a keyless movie-plots vector index out of the box.** `just stack` now ships a small (300-film)
  **`movies`** index — Wikipedia movie plots (CC-BY-SA), embedded locally with bge-small-en-v1.5 — as the
  console's default landing index, so semantic + hybrid search work on first run with no API key or
  egress. `just demo-data` upgrades it to the full corpus.
- **`just stack-dev`** — bring up the full stack from your working checkout (engine + console) instead of
  the released image, for smoke-testing local changes end to end.

### Changed

- **Retired the standalone "Ask" (grounded-retrieval) screen.** Semantic/hybrid retrieval already lives
  in Search, so a second door was redundant, and the "Ask" label over a retrieval-only feature (no answer
  generation — GrowlerDB never calls an LLM, [D42](okf/system/decisions/d42-retrieval-first.md)) invited
  the wrong expectation. Its value — source passages with governed Iceberg-coordinate provenance — is
  delivered in Search results.
- **Two-row search bar** so the query box gets full width; removed the redundant in-field syntax pill
  (the Lucene/KQL toggle already shows the active syntax).

### Fixed

- **Semantic results now hydrate after a demo re-seed.** A running node synced a reloaded source table
  into its **lexical** segments but not the **vector** ANN sidecars (sync/reindex re-embed is tracked
  separately), so semantic hits pointed at dropped data files — "row not found" on click — while lexical
  worked. The demo vector indexes (`movies`, `catalog`) now cold-rebuild on re-seed.
- **Honest empty states in vector modes:** the facet rail no longer claims "no facetable fields" for a
  top-`k` neighbour set (it points to Lexical mode instead), and term highlighting is gated to a lexical
  match — Hybrid marks its BM25 query terms, pure Semantic marks nothing (highlighting natural-language
  query words falsely implied a literal match).

## [0.4.1] - 2026-07-23

Release-tooling fix — **no functional change from 0.4.0**. The 0.4.0 container image and Helm chart
published fine, but the standalone binaries failed to build, so 0.4.0 shipped without them; 0.4.1 is
the first release of this line to include the downloadable binaries.

### Fixed

- **Release binaries now build and publish.** The local embedder links a native ONNX Runtime (the
  `ort` crate) whose prebuilt requires glibc 2.38+, which the `cross` containers the binaries job used
  don't provide — so the build failed with `could not find native static library onnxruntime` on both
  x86_64 and aarch64. The binaries are now built on **native per-arch runners** (as the container image
  already is), so they link ONNX the same way. **The release binaries require glibc 2.38+** at runtime,
  the same floor as the container image.

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

[Unreleased]: https://github.com/GrowlerDB/growlerdb/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/GrowlerDB/growlerdb/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/GrowlerDB/growlerdb/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/GrowlerDB/growlerdb/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/GrowlerDB/growlerdb/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/GrowlerDB/growlerdb/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/GrowlerDB/growlerdb/releases/tag/v0.2.0
