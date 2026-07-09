# Pre-GA update log

_Development history compiled before the GA baseline (order preserved, newest first). All timestamps are normalized to 2026-07-04T14:22:00 for the GA release; see [`log.md`](log.md) for post-GA updates._

* **Scale-run tooling hardening** (task-220/221/222/223 — the hiccups from the 2026-07-04 windowed
  runs, folded back into the tooling): (1) **Server image from merged main** — the `scale-images`
  workflow now builds the `growlerdb` **server** `:dev` (+ commit-SHA) image from main alongside
  connector/seed, so a post-merge / pre-release scale run deploys the **code under test** instead of a
  `latest` that only `release.yml` rebuilds (the stale-image trap that cost the 2026-07-04 run);
  `scale-up.sh` defaults `IMAGE_TAG=dev`, warns on `latest`, and prints the deployed binary's
  `--version` (the `:dev` build stamps `GROWLERDB_VERSION=dev-<sha>`). (2) **Robust private-NIC
  bring-up** — cloud-init (agent + server) now **actively brings `enp7s0` up + requests DHCP with
  retries** on a bounded, loud-failing 300s deadline, self-healing the boot-timing straggler that
  needed a manual `ip link set … up; dhcpcd` rescue on every provision. (3) **Windowed bring-up
  ordering** — `scale-up.sh` deploys the connector **before** the windowed gateway-ready wait (a
  windowed gateway isn't `/readyz` until ≥1 window exists, and the connector makes windows), removing
  the spurious 180s rollout timeout. (4) **Truly-empty windowed start** — new `growlerdb index
  --define-only` writes `index.json` and builds **zero** windows; the windowed node topology uses it so
  nodes start empty and windows are only ever CP-placed via streamed writes, never batch-built and
  replicated on every node. See [scale-test plan](/quality/scale-test-plan.md) + `deploy/iac/README.md`.

* **Windowed index ingestion feed represents windows** (task-226 AC#3): `/v1/ingestion` +
  `growlerdb_shards_up`/`_total` now report a windowed index as its **time windows** instead of the
  old "0 of 0 shards" / "No indexes registered". `collect_ingestion` iterates the registry's `windows`
  map (each window's primary, per-window checkpoint via `GetCheckpoint`'s `window` selector) when the
  index is windowed, mirroring the ordinal `shards` loop; `ShardIngestion` gains a `window` field
  (0 for ordinal) and `shard_count`/availability count windows. The console Observability Ingestion
  table labels a windowed index's rows as windows (`w<id>`, "N win") and its per-window sync state —
  so a live windowed index shows real ingestion health, not a blank "unknown". Closes the last
  task-226 gap (the header + throughput halves shipped earlier today).

* **Console header health + windowed ingest throughput** (task-226; found on the 2026-07-04 windowed
  run 3): the header pill read **"Unknown"** on every k8s deploy — the health roll-up matched targets
  by Compose instance name or a `namespace="growlerdb"` label, but k8s `up` samples carry a pod-IP
  `instance`, no `namespace`, and a `job` of `gdb-controlplane`/`gdb-node`/`gdb-gateway`, so nothing
  matched → no components → "Unknown". `isGrowlerdbTarget` now also matches those jobs (and names them
  by role). Separately, the **windowed** write path didn't emit `growlerdb_ingested_docs_total`, so the
  console Ingestion throughput + the Grafana "index docs/s" panel read a flat 0 under live ingest —
  `WindowedWriteService` now emits it per commit like the ordinal path. (NB: this corrects the
  2026-07-04 task-225 note that wrongly attributed the "Unknown" header to the `index:describe` 500 —
  that was a separate bug; the header is the Prometheus `up` roll-up. Representing a windowed index as
  *windows* in `/v1/ingestion` + `shards_up/_total` remains a follow-up.) See
  [observability](/system/observability.md).

* **Windowed gateway — hydration + describe** (task-225; fixes a gap found on the 2026-07-04 windowed
  scale run): `keys:get` (document hydration) and `index:describe` now work over a distributed windowed
  index — previously both failed *"all N shards failed to respond"* because the node's gRPC endpoint
  served only the search/suggest window multiplexers and the gateway hash-routed keys as if ordinal.
  The node now also serves **`WindowedLookupService` + `WindowedAdminService`** (window→service
  multiplexers mirroring search/suggest); `GetByKeyRequest`/`DescribeIndexRequest` gain a `window`
  selector each `WindowNode` stamps; the gateway **broadcasts** a hydration to every window (a key's
  coordinate carries no time, so it can live in any — the owning window returns its row; under the
  default COORDINATES locator non-owners return just their subset, under PREDICATE a missing key's
  NotFound is folded to an empty contribution) and **fans** a describe to every window, summing the
  per-window stats. This restores the console document-detail (Fields tab), the Indexes tab
  (docs/shards), and the header health badge — which read **"Unknown"** because it rolls up `describe`.
  Follow-on: per-key window routing to avoid the hydration fan-out; a PREDICATE multi-key request
  spanning windows.

* **Latency metrics export as true histograms** (task-224): the recorder now sets explicit buckets for
  `*_duration_seconds` (query / hydration / http-request), so `metrics-exporter-prometheus` exports a
  real cumulative **histogram** (`_bucket`) instead of a decaying-window **summary** (`{quantile}`).
  `histogram_quantile(…, rate(…_bucket[5m]))` now resolves (the scale dashboard's p50/95/99 panels
  rendered empty against the non-existent `_bucket`), the console Observability latency charts switch to
  the same expression and stop **drifting on every refresh / decaying to 0 with no traffic**, and the
  quantiles aggregate correctly across gateway replicas. Supersedes the 2026-07-04 note below that
  relied on the summary `{quantile}` rendering. See [observability](/system/observability.md).

* **Windowed k8s deployment topology** (task-219, stage 4 — closes the topology gap; ADR
  [D33](/system/decisions/d33-windowed-topology.md)): the sharded Helm chart gains a **windowed node
  topology** (`index.windowed=true`) — nodes start empty and run `serve` with **no
  `--shards`/`--shard-ordinal`** (auto-detecting windowing), heartbeat into the CP placement pool, and
  create/serve windows the connector streams to them. `deploy/k8s/scale-up.sh` detects a windowed
  workload (its `index.yaml` declares `windowing:`) and sets `index.windowed` instead of **failing
  fast** as before; the rendered connector's existing `--control-plane`/`--index` args auto-select the
  windowed write path, so no connector-manifest change was needed. Engine prerequisites: `serve_windowed`
  now starts on an **empty** window set (grows via ingest), and `RegisterServedIndex` classifies
  windowed-vs-ordinal by the **definition** (not whether `windows` is populated) so an empty windowed
  node still registers a *windowed* entry for `ResolveWindowOwner`. The
  [windowed-k8s-topology limitation](/quality/known-limitations/windowed-k8s-topology.md) is **resolved**
  (residual follow-ups: window replicas, resume bounding, worker parallelism, window-aware source
  maintenance, a live convergence run). See [scale-test plan](/quality/scale-test-plan.md).

* **Windowed streaming ingest — node + query side** (task-219, stage 3 of the distributed windowed
  k8s topology): a windowed node can now be **streamed into** and grows its window set live. A new
  `WindowedWriteService` on the node routes each incoming doc to its time-window shard (via
  `window_of`), **creates the window shard on first write**, commits per-window with an independent
  per-window checkpoint (`GetCheckpoint` gains a `window` selector), and **publishes** the new window
  so it's immediately queryable — the search/suggest multiplexers read a shared, mutable window map,
  and the node's in-process gateway hot-swaps via the new `Gateway::swap_windowed` (window routing
  moved into the swappable `RoutingState`). `serve_windowed` mounts the write service, seeds the
  shared maps with its boot windows, attaches auto-compaction to each new window, and **dynamically
  re-announces**: it heartbeats into the placement pool (`RegisterNode`) and re-registers the windows
  it currently serves (+ zone-maps) each tick, so a window created since boot is advertised. On the
  read path, the **cluster gateway hot-reloads windowed topology** over the live control plane
  (`swap_windowed` + a windowed `GetIndex` reloader), so runtime-created/placed windows become
  queryable through the gateway with no restart. The **Java connector** closes the producer side:
  `GetIndex` now also carries the window field's `field_format`, so `WindowRouter` computes each row's
  window id **byte-identically to the engine** (`window_of ∘ field_micros`, parity-tested), and a new
  `WindowedWriteClient` partitions each batch by window (per-window `#w{window}` batch-ids, no
  `from`/`safe` checkpoint — matching `partition_batch` so a skipped window isn't gap-rejected), routes
  each window to its owning node via `ResolveWindowOwner` (cached), broadcasts deletes, and resumes
  from the min committed checkpoint across committed windows. `ConnectorApp` detects a windowed index
  from `GetIndex` and uses it automatically. Remaining before the temporal workload runs on-cluster:
  the **chart/scale-up** wiring (a windowed node topology — nodes start empty, no `--shards` — and the
  scale-up guard replaced by a windowed bring-up) and a live end-to-end convergence run. See
  [windowed k8s topology](/quality/known-limitations/windowed-k8s-topology.md).

* **CP-driven windowed placement engine** (task-219, stage 2 of the distributed windowed k8s
  topology): the control plane now **places** time windows on nodes, rather than only recording what a
  node says it serves. Each windowed node heartbeats into a per-index **node inventory** (in-memory,
  ephemeral — liveness is not durable topology, so it isn't persisted; nodes re-register after a CP
  restart) via a new `RegisterNode` RPC; the connector resolves each row's window to its owning node
  via `ResolveWindowOwner`, which **assigns on first ask** — an unassigned window (or one whose owner
  has gone silent past the 30 s heartbeat TTL) is placed on the **least-loaded live node** (ties broken
  by endpoint, so placement is deterministic) and recorded in the durable window map the stage-1
  gateway reads. Idempotent for an already-placed live window; retryable (`Unavailable`) when no node
  is live yet. Primary-only for now — window replicas/read-HA stay a follow-up
  ([windowed replica gap](/quality/known-limitations/windowed-replica-gap.md)); re-placing a dead
  owner's window moves the assignment, and the new owner rebuilds its data from source (a later stage).
  See [windowed k8s topology](/quality/known-limitations/windowed-k8s-topology.md).

* **Live-CP windowed gateway — read path** (task-219, stage 1 of the distributed windowed k8s
  topology): the sharded gateway resolves a windowed index's topology from the **live control plane
  over gRPC**, not just the file `--registry` path. `GetIndex` now carries each window's event-time
  zone-map (`ShardStatus.event_min/max/has_event_bounds`) and the index's `WindowingConfig`
  (field/granularity/event-time field), so `gateway --control-plane` builds a window-pruning gateway
  (one `WindowNode` per window, deduped by endpoint) and a time-filtered search prunes to the
  overlapping windows — the gRPC analog of the registry-file windowed gateway. Windowed indexes are
  not hot-reloaded yet (static window set under today's single-process serve); dynamic-window reload
  arrives with streaming ingest. This unblocks *querying* a windowed index deployed on k8s; the node
  build/serve topology + windowed streaming ingest are later stages. See
  [windowed k8s topology](/quality/known-limitations/windowed-k8s-topology.md).

* **Config-driven scale workloads** (task-214): switching the k8s scale run to a different workload
  required editing THREE hardcoded files (generator.yaml's inline generation python, connector.yaml's
  `--identifier/--fields`, values-scale.yaml's mirrored index def) — now the whole deploy derives
  from one `bench/scale/workloads/<name>/` definition. The corpus module is both entry points
  (`load()` bulk + new `stream()` continuous — the generic generator Deployment mounts the
  workload's own corpus.py, killing the inline-python duplicate that had already drifted);
  `harness.py render` derives the connector's field lists from `index.yaml` and sizes `--nodes` to
  the shard count (the windowed connector's hardcoded copy had stale field names — rendering fixes
  that class of bug permanently); and the chart takes the workload's `index.yaml` **verbatim**
  (`--set-file index.definition=...`) instead of a values-level reconstruction
  (values-scale.yaml's mirror block is gone). `WORKLOAD=<name> deploy/k8s/scale-up.sh` is the one
  command; the four hardcoded generator/connector manifests are deleted; `just smoke` renders every
  streaming workload offline. See [helm-k8s](/system/deployment/helm-k8s.md).

* **Per-field TEXT indexing detail** (task-216, fourth of the index-storage-reduction cluster):
  TEXT mappings gain `record: BASIC | FREQ | POSITION` (per-posting detail; default `POSITION` —
  full fidelity) and `fieldnorms: bool` (BM25 length normalization; default on). Positions serve
  only phrase queries and are usually the biggest slice of a text field's inverted index —
  `FREQ` sheds them with **identical term/match results and identical BM25 scores** (proved
  side-by-side in-test; Tantivy's `read_postings` downgrades gracefully so only `PhraseQuery`
  truly needs positions, and that path now fails with the remedy: "set `record: POSITION`").
  Both knobs are TEXT-only (loud resolve errors elsewhere) and reindex-requiring to flip. The
  http_logs workload drops positions on `referer` only (token-searched by domain/word); `path`
  and `user_agent` keep POSITION — quoted-path adjacency search is a real log idiom, and a new
  `phrase_path` bench query (`match_phrase: "api v1 checkout"`) exercises what positions pay
  for. See [data model](/system/storage/data-model.md).

* **Single-format cleanup** (post task-212/215; pre-release, no live instances): removed the
  backward-compatibility machinery the storage cluster had carried — the `KeyStorage` enum and
  its per-index field are gone (the stored key **is** `enc(key)` bytes; the JSON write path,
  the type-branching reader, and the `key_storage` alter case were deleted; `to_tantivy` became
  infallible), and `ResolvedField.indexed` lost its rehydration default (always explicit in the
  persisted form). Existing throwaway test environments rebuild from source. See
  [index store](/system/storage/index-store.md).

* **Compact stored key + zstd doc store** (task-212, third of the index-storage-reduction
  cluster; the scale run measured store = 48 MB / 24.5% of the index, ~35 B/doc, almost all of
  it the per-doc `_key` JSON): the hit key is now stored as the same `enc(key)` bytes the delete
  term already computes (one frozen format, `CompositeKey::decode` as its strict inverse, written
  once per doc), and **new indexes compress the doc store with zstd** — measurement showed the
  real cost was entropy, not framing: lz4 match-copying leaves hex/UUID key bytes nearly
  uncompressed and already dedups JSON structure across docs (binary framing alone was only ~3%),
  while zstd entropy-codes the literals for a **~40% store cut** on hex keys. Both changes are
  compat-safe: `key_storage` (ENCODED, serde-default JSON) rehydrates pre-212 `index.json`/backup
  definitions to the exact schema their segments carry (flip = reindex-required alter; readers
  branch on the stored value's own type), and the doc-store compressor persists per index in
  `meta.json`. See [index store](/system/storage/index-store.md).

* **Fast-only numeric/date/IP fields** (task-215, second of the index-storage-reduction cluster):
  a `fast: true` numeric/date/IP field no longer gets an inverted index by default — Tantivy
  serves range, exact-match (already routed through Range), sort/search-after, and exists from
  the columnar store, so the per-doc postings + term-dict entries (worst case: a per-doc-unique
  `ts`) were pure dead weight. New per-field `indexed:` mapping flag (default `fast ? false :
  true` for non-text; TEXT/KEYWORD can't opt out; `indexed: false` without `fast` is rejected as
  unqueryable); flipping it is a reindex-requiring alter. Persisted pre-215 definitions
  (`index.json`, backup manifests) deserialize as `indexed: true`, so existing shards keep the
  exact schema their segments carry. The http_logs scale workload picks the default up
  automatically (`ts`, `response_time_ms`, `response_size`) — the next scale run's
  `growlerdb_index_bytes_component` term/postings split (task-218) quantifies the delta. See
  [data model](/system/storage/data-model.md), [create](/product/functional/index-management/create.md).

* **Index-size observability** (task-218, first of the index-storage-reduction cluster from the
  v0.1.2 scale run): split the `growlerdb_index_bytes_component` `inverted` bucket into `term` /
  `postings` / `positions` / `fieldnorms` so storage work (fast-only numerics task-215, per-field
  record task-216, compact key terms task-212/217) is verifiable against the structure it targets;
  made `growlerdb_index_bytes` (and `DescribeIndex` size) equal the component sum — it silently
  excluded the locator files before; added the missing **delete-debt gauge**
  (`growlerdb_index_deleted_docs`) so a size sample between merges reads in context
  (`NoMergePolicy` keeps superseded docs until compaction); the scale bench snapshot
  (`staged_run.py`) now records the per-component split + segments + debt at every milestone, and
  the console Data hero / Grafana stack pick up the finer components. See
  [observability](/system/observability.md).

* **Realistic http_logs corpus + index:source characterization** (task-159): the scale workload was a
  thin row (6 tiny columns), which made index:source **meaningless** — the fixed ~14 B/row hydration
  locator swamps a 26 B/row source, so the index reads >1x regardless. Redesigned `http_logs` into a
  **realistic CDN/app access log** (~17 fields, ~350-450 B/row; `request_id` key-only in the locator,
  a searchable subset indexed, the rich remainder hydrated from Iceberg) across the generator,
  connector, workload `index.yaml`/`queries.json`/`corpus.py`, and `values-scale.yaml`. Live result on
  a 1.44M-doc run: **index:source ≈ 2.3x** (thin was 2.9x), breakdown inverted 64% / store 25% /
  locator+fast 11%. Honest finding: for *log* search over compressible columns, index > source is
  inherent (Elasticsearch is 1-3x and stores `_source`, which GrowlerDB does not); **sub-1x is the
  large-text regime**. Filed TASK-210 (topk_hydrated latency scales ~linearly), TASK-211 (index_docs
  gauge sampled too coarsely → convergence panel flicker), TASK-212 (composite key stored as verbose
  JSON — the 'store' driver), TASK-213 (large-text workload for the sub-1x regime), TASK-214
  (config-driven workloads — the k8s deploy path is still hardcoded per workload).

* **Helm explicit index schema** (task-209): the k8s node built its shard by **auto-mapping** the
  Iceberg columns (`growlerdb index` with no `--def`), so `clientip` came out `TEXT` (not `IP`) and
  `ts` non-`fast` — which 500'd the http_logs `cidr_clientip` (CIDR needs IP) and `topk_hydrated`
  (sort needs a fast field) queries in the scale run. Not an engine bug: the OpenSearch adapter
  handles both; the shards were handed the wrong schema. Added chart support for an **explicit index
  definition** — `index.fields` (+ optional `key`/`windowing`) renders a def ConfigMap the node
  builds from via `--def`; empty keeps the auto-map path. `values-scale.yaml` now pins the http_logs
  schema (clientip=IP, ts/size fast). See [helm-k8s](/system/deployment/helm-k8s.md).

* **Scale-run deploy hardening** (task-159, from the first live Hetzner attempt): the cluster came up
  and served **223k http_logs docs across 6 shards** end-to-end (query p50 ≈ 455 ms @ 34.8 qps, 16
  workers), but only after clearing a string of setup bumps — captured now so the next run is one
  command. Added **`deploy/k8s/scale-up.sh`** (orders deps → generator-creates-table → chart → connector
  → observability → verify) + **`values-scale.yaml`** (in-cluster variant: static Prometheus scrape,
  no ingress/OIDC, `_search` on, deps creds wired — kills the five `--set` overrides the base
  values-hetzner needed). Fixed: the **deps kustomization** (now pins `namespace: growlerdb`, was
  landing in `default`); the **connector `--nodes`** (hardcoded 3 but the cluster has 6 shards →
  "routing config mismatch" → ingestion stuck at 0 — now 6 + a loud warning, and scale-up.sh sizes it
  to the shard count); the **IaC ssh key 409** ("SSH key not unique" when the key is already in the
  project — new optional `existing_ssh_key_name` reuses it via a data source). Two real product
  findings filed: the http_logs **`cidr_clientip` + `topk_hydrated` queries 500** ("all shards
  failed"). See [scale-test-plan](/quality/scale-test-plan.md).

* **Scale-run image automation + harness smoke test** (task-159 pre-run): closes two of the run's
  gaps. (1) The `growlerdb-connector` + `growlerdb-seed` images — which `release.yml` doesn't build,
  previously an undocumented `docker build && docker push` — now have a **`scale-images` CI workflow**
  (workflow_dispatch + connector/seed path triggers) and a local **`just scale-images`** target. (2) A
  **`just smoke` / `bench/scale/smoke.sh`** smoke test validates every workload offline (parse +
  schema, no cluster; all four pass) and runs a tiny gateway query round when one is up, with a
  Compose full-pipeline runbook in `bench/scale/README.md` — so workload/harness bugs are caught
  before the $200 cloud run. Also updated the scale-test-plan's image prereq to point at the
  automation.

* **Time-windowed scale-test workload** (`http_logs_windowed`, task-159 temporal case;
  [scale-test-plan](/quality/scale-test-plan.md)): a new scale workload alongside the flat `http_logs`
  so the run measures **both** ends of the temporal/non-temporal fork. Same corpus, but the Iceberg
  table is partitioned on an identity `day` column and the GrowlerDB index is **daily-windowed** on
  `ts` — exercising windowed sharding, cold-tier park/revive, event-time query pruning, per-window
  routing, and (via the identity partition) the per-partition reconcile + `growlerdb_source_partition_skew`
  metric. The streaming generator (`deploy/k8s/streaming/generator-windowed.yaml`) advances a
  **synthetic timeline** (`LOGS_PER_DAY` events per synthetic day, not wall-clock, default 750k) so
  day-windows form continuously; the sibling `connector-windowed.yaml` streams it (routing by
  event-time window, resolved from the windowed index def). Workload dir
  `bench/scale/workloads/http_logs_windowed/` (index/queries/corpus); the harness auto-discovers it.
  Kept simple first — key stays `id` (COORDINATES locator); the PREDICATE-locator + partition-routed-key
  variant (retires the location.arr hot floor, ceiling #3) is a follow-up.

* **Source→index convergence assertion** (task-187, [scale-test-plan](/quality/scale-test-plan.md);
  "does GrowlerDB match Iceberg?"): unified the two divergent checks and fixed the load-bearing bug.
  New native `growlerdb_index_docs{index}` gauge (shard live doc count, emitted on the compaction
  tick) — `sum(growlerdb_index_docs)` is GrowlerDB's own index count, so the convergence graph
  (`source rows − index docs → 0`) and the staged harness read **native telemetry**, not the
  scale-test exporter. `bench/scale/convergence_check.py` now counts the source's **DISTINCT ids**
  (Trino, dup-safe) instead of raw `total-records` — the previous raw compare was fooled by duplicate
  PKs (which collapse last-write-wins in the index; the exact 2026-07-04 OOM-restart incident) — and
  samples **real** ids from a match-all page (dataset-agnostic, no `req-N` id-format assumption),
  each asserted to resolve to exactly one doc that hydrates. `staged_run.py` + both Grafana
  dashboards (compose + k8s) migrated to the native metrics. The k8s drain gate `convergence-gate.sh`
  remains the count-only, compaction-racing sibling. Reconcile (task-195) is the systematic repairer.

* **task-194 closed — silent connector row-loss is now loud + regression-tested**
  ([D31](/system/decisions/d31-ingest-loss-guards.md)): the microk8s loss event (one trigger's
  writes landing on some shards but not others while the checkpoint advanced → 6 rows silently lost)
  is resolved by two independent guards that both shipped since it was filed — the connector's
  fan-out **all-settle throw** (a partial shard-write failure fails the trigger, so the Spark offset
  never advances) and the node's **window-covering continuity guard** (a batch whose `from` is
  strictly ahead of a shard trips a non-retryable `CheckpointGap`). Added the missing
  loss-detection regression (`store::tests::task194_missed_shard_write_trips_a_loud_checkpoint_gap`)
  reproducing the 3-shard "1 shard missed a write while the checkpoint advanced" signature and
  asserting it's detected, not sealed. Reconcile (task-195) remains the systematic repairer for any
  residue; the original 6 rows on that (torn-down) cluster stay evidence.

* **Observability redesign — slice 3: Runtime resource panels** (task-208.3): the last "coming soon"
  tiles — busiest-node **CPU / memory / fullest-disk** — are wired to standard `node-exporter`
  `node_*` series. GrowlerDB emits none of these itself, so the cards are marked *external*: when the
  query returns nothing (no metrics stack), they show a **"needs the metrics stack"** state instead
  of a fake 0. The **compose `stack` profile now includes a `node-exporter`** service (host /proc,
  /sys, rootfs mounted read-only) scraped by the bundled collector, so `just stack` lights up Runtime
  locally; production gets it from the k8s observability bundle or a cluster's `kube-prometheus-stack`.
  This completes the [observability redesign](/product/functional/observability.md) (task-208): every
  panel is now live or has an honest fallback. Also `.gitignore`s editor swap files.

* **Observability redesign — slice 2d: source partition skew** (task-208.2): the control-plane
  ingestion sampler now emits `growlerdb_source_partition_skew{index}` — the largest identity
  partition's record count over the mean (`1.0` = even; higher = a hotspot partition), from manifest
  metadata (`partition_record_counts`, no row reads). One `current_plan` per index per tick
  (O(indexes), the same order as the per-index metadata the sampler already reads; cached per
  snapshot), only for identity-partitioned sources. Lights up the Source "partition skew" panel and
  finishes slice 2 — the only remaining "coming soon" tiles are per-node CPU/mem/disk (slice 3,
  task-208.3, the metrics-stack wiring).

* **Observability redesign — slice 2c: login metrics** (task-208.2): the built-in `login` handler
  now records `growlerdb_logins_total{outcome}` (`success` / `bad_credential` / `locked` / `busy`)
  at its four outcome sites in `control_service`. Lights up the Access tab's **Logins** (success
  rate) and **Login failures** (non-success rate — a brute-force / misconfig signal) panels. OIDC
  logins are minted by the external IdP and never reach this handler, so this is built-in-credential
  logins only. **Active sessions** and **Logouts** cards were **dropped**: sessions are stateless
  JWTs (no server-side store to count) and there is no logout endpoint — an honest "we don't have
  that data" rather than a misleading estimate.

* **Observability redesign — slice 2b: per-shard skew** (task-208.2, console only): the Data
  "Per-shard skew" panel is lit up (was "coming soon") from the *existing* per-shard
  `growlerdb_index_bytes` metric — each serving node exposes its shard's bytes, so
  `max by (index) / avg by (index)` across nodes is the shard byte-skew ratio (1.0× = balanced). No
  backend change was needed; the exact per-shard-bytes REST DTO (`per_shard[]`) is deferred until a
  view needs exact per-shard sizes (the skew card is well-served by the ratio).

* **Observability redesign — slice 2a: REST RED metrics** (task-208.2): a single axum middleware
  over the merged `/v1/*` router records `growlerdb_http_requests_total{route,status}` +
  `growlerdb_http_request_duration_seconds{route}`, labelled by the matched route *template*
  (`MatchedPath` — bounded cardinality; unmatched paths bucket as `<unmatched>`). Lights up the
  console's Runtime "API" panels (request rate, server-error 5xx rate, status mix, p95 latency) and
  the Search "query status codes" panel (`route="/v1/search"`) — previously "coming soon". No new
  recording rules were needed: `metrics-exporter-prometheus` renders histograms as summaries with
  default `{quantile}` series, so the p95 panels resolve directly. See
  [observability](/system/observability.md).

* **Observability tab redesign — slice 1** (task-208 / task-208.1; console only): the Observability
  screen is reorganised to *answer the product questions* instead of listing a flat metric grid. A
  persistent Alerts strip sits above sub-tabs — **Search · Runtime · Data · Ingestion · Source ·
  Access** — with clean value+sparkline cards that now support **hover-to-read**, a **ⓘ** help
  popover (self-serve, the product questions live here), and **click-to-expand** detail charts (full
  axes/legend/tooltip); "hero" ECharts overlays show relationships sparklines can't (Iceberg-append
  vs GrowlerDB-index, index-size-by-component, small-file signal). The standalone **Ingestion screen
  is folded in** as the Ingestion sub-tab's per-index → per-shard drill-down; `/ingestion` (like
  `/cluster`) now redirects to Observability. Panels needing new instrumentation (per-endpoint HTTP
  codes, per-partition source stats, per-node CPU/mem/disk, auth counters) render as "coming soon"
  and land in slices 2–3 (task-208.2 / 208.3). See
  [observability](/product/functional/observability.md).

* **Source-health diagnostics** (task-197; retires the metrics deliverable of
  [scale ceiling #2](/quality/known-limitations/scale-ceilings.md); new concept
  [source-health](/system/source-health.md)): GrowlerDB reads the source O(files) on the query path,
  so a source accumulating small files / snapshot bloat silently slows queries — but that's the
  **user's** table to maintain (D30), never GrowlerDB's. New per-index `growlerdb_source_*` gauges
  (data-file count, mean file size, delete files, records, snapshots) let operators *diagnose* it,
  sampled from the current snapshot's `total-*` summary + snapshot count (metadata only, no scan) on
  the control-plane ingestion sampler's tick. `avg_file_bytes` is the small-file signal. The remedy
  (Iceberg compaction / `expire_snapshots`) stays outside GrowlerDB. Manifest/metadata *byte* sizes
  aren't exposed by the reader API (path only), so snapshot count + total bytes are the cheap proxies
  — documented in the runbook. No source-maintenance code added (AC #3).

* **Reconcile backstop: end-to-end drift-repair coverage + doc reconciliation** (task-195, AC #5;
  [D9](/system/decisions/d09-sync-model.md)): the D9 detect-and-repair backstop was already shard-
  scoped, count-gated, TOCTOU-guarded, metricised, and scheduled (Helm CronJob) across earlier PRs —
  this closes the last AC with an e2e drift-injection test
  (`e2e::reconcile_backstop_detects_and_repairs_drift_both_ways`) that deletes indexed rows the source
  still holds (the two *missing* provenances AC #5 names — a deleted indexed row and a skipped source
  row, which reconcile repairs regardless of cause) and plants a stale row the source never held, then
  asserts one `reconcile` cycle re-indexes the missing and removes the stale, idempotently. Also
  refreshed [scale ceiling #4](/quality/known-limitations/scale-ceilings.md) (count-gated reconcile,
  task-198) and the reconcile TOCTOU correctness note (fixed, task-195) to match what shipped, and the
  Recommended-sequencing list now reflects that only source-health metrics (task-197) remain open.

* **Parallel ingest: shard-group connector sets** (task-196, new ADR
  [D32](/system/decisions/d32-parallel-ingest.md); retires
  [scale ceiling #1](/quality/known-limitations/scale-ceilings.md)): ingest now scales out as a
  StatefulSet of W independent connector workers, worker `i` owning shards `{s : s % W == i}` —
  one writer per shard, so the continuity guard needs no writer identity and scaling W is a
  plain roll (regrouping self-heals via the covering guard, proven even with fully pruned
  idempotency stores). Each worker filters the changelog executor-side to its owned rows (~1/W
  per driver) and resumes from its own group's lineage-min. The single connector remains the
  simple low-scale mode; never run both on one table (fails fast, `CHECKPOINT_GAP`). Deploy:
  `deploy/k8s/streaming/connector-set.yaml` + runbook in the streaming README.

* **Ordered checkpoints + window-covering continuity guard** (task-196 foundation; closes
  task-205/206/207): `SourceCheckpoint` now carries the snapshot's lineage-monotone Iceberg
  **sequence number** (snapshot ids are random longs — three paths ordered them numerically
  anyway: resume-min, the prune-index range key, reindex max; each a latent permanent-stall or
  broken-dedup bug). The node's continuity guard relaxes from exact `from == current` to
  **window-covering** (apply iff `from ≤ current < end`; at/behind ⇒ no-op, never a regression;
  strictly-ahead `from` ⇒ `CHECKPOINT_GAP` — loss detection kept), and is re-decided at commit
  under the writer mutex (the stage/commit TOCTOU let racing writers regress the checkpoint).
  Batch-id dedup and deterministic chunk boundaries demote from correctness dependencies to
  optimizations. See [D31](/system/decisions/d31-ingest-loss-guards.md) (refinement section) and
  [checkpoints & exactly-once](/product/functional/ingestion/checkpoints-exactly-once.md).

* **Concurrent per-shard write fan-out** (task-196 floor, AC3): `ShardedWriteClient` commits a
  chunk's sub-batches overlapped (join-all barrier per chunk, all-settle error aggregation) —
  the slowest shard, not the sum of shards, bounds a chunk. Rust `ShardRouter::partition_batch`
  now carries `safe_checkpoint` onto sub-batches (it silently dropped the task-204 prune floor).

* **Control-plane serial-constant fixes** (task-202,
  [scale-ceilings](/quality/known-limitations/scale-ceilings.md) #7): the two hotspots the audit
  ranked highest. (1) **Concurrent status poller** — `collect_ingestion` fetched every shard's
  `GetCheckpoint` *serially* (a fresh connect each), so a sample over hundreds of shards took hundreds
  of round-trips and fell behind its 15s cadence; now a bounded `JoinSet` runs them in parallel. (2)
  **Batched registration** — node registration called `assign_primary` once per ordinal, each a whole
  `registry.json` rewrite (O(N) rewrites of an O(N) file = O(N²) bytes to bring up an N-shard index);
  a new `assign_primaries` mutates all a node's ordinals under one lock and persists once. And the
  `.prev` last-known-good roll is now an **O(1) hardlink** (crash-safe, recovery unchanged) instead of
  a full byte copy on every mutation. Deferred within task-202: resumable/parallel reshard build
  (AC3) and a lag metric for windowed indexes.

* **Bounded compaction** (task-200,
  [scale-ceilings](/quality/known-limitations/scale-ceilings.md) #6): the node's `compact()` merged
  *every* segment in one writer-lock-held call, whose cost grew O(shard size) and blocked all commits
  → admission exhaustion → the connector shed-storm (the documented compaction I/O storm). Now it
  merges bounded **size tiers** in a lock-releasing loop: each pass merges the smallest tier with ≥2
  segments (up to `merge_factor`, default 8) and releases the writer lock before the next pass — so a
  single lock-hold is O(a tier), not O(shard), and ingest commits interleave. `select_tiered_merge`
  (pure, unit-tested) tiers by live-doc magnitude in base 4; the poll re-runs to drain any remainder.
* **Reframed task-197 (source maintenance → source-health metrics)**: corrected the audit's framing —
  GrowlerDB **never** manages the source Iceberg table (D30), so "distributed maintenance" was never
  its job. The scale ceiling (a source that isn't compacted degrades GrowlerDB's O(files) reads) is
  the user's to fix, outside GrowlerDB; GrowlerDB's deliverable is *observability* (source-health
  gauges users can diagnose with). The demo/scale-test maintenance CronJob stays a convenience.

* **Search partition-routing + fan-out cap** (task-199,
  [scale-ceilings](/quality/known-limitations/scale-ceilings.md) #5): a search that AND-pins all of a
  partition-routed index's **keyword** partition fields now routes to the single owning shard (reusing
  the same `ShardRouter` as `get_by_key`) instead of broadcasting to every shard — turning a
  partition-scoped query from O(shards) into O(1) shard, with the slowest-shard-wins tail that
  implies. Correct because Partition routing depends solely on the partition (identifier dropped), so
  no matching key lives elsewhere; only keyword partitions are eligible (a non-keyword type could
  route a string value to the wrong shard), and only AND clauses pin (should/OR and negation fan out).
  Also bounds concurrent per-shard RPCs across all scatter-gathers with a semaphore
  (`GatewayLimits.max_concurrent_fanout`, default 256) so hundreds of shards can't exhaust the
  Gateway's socket budget. True hedging-to-replicas for tail latency stays future work (replicas are
  single-shard today); the existing global deadline + partial-result tolerance caps tail meanwhile.

* **Count-gated reconcile** (task-198, [D9](/system/decisions/d09-sync-model.md)): retires the
  reconcile full-scan ceiling from the [scale audit](/quality/known-limitations/scale-ceilings.md) —
  the backstop no longer reads the whole source every cycle. Two gates: a **whole-index gate** (the
  cluster driver probes each shard `count_only`; if Σ index docs == the source table's `total-records`
  it skips the row-level reconcile entirely — routing-agnostic, covers hash-routed indexes) and a
  **per-partition gate** (for identity-partitioned sources aligned on the index's partition-key
  fields, each node compares per-partition source `record_count` from manifest metadata to the
  partition's index key-count and reads rows only for divergent partitions — memory bounded to one
  partition). New source helper `partition_record_counts` (metadata only, zero row reads) + a
  partition-scoped read. Counts are an exact trigger, not a proof, so `growlerdb reconcile … --full`
  forces a deep sweep (catches compensating stale+missing / dup PKs / wholly-dropped partitions). Non-
  identity / hash-routed / empty-source cases fall back to the whole-shard scan.

* **Reconcile TOCTOU guard + hydration lookup fix** (task-195,
  [D9](/system/decisions/d09-sync-model.md)): closes a correctness race in the online reconcile — it
  read a source snapshot, then deleted indexed keys absent from it, so a key a concurrent ingest
  committed *after* the read could be mistaken for stale and dropped (the continuity guard then kept
  the connector from re-sending it). Fix: capture the shard checkpoint before the scan and, under the
  writer lock, delete only if it hasn't advanced since; otherwise skip the stale-delete that cycle
  (missing-repair still runs, next cycle retries). Also a scale cheap-win: the hydration pass-1
  per-file lookup was a linear scan of the whole plan per requested file (O(files × requested)); now a
  `HashMap` built once per plan (O(1) lookup) — matters once a continuously-appended table
  accumulates many small files between compactions.

* **Scale-ceilings audit** ([scale-ceilings](/quality/known-limitations/scale-ceilings.md)): a
  code-grounded map of where GrowlerDB breaks on the path to a single index over 10s–100s TB. Ranks
  the structural ceilings — single-process connector (blocker), table maintenance keeping compaction
  ahead (blocker), the O(rows) `location.arr` hot floor that cold-tiering doesn't bound, reconcile
  full-scan, search never partition-pruning, the node commit path, and the control-plane serial O(N)
  constants — and frames the temporal-vs-non-temporal fork (windowed time-series is credible at
  100 TB; hash-sharded non-temporal has no cold tier and no fan-out pruning). Notes a reconcile TOCTOU
  correctness bug (fixed separately) and the cheap wins. Backing work filed as task-196…task-204.

* **Scheduled reconcile backstop** (task-195, [D9](/system/decisions/d09-sync-model.md)): the
  shard-scoped reconcile RPC is now driven on a schedule. A new `growlerdb reconcile <index>
  --control-plane <cp>` cluster mode reads the index's shard map + bucket owners from the registry and
  fans a shard-scoped `ReconcileIndex` out to every shard's primary node (mirrors the reshard
  fan-out); any unreachable shard or missing primary fails the run non-zero, so a scheduled run can't
  silently skip a shard. Wired as an **opt-in Helm CronJob** (`reconcile.enabled`, default off;
  `reconcile.schedule` hourly at :23, offset off table maintenance's :17). The CronJob pod only
  orchestrates over gRPC — the nodes do the source read + repair — so it needs no Iceberg creds, just
  an ephemeral work dir for the embedded engine it opens at startup.

* **Ingest observability + exact-count-at-drain gate** (task-194 AC6/AC7,
  [D31](/system/decisions/d31-ingest-loss-guards.md)): the loss guards now come with the signals to
  see them. **AC6** — the connector's ingest metrics were printf-only, so they rotated out of the
  10 MB container log exactly when the silent loss was investigated; they're now Prometheus metrics
  (`growlerdb_connector_rows_read_total` / `_rows_expected_total` / `_under_reads_total`, per-shard
  `_shard_acks_total`, `_stream_restarts_total`, `_write_retries_total`, a checkpoint gauge) off a
  tiny in-process HTTP server, plus a bundled log4j2 config that quiets the Spark/Hadoop/Iceberg INFO
  flood. The node counts `growlerdb_dedup_hits_total` + `growlerdb_checkpoint_gap_total`. Alert rules
  fire on any under-read, checkpoint-gap rejection, drift repair, or sustained idle-lag. **AC7** — the
  streaming convergence check is upgraded from lag-based/row-count to an automated
  **exact-count-at-drain** gate (`convergence-gate.sh`): drain, then assert index doc count ==
  source `COUNT(DISTINCT id)` (distinct, because duplicate ids collapse last-write-wins), optionally
  racing a live Iceberg compaction (`--with-maintenance`).

* **Shard-scoped reconcile backstop** (task-195, [D9](/system/decisions/d09-sync-model.md)): the
  drift backstop — the one mechanism that both detects and repairs silently lost/orphaned rows
  regardless of cause — is now **shard-aware**. The old `Engine::reconcile` was CLI-only,
  whole-table, and `ShardId::single`: run against one shard of a sharded index it would re-index the
  other shards' keys into it, destroying placement. Added a node Admin RPC **`ReconcileIndex`** that
  restricts the comparison to the keys the shard owns (registry-vended bucket map + ordinal, the same
  `ShardRouter::owns` the gateway/connector route by), reads the source streamed (peak memory
  O(owned keys)), applies drift to the live shard (delete stale, re-index missing), and emits
  `growlerdb_drift_stale_total` / `growlerdb_drift_missing_total` / `growlerdb_drift_reconcile_total`
  (labelled by index + ordinal, alert-on-nonzero) plus a bounded affected-key log. The gateway
  returns `not_routed` (a reconcile is per-shard). Follow-ups: wiring it into a scheduled CronJob
  (with a CLI/control-plane caller that vends each node's ordinal + map) and per-partition/windowed
  scoping for very large tables.

* **Ingest silent-loss guards** (task-194, [D31](/system/decisions/d31-ingest-loss-guards.md)):
  defense-in-depth against a changelog under-read after a post-D30 validation run found a shard
  silently short 6 rows. Five guards, AC1–AC5: a connector **expected-row-count gate** (assert
  changelog rows ≥ `Σ added-records` over the window's append snapshots before any write — an
  under-read now throws with no cursor advance, and self-heals on the re-read); a node
  **checkpoint-continuity guard** (`DocBatch` gains an optional `from_checkpoint`; a new batch whose
  `from` ≠ the shard's current checkpoint is refused non-retryably as `CHECKPOINT_GAP`); **lockstep**
  checkpoint advance (empty sub-batches are sent and a no-work batch advances the checkpoint via a
  redb-only commit, so shards don't drift); **lineage-ordered head** (resolve the trigger head from
  the `main` ref, not `ORDER BY committed_at`); and **retryable `RESOURCE_EXHAUSTED`** admission
  backpressure at the connector plus the node holding its admission permit for the true commit
  duration (sheds load under a compaction I/O storm instead of spawning unbounded concurrent
  commits). Metrics/log hygiene (AC6) and chaos/scale exact-count-at-drain gates (AC7), plus the
  sharded scheduled **reconcile** backstop (task-195), are tracked as follow-ups.

* **Deploy fixes from the post-D30 microk8s validation run** (task-184): metrics-exporter
  defaults switched to `growlerdb.http_logs` + `pyiceberg[s3fs,pyarrow]` (newer pyiceberg needs
  the explicit pyarrow extra for snapshot scrapes); Polaris **pinned to 1.5.0** (unpinned `latest`
  moved under us mid-validation — newer Polaris enforces catalog auth, so the runbook now shows the
  chart credentials flags); seed-runbook env names corrected to the script's actual `POLARIS_*` /
  `AWS_*`. Validation results are recorded in the backlog (task-184).

* **`PREDICATE` location strategy + duplicate-PK detection** (task-184 slice 4, D30 — the last
  planned slice before the v3-gated `row_id`): the index definition gains a per-index
  **`location_strategy`** option (`COORDINATES` default | `PREDICATE`), a plain YAML field so it
  rides the existing `definition_yaml` wire surface (gRPC + REST) unchanged; carried onto
  `ResolvedIndex`; a strategy change is reindex-only in the alter plan. A `PREDICATE` shard
  stores **no location data**: the commit path skips file interns, `location.arr` writes, and the
  `_locid` fast-field value — the schema keeps the field for uniformity (no field-ordinal
  divergence across strategies; an unpopulated u64 fast column is ~free) — and the commit
  collapses to the two-phase Tantivy→redb ordering (asserted by trace). Hydration resolves
  requests **per strategy** (`hydrate::resolve_requests`): `PREDICATE` sends every present key
  locator-less straight to the pruned key-equality scan (pass-2 machinery promoted to primary),
  after a local key-presence probe so missing keys stay `NotFound` before any catalog connect;
  `refresh_locators` is a no-op, the re-map loop isn't spawned, and predicate hydration never
  counts toward `growlerdb_stale_locators_total` (it isn't a refresh). **Honest scope** is stated,
  not detected: resolve emits a warning (now surfaced via `CreateIndexResponse.warnings` + the
  REST create DTO + the log) that unclustered high-cardinality keys degrade to broad scans;
  auto-detection is deferred until `row_id` exists. **Duplicate-PK detection** lands on the shared
  key-scan path (`index_batch`): a second distinct row for a matched key increments the new
  `growlerdb_duplicate_pks_total` and logs a rate-limited warning naming the key; the winner is
  deterministic — highest `(file, position)` among scanned rows — never silent map last-wins.
  Updated [locators & segments](/system/storage/locators-segments.md) (strategies section),
  [hydration](/product/functional/hydration.md) (how to choose),
  [create](/product/functional/index-management/create.md) (the option + response warnings), and
  [D30](/system/decisions/d30-layered-locator.md)'s status (remaining: `row_id` + auto-detection).

* **Compaction re-map + live-file bitmap** (task-184 slice 3, D30 `coordinates` strategy): source
  compaction no longer costs hydration a per-read locator refresh. The file-intern layer gains a
  **dead-file bitmap** (parallel `dead_files` key-set table in `aux.redb` — the `files` rows stay
  immutable; flags are permanent tombstones, durable before visible, load at open) that hydration
  consults at locator resolution: a locator into a dead file skips the doomed point read and goes
  straight to the pass-2 fallback (`hydrate` requests now carry `Option<RowLocator>`; `None` =
  known stale). A **background re-map loop** on the serving node (`spawn_locator_remap`,
  `--remap-interval-secs`, default 45 s, 0 disables; windowed serve shares one loop across hot
  windows; never on replicas) polls the table's current plan through the snapshot-pinned plan
  cache (new `IcebergReader::current_plan`), diffs the live file set against the shard's interned
  live files, and on a `replace`: marks disappeared files dead, **column-projects only the key
  columns + positions** of the added files (`read_file_key_rows`, projection-masked — measured ≪
  file bytes; delete-bearing files skipped, they heal lazily), and bulk-patches slots key-sorted
  in chunks (`Shard::remap_locations`; writer lock per chunk; interns commit before patches +
  fsync). Interleaving safety: a slot is patched **only while it still points at a dead file**, so
  ingest upserts / lazy refreshes are never overwritten with the older rewritten row, and
  verify-and-fallback stays the net. New SLIs `growlerdb_locator_remap_events_total`,
  `growlerdb_locator_remapped_rows_total`, `growlerdb_locator_dead_files`. The task-184 acceptance
  **compaction-under-hydration regression test** (engine `compaction_remap.rs`, real parquet +
  the diff fed to `remap_shard` directly): with re-map, all 200 keys re-resolve with
  `growlerdb_stale_locators_total` delta = 0 and slots at the new file/positions; bitmap-only,
  hydration stays correct via fallback with 0 doomed pass-1 reads and the counter counts all 200.
  Updated [locators & segments](/system/storage/locators-segments.md) (re-map/bitmap section),
  the [scale test plan](/quality/scale-test-plan.md) (compaction↔hydration now self-heals; assert
  stale ≈ 0 + flat hydration p99), and [D30](/system/decisions/d30-layered-locator.md)'s status.

* **Single layered layout; keyed locator table deleted** (task-184; unreleased-product reset):
  GrowlerDB has no released data, so the back-compat machinery slice 2 carried was removed — the
  `ShardLayout` enum + `layout` marker, every legacy/layout-1 branch, the dual-format backup
  stamping (manifest **format reset to 1 = the layered format**; the version field and
  refuse-newer check are kept as release hygiene), and the **keyed redb `LOCATOR` table with its
  dual-writes** (`aux.redb` is now meta + batch idempotency + file interns only — this is where
  the D30 size win physically lands). Its second job, the **live-key set**, moved to the index:
  `key_count` / `reconcile_partition` enumerate the `_keyenc` term dictionary over the partition's
  raw-bytes prefix range with a **per-term liveness probe** (postings + alive bitset), so
  deleted-but-unmerged keys are never counted (tested under real delete debt), and drift's
  presence check (`doc_generation`, whose generation half was vestigial) became a live-term
  `contains_key`. `RowLocator` slimmed to `{iceberg_file, row_position}` (in-memory only; its
  `partition` duplicated the key and `snapshot_seen` had no reader);
  `growlerdb_locate_keys_total` lost its `path` label. Task-191 (legacy-shard migration trigger)
  is cancelled. Updated [locators & segments](/system/storage/locators-segments.md) (single
  layout, live-key enumeration), [index store](/system/storage/index-store.md), [backup
  format](/system/storage/backup-format.md) (format 1 is the layered format), and
  [D30](/system/decisions/d30-layered-locator.md)'s status.

* **D30 layered locator activated** (task-184, slice 2 PR II): hydration on layout-2 shards now
  resolves through the layers — key term → live doc → `_locid` fast field → `location.arr` slot →
  interned file path — and **new shards (and reindex rebuilds) default to layout 2**; existing
  legacy shards keep the keyed-redb path untouched until reindexed (trigger: task-191). Locator
  refresh patches the array slot in place (interns first, then patch — a reachable slot never
  references un-durable state) while keeping the dual-written keyed entry in step. Backups of
  layout-2 shards ship `location.arr` and stamp **manifest format 2** (legacy shards stay format
  1; pre-D30 binaries refuse format 2 instead of corrupting the fast field on resume); replica
  refresh always re-fetches the array (it's patched in place, so size can't detect change); cold
  park keeps the array **local** beside `aux.redb` — the parked window hydrates via the layered
  path over read-through segments. New `growlerdb_locate_keys_total{path=layered|legacy}` counter
  for the scale run. Rewrote [locators & segments](/system/storage/locators-segments.md) (layers,
  layouts, extended crash contract), updated the [index store](/system/storage/index-store.md)
  structure and the [backup format](/system/storage/backup-format.md) (format 2), and refreshed
  [D30](/system/decisions/d30-layered-locator.md)'s status.

* **D30 locator layers landed inert** (task-184, slice 2 PR I): the index store now has the
  layered-locator storage layers — a dense `location.arr` array (12 B/entry: interned `u32`
  file id + `u64` row position; locator ID = slot index), a `files` intern table in `aux.redb`,
  a `_locid` u64 fast field in every derived schema, and a per-shard **layout marker** (absent/1 =
  legacy, 2 = layered). Layout-2 shards write the layers at commit (array fsync ordered **before**
  the durable Tantivy commit; an upsert reuses + patches a live key's slot; the keyed locator
  stays dual-written), but new shards still default to layout 1 and **the read/hydration path is
  unchanged** — activation (layered reads, refresh, backups, new-shard default) is the next PR.
  See [D30](/system/decisions/d30-layered-locator.md);
  [locators & segments](/system/storage/locators-segments.md) still describes the serving path
  accurately.

* **Snapshot-pinned plan cache + shared catalog client for hydration** (task-184, D30
  foundations, slice 1): the lookup service no longer reconnects to the Iceberg REST catalog on
  every `GetByKey` — it holds a shared, lazily-connected reader (invalidated on source failure so
  the next RPC reconnects), and hydration pass 1's unpredicated current-snapshot plan is cached
  **pinned to the snapshot id** (one catalog call per hydrate to learn it; manifest reads only on
  a snapshot advance). New `growlerdb_plan_cache_hits_total` / `_misses_total` counters make
  planning cost observable for the scale tests. The predicate fallback stays uncached. See
  [query execution](/system/query-execution.md) and
  [D30](/system/decisions/d30-layered-locator.md).

* **Positional parquet point reads for hydration** (task-184, D30 foundations, slice 1): pass 1
  of hydration no longer streams a data file's Arrow reader from row 0 to reach a located row —
  it reads the parquet footer once per file, scopes the read to the row group(s) holding the
  requested positions, and applies a row selection to the exact rows, over the same opendal
  `FileIO` stack. A late row in a large file now costs ~one row group instead of ~the whole file.
  Verify-and-fall-back semantics unchanged; delete-bearing files keep the delete-applying
  streaming read. See [query execution](/system/query-execution.md) and
  [D30](/system/decisions/d30-layered-locator.md).

* **Temporal key support** (task-184, D30 foundations, slice 1): DATE/TIMESTAMP key fields now work
  end-to-end — a new `Value::Ts` variant (canonical **epoch micros UTC**, wire `ts_micros`, key
  encoding type tag 5) flows from extraction (Arrow Date32/Date64/Timestamp any-unit/any-tz) through
  key encoding/routing (Rust + JVM connector, parity-tested) into the index, and the hydration
  fallback now builds a typed Iceberg predicate for timestamp/date keys instead of an unfiltered
  full-table scan. Prerequisite for the hydration predicate path (locator-v2). See
  [data model](/system/storage/data-model.md).

* **Update**: the [backup format](/system/storage/backup-format.md) manifest now carries a
  **format version** (task-184 slice 1, [D30](/system/decisions/d30-layered-locator.md)
  foundations): introduced at **1** (pre-versioning manifests default to 1); every consumer
  refuses a newer format with a clear `UnsupportedFormat` error, so when the D30 locator layers
  change the backup/cold-bundle contents and bump it, old binaries fail loudly instead of
  mis-restoring. No behavior change for current backups.

* **Creation**: added [D30 — layered locator](/system/decisions/d30-layered-locator.md)
  (task-184): identity (key terms) / reference (locator-ID fast field) / location (dense-array
  store) with per-index location strategies (`coordinates` / `row_id` / `predicate`); no
  constraints imposed on the source table. Driven by the scale-run finding (locator ≈ 84% of index
  bytes, wholesale staleness on compaction), a 3-lens adversarial design review that rejected a
  v3-first draft, and decision spikes (dense array 12.0 B/entry vs redb-u64 53.9; re-map key
  lookups ~1M/s). Refined [D13](/system/decisions/d13-locator.md), scope-noted
  [D28](/system/decisions/d28-iceberg-v3.md), and flagged
  [locators & segments](/system/storage/locators-segments.md) as current-behavior-until-landed.

* **Scale-test fixes**: generator now emits **unique PKs across restarts** (per-run id prefix) —
  it was resetting `n=1000` each restart, producing duplicate ids that broke GrowlerDB's
  unique-identifier assumption (search/index/hydrate resolved different rows → a "wrong status"
  bug; not an engine fault). Connector given a real Spark driver heap (8g) + local[6] — the
  default ~1g heap OOM'd (exit 52) under load. Connector sustains ~17k docs/s; locator (redb) is
  ~84%% of index bytes (task-184). Interim Hetzner cluster torn down after the quick pass.

* **Scale-test gap-fill**: kube-state-metrics + a Stability dashboard row (errors/restarts/OOM),
  a source→index **convergence check** (`bench/scale/convergence_check.py`), **Trino** deployed for
  the GrowlerDB-vs-Iceberg-alone comparison (`bench/scale/compare_trino.py`), a **staged-run driver**
  (`bench/scale/staged_run.py`), and an engine **index-size breakdown** metric
  (`growlerdb_index_bytes_component` = inverted/fast/store/locator) with a stacked dashboard panel.

* **Scale test → http_logs**: switched the streaming stack (generator/connector/maintenance) from the
  compressible `docs` demo to realistic high-cardinality `http_logs` (unpartitioned, hash-routed).
  Source is now ~28 B/row (was ~7); index:source ratio 7.6x -> ~2.4x. Revised the scale-test plan with a
  staged step-up protocol (ingest 1k->10k rec/s; storage 1/10/100 GB; extrapolate 100k/s + 1 TB) and
  per-milestone query capture. Gap tasks: 185 (staged protocol), 186 (Trino comparison), 187
  (convergence), 188 (connector scaling), 189 (stability).

* **Update**: added the `growlerdb_index_bytes` gauge (task-182) — on-disk index size per shard,
  emitted on the serving loop alongside `segments_live`. (The hydration-latency histogram and
  ingestion-lag gauge already existed in telemetry; they emit when hydration runs / the CP scrape is
  fixed — see task-182.)

* **Update**: expanded the [scale test plan](/quality/scale-test-plan.md) with a **Metrics &
  observability** section + **operational prerequisites** — the engine-native metrics we have vs the
  gaps (hydration-latency histogram, index bytes, ingestion-lag gauge → task-182), the in-cluster
  metrics exporter for the Iceberg/source side (`deploy/k8s/streaming/metrics-exporter.yaml`:
  dataset size, ingest rate, source→index lag, compaction events), Prometheus-for-headless, the
  image-build prereqs, and the per-test Iceberg-maintenance cadence (~10 min, not hourly).

* **Update**: the scale-test [IaC](/system/deployment/iac.md) (`deploy/iac/`, Terraform + hcloud) and
  the pluggable [harness](/quality/scale-test-plan.md) (`bench/scale/`, http_logs default) now exist —
  updated both concepts from "planned" to their concrete locations (task-159).

* **Creation**: added the [scale test plan (Hetzner)](/quality/scale-test-plan.md) under quality — the
  repeatable, time-boxed scale run: what is measured, run duration/phases, the Hetzner cluster shape,
  the run-duration cost model, and the IaC/repeatability; cross-linked from
  [scalability](/quality/scalability.md). Scoped to a **hard $200/run budget** (pinned to 4× CCX33
  cloud nodes, time-boxed; dedicated-NVMe multi-TB runs are a separate higher-budget effort). Uses a
  **pluggable, dataset-agnostic harness** (OpenSearch Benchmark "workload" contract via the
  OpenSearch adapter) with **`http_logs`** as the default and Wikipedia/MS MARCO/synthetic as
  drop-in datasets.

* **Update**: release versioning is now **tag-derived + auto-incremented** (task-156) — added
  [D29](/system/decisions/d29-release-versioning.md) and refreshed the
  [build & release](/system/build.md) pipeline section (dispatch `bump`, `scripts/next-version.sh`,
  `0.1.0` GA baseline, immutable + moving image tags).

* **Update**: landed OKF-update **enforcement** (task-181) — a PR template + `CONTRIBUTING` rule +
  a CI conformance check (`okf/check.sh`) that every concept carries a `type`; recorded it in
  [workflow.md](/workflow.md).

* **Creation**: populated the **quality** area — [overview](/quality/overview.md) (correctness
  guarantees), [tests](/quality/tests/index.md) (7 suites), [scalability](/quality/scalability.md),
  [reliability](/quality/reliability.md), [security](/quality/security/index.md),
  [CI & gates](/quality/ci-and-gates.md), [release readiness](/quality/release-readiness.md),
  [how issues are handled](/quality/issues.md), and
  [known limitations](/quality/known-limitations/index.md) (task-180).

* **Creation**: populated [deployment](/system/deployment/index.md) (compose, helm-k8s, single-binary,
  sharded-ha, iac) and the [decision records](/system/decisions/index.md) — 28 ADRs (D1–D28) migrated
  from the wiki, self-only — completing the **system** area (tasks 178 + 179).

* **Creation**: populated system internals — [storage](/system/storage/index.md) (index-store,
  data-model, locators-segments, cold-bundles, backup-format, catalog-metadata),
  [distribution](/system/distribution.md), [query-execution](/system/query-execution.md), and
  [observability instrumentation](/system/observability.md) (task-177).

* **Creation**: populated [runtime](/system/runtime/index.md) — 7
  [components](/system/runtime/components/index.md) (control-plane, gateway, node, connector,
  compactor-maintenance, console-ui, cli-engine) and the
  [dependencies](/system/runtime/dependencies/index.md) (Polaris + REST catalog, S3/MinIO,
  Kafka/Redpanda, Postgres, Spark/Trino, LGTM) (task-176).

* **Creation**: began the **system** area — [architecture](/system/architecture.md) (components, data
  flow, JVM/Rust boundary), [repository layout](/system/git-repo.md) (Cargo workspace + subprojects),
  and [build & release](/system/build.md) (toolchain, CI, release pipeline) (task-175).

* **Creation**: populated [non-functional](/product/non-functional/index.md) requirements — latency,
  throughput & freshness, scale & cost, availability, durability & recovery, consistency, tenancy,
  security & compliance, openness (v1 design targets, flagged as pending validation) — completing the
  **product** area (task-174).

* **Creation**: populated the write/admin/ops functional capabilities —
  [auth](/product/functional/auth/index.md) (login/logout/tokens/mtls),
  [RBAC & tenancy](/product/functional/rbac-and-tenancy.md),
  [user management](/product/functional/user-management.md),
  [index management](/product/functional/index-management/index.md) (create/alter/drop/reindex/compact/
  backup/aliases), [ingestion](/product/functional/ingestion/index.md) (streaming/cdc/exactly-once),
  and windowing / cold-tiering / replicas / observability (task-173).

* **Creation**: populated the read path — [search](/product/functional/search/index.md) (query,
  syntax, facets, suggest, sort/paging, highlight, export) and
  [hydration](/product/functional/hydration.md) (task-172).

* **Creation**: populated [actors](/product/actors/index.md) (platform-admin, search-analyst,
  ingest-operator, app-developer, service-account) and [use cases](/product/use-cases/index.md)
  (IoT telemetry lead, RAG, search-backed app, adjacent domains) — competitor framing stripped
  (task-171).

* **Creation**: populated [product overview](/product/overview.md) and the nine
  [interfaces](/product/interfaces/index.md) — gRPC, REST, UI, CLI, client SDKs, OpenSearch adapter,
  SQL UDFs, website, git repo (task-170).
* **Initialization**: created the OKF bundle scaffold (task-169) — root concepts
  ([overview](/overview.md), [glossary](/glossary.md), [workflow](/workflow.md)), the
  product / system / quality directory skeleton with listing stubs, and this log.
