# Comparison systems — OpenSearch + Data Prepper (Iceberg CDC)

Manifests and parity decisions for the GrowlerDB-vs-OpenSearch head-to-head. Plan and fairness
charter: [`bench/scale/comparison-plan.md`](../../../bench/scale/comparison-plan.md). These deploy
into the same `growlerdb` namespace and read the same Polaris REST catalog + MinIO as GrowlerDB and
Trino (see `deploy/k8s/observability/trino.yaml` for the shared catalog/S3 coordinates).

## Contents

- `opensearch.yaml` — OpenSearch cluster (StatefulSet + Service) + the index mapping (ConfigMap) and
  a setup Job that creates the index with parity settings.
- `data-prepper.yaml` — Data Prepper deployment + the Iceberg-source→OpenSearch-sink pipeline
  (snapshot-poll CDC, CoW-only). **Pending schema verification against Data Prepper 2.15 docs.**

## Isolation decision — run systems SEQUENTIALLY on the full cluster

Both systems read the same Iceberg table, but they are **not** benchmarked at the same time. Running
GrowlerDB and OpenSearch concurrently on shared nodes lets one starve the other and makes latency
numbers meaningless. Instead: bring up one system, ingest, run the full query + QPS matrix, capture,
tear it down, then the next — each gets the **identical full cluster**. This is the cleanest reading
of the charter's "equal total budget" rule (equal = the same hardware, not a split), at the cost of
running the 100 GB ingest twice. Node-partitioning (3+3) was rejected: it halves each system's
resources and still shares disk/network. *(This supersedes the "equal budget, partitioned" phrasing
in the plan; the plan's fairness charter #2 will be updated to say "sequential on the full cluster".)*

## Parity settings (held constant, disclosed in the report)

- **Shards:** OpenSearch `number_of_shards: 6` = GrowlerDB `shard_count: 6`.
- **Replicas:** matched to GrowlerDB's replication factor for the run; set once and disclosed (read
  replicas add query capacity, so this must match, not be tuned per system).
- **refresh_interval:** fixed at `1s` (OpenSearch default) and disclosed. Not tuned per run — raising
  it lifts bulk throughput but worsens freshness, which is a trade we report, not a knob we spin.
- **Analyzer:** OpenSearch `standard` analyzer on TEXT fields. This tokenizes + lowercases but does
  **not** stem — which matches GrowlerDB's default lexical (no stemming), so it is fair parity, not a
  handicap. Disclose it.
- **`_source`:** left ENABLED on OpenSearch (its normal mode). We do NOT disable it for "parity" —
  instead we measure storage footprint and report both index-only and full-document top-K, since
  `_source`-vs-hydrate is a first-class result. If ever disabled, that is disclosed as a changed mode.
- **Autocomplete (resolved):** compare each system's *intended* typeahead path on `user_id`
  (whole-value prefix). GrowlerDB serves suggest via the **native `POST /v1/suggest`** (a bounded
  live scan of the ordinary term dictionary — no dedicated structure; the OpenSearch compat adapter
  has no suggest route). OpenSearch uses a dedicated **`completion` FST field** (`user_id_suggest`,
  populated via `copy_to`) queried through `_search` `suggest`. This is a genuine architectural
  asymmetry (GrowlerDB reuses the index; OpenSearch builds an extra structure) — disclose it and
  report the completion field's added storage, like `_source`-vs-hydrate. The harness therefore hits
  two different endpoints per system, which is also disclosed. Query pair: see the
  `autocomplete_user_id` entry in `bench/scale/workloads/http_logs/queries.comparison.json`.

**Field-type mapping** (from `bench/scale/workloads/http_logs/index.yaml`):

| GrowlerDB | OpenSearch |
|---|---|
| `ts` LONG fast | `long` |
| `method`,`status`,`user_id`,`region` KEYWORD | `keyword` |
| `path`,`user_agent` TEXT (positions) | `text` (positions default) |
| `referer` TEXT record:FREQ | `text`, `index_options: freqs` |
| `client_ip` IP | `ip` (CIDR term queries) |
| `response_time_ms`,`response_size` LONG fast | `long` |
| `request_id` (key-only, not searchable) | document `_id` via Data Prepper `identifier_columns` |

## GrowlerDB query path (smoke-verified)

The neutral driver queries GrowlerDB through its OpenSearch `_search` adapter, which is **off by
default** and lives on the **gateway** (`gateway --opensearch`), not on `serve`'s embedded REST
front. The scale deploy already enables it (`gateway.opensearch: true` in
`deploy/helm/growlerdb/values-scale.yaml`), so the comparison run gets it for free. Autocomplete uses
the **native `/v1/suggest`** (also fronted by the gateway) — there is no `_search`-adapter suggest
route. Local smoke confirmed both: driver ran all query kinds 0-error against a built `http_logs`
index, and `topk_hydrated` showed the expected hydration-path latency (the `_source`-vs-hydrate cost).

## TODO (tracked)

- [x] **Corpus (Phase 1.5) — DONE.** Generated `http_logs` at ~50 GB (no permissive real dataset fit
      log-shaped + commercial + scale). `corpus.py` enhanced with realistic distributions; methodology
      + validation report in `bench/scale/synthetic-corpus.md`. Schema/mapping/queries unchanged (the
      generated corpus keeps the 17-field shape), so autocomplete stays on `user_id`.

- [x] Data Prepper Iceberg-source pipeline — `data-prepper.yaml` **verified end-to-end** in a local
      smoke (Polaris + MinIO + OpenSearch, real 500-row Iceberg table): CDC converged exactly
      (500 -> 500, _id = request_id) and completion-field autocomplete populated via copy_to. Fixes
      the smoke found: experimental plugin must be enabled; `catalog` is per-table. CoW-only remains
      the standing constraint to honor at scale.
- [x] Autocomplete parity — resolved above (`user_id` completion field + `/v1/suggest`).
- [x] Pin OpenSearch + Data Prepper images: OpenSearch **2.19.1** (smoke-verified) + Data Prepper
      **2.15.1** (latest 2.15.x; both tags confirmed on Docker Hub). Restate the exact tags in the report.
- [ ] `_bulk` fallback path (labeled) in case the CDC source underperforms.
