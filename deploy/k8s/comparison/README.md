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

## Driver Jobs — load runs IN-CLUSTER, never over port-forward

The shake-out proved a `kubectl port-forward` load driver reports garbage (500ms/25s, timeouts): the
localhost tunnel, not the engine, becomes the bottleneck. So every load/convergence/freshness driver
runs as an **in-cluster Job** rendered from [`driver-job.template.yaml`](driver-job.template.yaml): an
initContainer `git clone`s the branch under test, the main `python:3.12-slim` container `pip install`s
the driver's deps and runs one `bench/scale` command against the in-cluster Services
(`gdb-growlerdb-gateway:8080`, `opensearch:9200`, `trino:8080`, `prometheus:9090`), and the
orchestrator reads the machine-readable result back from the pod's stdout (a marker-delimited JSON it
persists host-side for `capture.py`). `compare_run.py` renders, applies, waits on, and slurps each
Job; `compare_run.py --self-check` renders + validates the manifest offline. The only host-side step
is the final `capture.py` metrics scrape (read-only Prometheus, not a load path).

## Running the comparison

Orchestrated by [`bench/scale/compare_run.py`](../../../bench/scale/compare_run.py) (sequential:
generate once → GrowlerDB phase → transition → OpenSearch phase → finalize). Shakedown first.

```sh
# 0. provision + bring up GrowlerDB stack (deps, observability, Trino, generator @ SPAN_DAYS=7, http_logs)
cd deploy/iac && terraform apply            # 6x ccx43, non-windowed http_logs (terraform.tfvars)
export KUBECONFIG=deploy/iac/kubeconfig.yaml
SPAN_DAYS=7 WORKLOAD=http_logs IMAGE_TAG=<dev-sha> deploy/k8s/scale-up.sh

# 1. dry-run the orchestration (touches nothing), then the 10 GB shakedown, then the 50 GB run
python bench/scale/compare_run.py --plan --scale shakedown
python bench/scale/compare_run.py --scale shakedown
python bench/scale/compare_run.py --scale full

# 2. export raw logs + push corpus/results to the Hetzner bucket (bucket `growlerdb` @ nbg1, verified)
#    endpoint/bucket default correctly; creds auto-read from ~/.ssh/hetzner-gdb-storage. Keep the
#    NDJSON dir under the repo (a Docker-shared path) for the mc upload mount. Port-forward MinIO first.
python bench/scale/corpus_export.py --seeds 42,43,44,45,46,47 --rows-per-shard 20000000 --out-dir bench/scale/ndjson
kubectl -n growlerdb port-forward svc/minio 9000:9000 &   # for the Iceberg warehouse mirror
NDJSON_DIR=bench/scale/ndjson RUN_ID=2026-...-50gb bash bench/scale/artifacts.sh push
```

`--phase <name>` reruns a single phase (generate / growlerdb / transition / opensearch / finalize) for
resumability.

**Shakedown must confirm these cluster-specific bits** (the orchestrator's kubectl selectors are best
guesses from the Helm chart — verify names on the first run and correct `compare_run.py`):
generator Deployment name (`growlerdb-generator`), node StatefulSet name (`gdb-growlerdb-node`),
Service names the driver Jobs hit (`gdb-growlerdb-gateway:8080`, `opensearch:9200`, `trino:8080`,
`prometheus:9090`), and the maintenance CronJob name (`growlerdb-iceberg-maintenance` — `compact_source`
deploys `deploy/k8s/streaming/maintenance.yaml`, which scale-up.sh does not, then triggers a one-shot
Job from it). Also confirm the Data Prepper metrics path (`/metrics/prometheus`) once the pod is up.

The **Trino/Iceberg-scan baseline (`compare_trino.py`) is deferred this pass**: its predicate set +
day-pruning target the old windowed schema (`id`/`request`/`day`), but non-windowed `http_logs` has
`request_id`/`path` and no partitions — it needs a predicate refresh before it can run as a driver
Job. `phase_growlerdb` logs the skip rather than launching a doomed Job.

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
- [x] Run orchestration — `compare_run.py` (sequential flow, `--scale` shakedown/full, `--plan`),
      `up.sh`/`down.sh` deploy wiring, `corpus_export.py` (raw-log NDJSON) + `artifacts.sh` (bucket
      push/restore, env-driven). tfvars set to 6× ccx43 / http_logs.
- [x] **`--engine opensearch` mode for `convergence_check.py` — DONE.** OpenSearch `_count` == source
      `COUNT(DISTINCT request_id)`; the source count uses the Trino REST API (`TRINO_URL`) so the check
      runs inside a driver Job (no kubectl). `phase_opensearch` gates on it.
- [x] **In-cluster driver Jobs — DONE.** `driver-job.template.yaml` + `compare_run.py` render/apply/
      wait/slurp; no port-forward in the load path. `compare_run.py --self-check` validates offline.
- [ ] Shakedown-verify the cluster-specific selectors (see "Running the comparison" above).
- [ ] Refresh `compare_trino.py` predicates for the non-windowed `http_logs` schema (deferred, above).
- [ ] `_bulk` fallback path (labeled) in case the CDC source underperforms.
- [ ] Hetzner artifact bucket + S3 credentials (Kira to create when it's time).
