# Scale-test harness (pluggable workloads)

Dataset-agnostic driver for the GrowlerDB scale test. Spec:
[`okf/quality/scale-test-plan.md`](../../okf/quality/scale-test-plan.md). Provision the cluster with
the [IaC](../../deploy/iac/README.md), deploy GrowlerDB via the Helm chart, then drive load + queries
here.

## The workload contract

A workload is a directory `workloads/<name>/` with three parts (the OpenSearch Benchmark "workload"
shape):

| File | What |
|---|---|
| `index.yaml` | the GrowlerDB index definition (schema / field mapping) |
| `corpus.py` + `workload.yaml` `corpus:` | how to get the data into Iceberg (download a public corpus, or generate) |
| `queries.json` | the query mix — OpenSearch `_search` DSL bodies + `weight` |

**Adding a dataset is a new directory, not a code change.** `http_logs` is the default — a **realistic
CDN/app access log** (~17 rich fields, ~350-450 B/row; `request_id` key + searchable subset, the rest
hydrated from Iceberg), hash-routed by `request_id`. The rich rows are what make **index:source**
ratios meaningful: a thin row is swamped by the fixed ~14 B/row hydration locator (measured ~2.3x for
this log workload — high-cardinality searchable fields the columnar source compresses away; sub-1x is
the large-text regime). **`http_logs_windowed`** is the temporal variant (partitioned by
day, daily-windowed — cold-tier park/revive + event-time query pruning). `synthetic` is a download-free
seeded fallback; `wikipedia` is a drop-in example (corpus loader TODO).

For the **streaming** path, the k8s manifests are RENDERED from the workload itself —
`python harness.py render <workload> --shards N` → `.render/<workload>/{generator,connector}.yaml` —
so the generator runs the workload's own `corpus.py` `stream()` (the windowed one advances a
**synthetic timeline**, `LOGS_PER_DAY` events per synthetic day, so day-windows form as the run
proceeds) and the connector's `--table/--identifier/--fields/--index` derive from `index.yaml`.
`WORKLOAD=<name> deploy/k8s/scale-up.sh` does the whole ordered bring-up; switching workloads is
configuration, never a manifest edit.

## Use

```sh
# Queries hit the gateway's OpenSearch _search adapter (`gateway --opensearch`).
export GROWLERDB_OS_URL=http://<gateway>:8081
# Corpus loaders use the same POLARIS_*/AWS_* vars as bench/gen_telemetry.py.

python harness.py list
python harness.py validate http_logs          # parse-only; no cluster needed
python harness.py load http_logs --fraction 0.1   # corpus -> Iceberg (then build the index)
python harness.py query http_logs --duration 120 --concurrency 16 --out report.json
python harness.py run  synthetic --fraction 1.0   # load + query (download-free)
```

`load` writes the corpus to Iceberg and prints the `index create` step; GrowlerDB's connector then
indexes the table. `query` runs the weighted mix, reporting per-query **p50/p95/p99** and aggregate
**throughput (QPS)** to stdout + a JSON report.

## Smoke test (before a run)

`just smoke` (or `bench/scale/smoke.sh`) validates **every** workload offline (parse + schema, no
cluster) and — if a gateway is up at `GROWLERDB_OS_URL` — runs a tiny query round per workload. Run it
before the cloud run to catch workload-def / harness bugs cheaply.

For a **full-pipeline smoke** on the local Compose stack (exercises load → connector → windowing →
convergence end to end, download-free via `synthetic`):

```sh
just stack                                    # GrowlerDB + deps + LGTM (needs 127.0.0.1 minio in /etc/hosts)
export GROWLERDB_OS_URL=http://localhost:8081
export POLARIS_URI=http://localhost:8181/api/catalog POLARIS_CATALOG=growlerdb POLARIS_CREDENTIAL=root:s3cr3t
export AWS_ENDPOINT_URL_S3=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
python harness.py load synthetic --fraction 0.001         # tiny corpus -> Iceberg
growlerdb index create -f workloads/synthetic/index.yaml  # then run the connector to index it
python harness.py query synthetic --duration 10
INDEX=synthetic TABLE=synthetic python convergence_check.py   # index doc count == source distinct ids
```

## Staged perf run (the GA graphs)

`query` is one continuous scale; the **staged driver** steps ingest rate + storage size and captures
the full metric set per milestone so the scale questions get graphs, not a single point. Run it from a
kubectl-capable host with a **gateway + Prometheus port-forward** up (or in-cluster URLs):

```sh
# port-forwards: gateway → :8080, prometheus → :9090 (see scale-up.sh output)
export GATEWAY_URL=http://localhost:8080 PROM_URL=http://localhost:9090
export WORKLOAD=http_logs INDEX=http_logs NAMESPACE=growlerdb OUT=results.json
python staged_run.py            # ingest step-ups (1k→10k/s) then storage milestones (1/10/100 GB)
python results_table.py results.json > results.md   # milestone×metric table + 1 TB / 100k-rps projection
```

At each **storage milestone** the driver freezes ingest, drives the query mix (via `harness.py query`),
runs the **convergence check**, and — if `deploy/trino` is up (it's in the observability bundle) — the
**GrowlerDB-vs-Iceberg(Trino)** comparison (`compare_trino.py`; `TRINO_TABLE` defaults to
`INDEX` so a windowed run compares against its own table). `results_table.py` is pure post-processing
(no cluster): the measured milestones plus a **linear projection** to 1 TB / 100k rec/s, each labelled
measured vs modeled with a ±residual band (honesty convention). `compare_trino.py` /
`staged_run.py` also run standalone if you only want one phase.

**Parallelize ingest:** a single generator pod caps ~8.5k rows/s. Raise the generator
replica count with `GENERATORS=N deploy/k8s/scale-up.sh` (or `harness.py render --generators N`) — each
pod's rows carry a unique `run` prefix, so replicas produce disjoint ids and aggregate rows/s scales
~linearly. Use this to feed GrowlerDB above the single-pod ceiling. When an ingest step still can't
keep up, the **write-path panels** (write latency `growlerdb_write_duration_seconds`, queue depth, and
the scraped connector rows-read/retries) localize the bottleneck to the commit path vs node compute vs
the connector, and **Loki** (in the bundle) has the pod logs alongside.

**Snapshot size = commit latency:** the generator's `BATCH` is one Iceberg snapshot = the
connector's commit size (it cuts only at snapshot boundaries, so it can't sub-divide a snapshot).
Commit latency is ~O(snapshot): write p95 ~880ms at 10k-row snapshots vs ~4.5s at 150k. So drive a
rate with a **bounded `BATCH` (≤ the connector's 50k `maxCommitRows`) + short `SLEEP_S`**, not a huge
`BATCH` — `staged_run.py`'s ingest steps do (a 300k `BATCH` self-inflicts ~9.5s p99 commits). Bounding
the source snapshot is the practical lever until the connector can split within a snapshot.

## Notes

- **`http_logs` corpus is large** (~247M docs / ~31 GB) — download it separately (see
  `workloads/http_logs/workload.yaml` `source`) and point `CORPUS_PATH` at it; use `--fraction` /
  `BENCH_ROWS` to stay within the run's budget/time-box.
- **Repeatable:** public corpora are fixed and the synthetic generator is seeded (`BENCH_SEED`), so
  runs are comparable and regression-gated regardless of dataset.
- **Scope:** GrowlerDB's own numbers go here; a head-to-head vs ES/OpenSearch (same corpus, real
  `opensearch-benchmark`) is a separate report, not the OKF bundle.
- Deps for the corpus loaders: `pyiceberg`, `pyarrow` (as in `bench/`); the query driver + `validate`
  use only the stdlib plus `PyYAML`.
