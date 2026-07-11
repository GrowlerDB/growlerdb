# Streaming pipeline + chaos drills

Adds continuous ingestion to the [K8s deploy](../README.md): a **generator** appends to
`growlerdb.docs` and the **Spark connector** reads its changelog and continuously updates the `docs`
index (routing each row to its shard via the live control-plane). Then we inject chaos and assert the
index stays correct (exactly-once, no loss/dup, auto-resume).

> Prereq: the `docs` index is deployed and serving (see [../README.md](../README.md)). The connector
> resumes from each Node's committed checkpoint, so it streams snapshots *after* the index's build.

**Workload-driven manifests**: `generator.template.yaml` + `connector.template.yaml`
are RENDERED per workload — `python3 bench/scale/harness.py render <workload> --shards N` derives
the generator (the workload's own `corpus.py` mounted, its `stream()` driven) and the connector
(`--table/--identifier/--fields/--index` from the workload's `index.yaml`, `--nodes` sized to the
shard count) from one definition under `bench/scale/workloads/<name>/`. `http_logs` is the flat
hash-routed workload; `http_logs_windowed` the **daily-windowed** temporal one (partitioned-by-day
source, synthetic advancing timeline). Switching is configuration (`WORKLOAD=<name>
deploy/k8s/scale-up.sh`), never a manifest edit. See `okf/quality/scale-test-plan.md`.

## 1. Build + push the connector image (amd64)

```sh
docker build -t ghcr.io/growlerdb/growlerdb-connector:dev -f deploy/k8s/streaming/connector.Dockerfile .
docker push ghcr.io/growlerdb/growlerdb-connector:dev
```

(The build compiles the connector fat jar from the repo's protos and bakes it into `apache/spark`.)

## 2. Deploy the pipeline

```sh
python3 bench/scale/harness.py render http_logs --shards 6   # → bench/scale/.render/http_logs/
kubectl -n growlerdb apply -f bench/scale/.render/http_logs/generator.yaml
kubectl -n growlerdb apply -f bench/scale/.render/http_logs/connector.yaml
kubectl -n growlerdb logs deploy/growlerdb-connector -f   # watch "[trigger N] committed M op(s) → snapshot S"
```

(Or the whole ordered bring-up in one command: `WORKLOAD=http_logs deploy/k8s/scale-up.sh`.)

## 3. Confirm steady-state streaming

The source grows (generator) and the index tracks it (connector). Watch the index doc count climb:
```sh
kubectl -n growlerdb run c --rm -i --restart=Never --image=curlimages/curl:latest --command -- sh -c \
  'curl -s -X POST http://gdb-growlerdb-gateway:8080/v1/search -H "Content-Type: application/json" -d "{\"query\":\"*\",\"limit\":1}" | grep -o "\"total\":[0-9]*"'
# run a few times — total should increase
```

## 4. Chaos drills (the point)

For each, the **convergence invariant** is: once the dust settles, the index doc count equals the
source's **distinct-id count** — no silent loss or duplication. (Distinct id, not raw row count: the
generator can re-emit duplicate ids under load, and the engine collapses them last-write-wins, so the
index is legitimately below the row count.) Gate it automatically with **exact-count-at-drain** —
which also survives an under-read
window that lag-based checks miss (an under-read advances the cursor with lag reaching ~0, yet rows
never applied):

```sh
# stop the generator, drain, assert index total == source COUNT(DISTINCT id)
deploy/k8s/streaming/convergence-gate.sh
# ...and race it against a live Iceberg compaction (the changelog-read-vs-compaction window):
deploy/k8s/streaming/convergence-gate.sh --with-maintenance
```

The connector's own drain signals are metrics: scrape
`growlerdb_connector_rows_read_total` vs `growlerdb_connector_rows_expected_total`, per-shard
`growlerdb_connector_shard_acks_total`, `growlerdb_connector_under_reads_total`,
`growlerdb_connector_stream_restarts_total`, and the node's `growlerdb_checkpoint_gap_total` —
alerts fire on any nonzero (see `deploy/compose/lgtm/growlerdb-alert-rules.yml`).

- **Node crash mid-stream** (🔴 prime suspect): `kubectl -n growlerdb delete pod gdb-growlerdb-node-1
  --force --grace-period=0`. Watch the connector log — it reconnects to the restarted node (new IP)
  and resumes; the index `total` keeps climbing. `WriteClient` fails fast on a per-call deadline and
  retries with backoff (idempotent `batch_id`), so the write resumes in place — or, if the node stays
  down, the stream errors → the pod auto-restarts → resumes exactly-once from each Node's checkpoint.
  Either way, no silent freeze.
- **Connector crash**: `kubectl -n growlerdb delete pod -l app=growlerdb-connector`. On restart it
  re-reads `GetCheckpoint` and resumes exactly-once — no gap, no dup.
- **Catalog down**: `kubectl -n growlerdb delete pod -l app=polaris`. Ingestion pauses (changelog read
  fails) while the backlog grows; when Polaris returns, the connector drains it via **bounded
  catch-up** and search keeps serving throughout.
- **Big backlog**: scale the connector to 0 for a while (`kubectl -n growlerdb scale deploy
  growlerdb-connector --replicas=0`), let the source grow, then back to 1 → it catches up in bounded
  batches and converges.

## Parallel connector set

`connector-set.yaml` runs the same connector as a **StatefulSet of W workers** for horizontal
ingest scale-out: worker `i` (the pod ordinal, via `GROWLERDB_WORKER_ID`) owns shards
`{s : s % W == i}`, filters the changelog to its rows executor-side, writes only its shards, and
resumes from its own group's lineage-min checkpoint. Rules:

- **Either/or, never both**: don't run the set and the single connector (`connector.yaml`) on one
  table simultaneously. Two writers on one shard fail fast at the node (`CHECKPOINT_GAP` — loud,
  no silent loss). Migrate in either direction by scaling one to 0 first; resume always comes from
  the nodes' durable checkpoints, and the window-covering guard makes the handoff plain
  resume-from-min — no drain barrier needed.
- `--workers` in the pod args **must equal `spec.replicas`**, and `replicas` must be ≤ the shard
  count (an extra worker owns no shards and crash-loops with an actionable message).
- Scaling W up/down needs no coordination (regrouping self-heals), but keep `--max-commit-rows`
  uniform across the set and roll the whole set after a reshard (routing is read at startup).
- `convergence-gate.sh` works unchanged (it asserts per-shard checkpoints against the source).
- Restore caveat: restoring a **subset** of shards from backup rewinds their checkpoints below
  already-pruned idempotency floors; content replays stay safe under the covering guard, but
  prefer whole-index restores.

After each: run `deploy/k8s/streaming/convergence-gate.sh` — it stops the generator, drains, and
asserts the index `total` equals the source `COUNT(DISTINCT id)` exactly (exit non-zero on any
divergence, so it's a CI/soak regression gate). Add `--with-maintenance` to drain against a live
compaction.

## Iceberg table maintenance

The generator commits a tiny append every few seconds, so `growlerdb.docs` accumulates thousands of
small data files + manifests — which slows **hydration** (search reads the authoritative rows from
Iceberg via the index's row locators; many tiny files + fat metadata = slow planning/opens). A
**CronJob** runs the standard Iceberg maintenance via spark-sql against the Polaris catalog:
`rewrite_data_files` (compaction — the hydration win), `rewrite_manifests`, and `expire_snapshots`.

Safe for the index: a rewrite is a non-append snapshot the connector skips, and GrowlerDB self-heals
stale row locators on hydration (`engine/hydrate.rs`) — no reindex needed. Retention (env on the
CronJob) keeps the connector's recent resume snapshot: `EXPIRE_OLDER_THAN_HOURS`, `RETAIN_LAST`.

```sh
kubectl -n growlerdb apply -f deploy/k8s/streaming/maintenance.yaml   # hourly CronJob
kubectl -n growlerdb create job --from=cronjob/growlerdb-iceberg-maintenance maint-now   # run now
kubectl -n growlerdb logs -l job-name=maint-now -f   # watch the rewrite/expire counts
```

> `remove_orphan_files` is intentionally omitted — it lists the data dir via the Hadoop FileSystem,
> which can't speak S3FileIO/MinIO's `s3://` ("No FileSystem for scheme s3"). `expire_snapshots`
> reclaims expired data files; run orphan sweeps manually with hadoop-aws/s3a configured if needed.

## Teardown
```sh
kubectl -n growlerdb delete -f bench/scale/.render/http_logs/connector.yaml -f bench/scale/.render/http_logs/generator.yaml
kubectl -n growlerdb delete -f deploy/k8s/streaming/maintenance.yaml   # + the CronJob
```
