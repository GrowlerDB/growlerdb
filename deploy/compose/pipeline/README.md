# Streaming demo: generator → Kafka → Iceberg → Spark → GrowlerDB

A self-contained, steady-stream pipeline so you can watch data flow end to end and see GrowlerDB's
**ingest rate** and **lag** move in real time.

```
generator.py ──JSON──▶ Redpanda (Kafka)  topic: telemetry
            ──▶ sink.py  (consume → micro-batch append)
            ──▶ Iceberg  growlerdb.telemetry_stream   (the lake landing zone)
            ──▶ Spark Structured Streaming + GrowlerDB connector  (changelog read → Write gRPC)
            ──▶ GrowlerDB node index  telemetry_stream
            ──▶ search it in the console + watch the metrics
```

Each hop is a real component: a Kafka-compatible broker (Redpanda), an Iceberg table on MinIO via
Apache Polaris, and the shipped GrowlerDB **Spark connector** (`ConnectorApp --stream`) — the same
ingestion path used in production, resuming exactly-once from the node's committed checkpoint.

## Run it

```sh
just pipeline          # deps + Polaris bootstrap + build the connector jar + bring up everything
```

Then open:

- **Console** — <http://localhost:8081>
  - **Search**: query the live index, e.g. `status:critical`, `metric:vibration`, `message:bearing`,
    `reading:[800 TO 1000]`. New readings keep arriving.
  - **Ingestion** screen: per-shard **lag** = source head − committed checkpoint. As the generator
    runs you'll see lag rise between connector micro-batches and fall as each batch commits.
  - **Indexes**: `telemetry_stream` doc count climbing.
- **Grafana** — <http://localhost:3000> → *GrowlerDB SLIs* dashboard
  - **Ingestion throughput (doc-ops/s)** = `rate(growlerdb_ingested_docs_total[1m])` — the **ingest
    rate**, driven by the connector's writes.
  - Query rate / latency / hydration panels populate as you search.

Tear down with `just pipeline-down` (wipes the demo's data volumes — see Notes).

## Tuning (env on the compose services)

| Service | Var | Default | Effect |
|---|---|---|---|
| `generator` | `RATE` | `50` | readings/sec produced to Kafka |
| `sink` | `BATCH_SIZE` / `FLUSH_SECS` | `500` / `5` | Iceberg append batch size / max flush interval |
| `connector` | (Spark trigger) | `5s` | how often the connector pulls a changelog micro-batch |

Raise `RATE` to push ingest throughput up; the lag sawtooth tracks the connector's `5s` trigger.

## Pieces

| File | Role |
|---|---|
| `generator.py` | Stage 1 — synthesize IoT readings → Redpanda |
| `sink.py` | Stage 2 — Redpanda → append to Iceberg `growlerdb.telemetry_stream` |
| `telemetry-stream.yaml` | the index definition the node builds + serves (mounted at `/defs`) |
| `Dockerfile` | one image for the generator + sink (pyiceberg + kafka-python) |
| `../pipeline.override.yml` | compose override that points the node at `telemetry_stream` (deterministic — no env) |

The Spark connector stage runs the project's `connector/` fat jar (built by `just pipeline`) against
the Polaris REST catalog; see [`connector/README.md`](../../../connector/README.md).

## Notes

- The node **retries** its initial index build until `growlerdb.telemetry_stream` exists, so startup
  ordering is race-free — the sink creates the table on first message.
- The connector resumes from the node's durable checkpoint, so a restart neither loses nor
  double-applies readings (exactly-once; `batch_id` dedups a boundary re-read) — verified by
  restarting the node mid-stream and watching the connector pick up where it left off.
- Running the connector inside `spark-submit` against Polaris + MinIO needed a few connector
  hardenings (all in `connector/`): force the gRPC `dns` scheme for in-network `host:port` channels,
  merge `META-INF/services` in the shaded jar (so the `dns` resolver survives), a Spark `rate`-source
  heartbeat trigger (the Iceberg streaming source's offset log goes through the table's S3 FileIO,
  which rejects the local checkpoint), and `cache-enabled=false` on the catalog so each trigger sees
  the newest snapshot. These make the connector work in any containerized / object-store deployment,
  not just this demo.
- **Polaris uses a persistent metastore** (a volume-backed Postgres, `polaris-db`, bootstrapped once
  by the idempotent `polaris-bootstrap` one-shot). This is a guardrail: if Polaris keeps the catalog
  in memory and is restarted, the source table is recreated with a new identity and the node's
  persisted index is silently orphaned — search returns rows that won't hydrate ("Row not found").
  Persisting the catalog means an accidental Polaris bounce no longer wipes it. For UI-only changes,
  use `just ui-reload` (recreates only the gateway, never the node/Polaris). `just pipeline-down -v`
  is still the way to reset the demo from scratch (drops MinIO + index + metastore volumes). The
  durable, product-side guard for a genuinely recreated source is tracked in task-114.
- `ts` is indexed as a numeric `LONG` (sortable + range-queryable, e.g. `ts:[from TO to]`), not a
  `DATE`, so the console's auto-detected **time-filter** control doesn't appear for it — a known demo
  limitation (wiring `ts` as a DATE needs the millis-vs-micros encoding aligned end to end).
