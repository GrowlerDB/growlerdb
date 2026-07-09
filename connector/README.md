# GrowlerDB Spark connector (JVM)

The **ingestion connector** ([D10](../../wiki/21-decisions.md#d10-ingestion-runtime-spark-until-rust-matures)): a Spark Structured Streaming job that reads the Iceberg **changelog** and writes `DocOp`s to a GrowlerDB Node over the **Write gRPC** service (`growlerdb serve`). It is a separate JVM subproject — **not** part of the Rust cargo workspace.

> Status: the pipeline is wired (task-11). `ChangelogReader` (task-63) → `ChangelogMapper` (task-13) → `WriteClient` Write gRPC, orchestrated by `ConnectorJob` and the `spark-submit` entrypoint `ConnectorApp`. See the [connector plan](../../backlog/docs/task-11-plan.md).

## Pipeline

`ConnectorJob.runOnce` is one micro-batch: read the changelog window since the last
checkpoint (a GrowlerDB `SourceCheckpoint` = Iceberg snapshot id) up to the table's current
snapshot, reduce it to a `DocBatch` (last-write-wins per key, ordered by
`_change_ordinal`), and commit it through the Write gRPC. The batch carries the
checkpoint + a deterministic `batch_id`, so the Node commits the write and the
checkpoint atomically and a replay is a no-op.

`ConnectorApp` is the `spark-submit` entrypoint. The catalog is configured by the
submitter (`--conf spark.sql.catalog.<name>.*`), so it works against a Hadoop catalog
(local) or Polaris REST (dev stack). It runs one batch by default, or a `foreachBatch`
Structured Streaming loop with `--stream`.

**Exactly-once / resume (task-16):** on startup (unless `--start` overrides it) the
connector reads the checkpoint the Node has durably committed via the `GetCheckpoint`
RPC and resumes the changelog from there. Because the Node commits the write and the
checkpoint in one atomic transaction and dedups on `batch_id`, a window re-read at the
boundary is a no-op — so a connector or Node restart neither loses nor double-applies
data.

**Deferred (not silently skipped):** Spark-on-K8s `spark-submit` packaging and
streaming checkpoint/restart *resumability at scale* are verified only in a real
cluster.

## Stack

- **JDK 21** (pinned via `mise` — see `mise.toml`), **Maven** (a prerequisite; system `mvn` or `brew install maven`).
- **Spark 4.0** + **Iceberg 1.10** (`iceberg-spark-runtime-4.0_2.13`) — `provided` (supplied by `spark-submit`).
- **gRPC-Java** stubs generated from the shared `growlerdb.v1` protos in [`crates/growlerdb-proto/proto/`](../crates/growlerdb-proto/proto/growlerdb/v1) — single source of truth with the Rust server.

## Build

```sh
cd connector
mise install           # JDK 21
mise exec -- mvn verify # generate stubs, compile, test, shade the fat jar
```

Outputs `target/growlerdb-connector-<version>.jar` — the `spark-submit` fat jar (bundles gRPC; excludes the `provided` Spark/Iceberg).

## Tests

`mvn verify` runs only the fast unit tests (mapping, proto round-trip). The heavy
tests are `@Tag`-gated out by default (they pull the Spark/Iceberg runtime), like the
Rust `#[ignore]` live tests:

```sh
just connector-it    # integration: Spark local mode → pipeline → in-process Node stub
just connector-e2e   # cross-process: JVM connector → real `growlerdb serve` (Rust) → searchable
```

`connector-e2e` builds the `growlerdb` binary (`cargo build -p growlerdb-cli`) and spawns it; the
test skips (it does not fail) if the binary is absent.
