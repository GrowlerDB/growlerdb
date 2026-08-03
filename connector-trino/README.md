# GrowlerDB Trino connector (JVM)

A **Trino plugin** that adds a `search` **polymorphic table function**: run a GrowlerDB full-text query
from SQL, get back the matching document **keys** + a relevance score, and JOIN them against the source
Iceberg table in Trino. Boolean retrieval + ranking run in GrowlerDB; the row data comes from your
lakehouse — the same coordinate → hydrate model as the rest of GrowlerDB, expressed as a JOIN. It is a
separate JVM subproject — **not** part of the Rust cargo workspace.

> Status: **experimental** (`0.0.0`). Read-path only: the function runs a query and returns keys +
> `growlerdb_score`; there is no write/DDL surface. The endpoint is passed per call (not catalog
> config), so one catalog can query any GrowlerDB gateway.

## The `search` table function

Registered as `<catalog>.system.search` under a catalog whose `connector.name=growlerdb`:

```sql
SELECT e.*
FROM lake.events e
JOIN TABLE(growlerdb.system.search(
       endpoint => 'gateway-host',              -- GrowlerDB read endpoint (a Gateway, usually)
       port     => 50061,                       -- gRPC port (default 50061)
       query    => 'body:error AND env:prod',   -- Lucene/KQL query string
       "limit"  => 1000)) m                     -- max hits (default 1000; "limit" is quoted — reserved word)
  ON e.id = m.id
ORDER BY m.growlerdb_score DESC;
```

- **Arguments:** `ENDPOINT` (varchar, required), `PORT` (integer, default `50061`), `QUERY` (varchar,
  required), `LIMIT` (integer, default `1000`).
- **Returned columns:** one column per **key field** (the composite key — partition fields then
  identifier fields, typed VARCHAR/BIGINT to match the index) plus **`growlerdb_score`** (double). The
  result schema is learned from the index's key fields at analysis time.
- **How it runs:** the function calls the endpoint's `Search` gRPC (a Node, or usually a
  `growlerdb gateway` that fans across shards/windows) and streams back the ranked keys. You JOIN those
  keys against the authoritative Iceberg table — Trino reads the row data, GrowlerDB did the search.

## Requirements

- **Trino 483** (`trino-spi` 483).
- **JDK 25** (`maven.compiler.release=25`; pinned via `mise` — see `mise.toml`). Trino 483's SPI is
  compiled for Java 25, so this connector is built and loaded against that runtime — a newer line
  than the Spark connector's JDK 21.
- A reachable **GrowlerDB read endpoint** (a `growlerdb gateway` gRPC address).

## Get the jar (no build)

Download the fat jar attached to a [GitHub Release](https://github.com/GrowlerDB/growlerdb/releases/latest)
(`growlerdb-trino-connector-<version>.jar`, with a `.sha256`) — a single self-contained plugin jar,
no host-side Java/Maven build.

## Build (from source)

```sh
cd connector-trino
mise install                 # JDK 25 + Maven
mise exec -- mvn -q package   # → target/growlerdb-trino-connector-<version>.jar (fat jar)
```

## Install into Trino

1. Create a plugin directory `plugin/growlerdb/` in your Trino install and copy the fat jar into it —
   the standard Trino plugin layout. The jar bundles its gRPC runtime, so no other dependency jars
   are needed.
2. Add a catalog file `etc/catalog/growlerdb.properties` with just:

   ```properties
   connector.name=growlerdb
   ```

   The search endpoint/port are function arguments, so no per-endpoint catalog config is needed — one
   `growlerdb` catalog can query any gateway.
3. Restart Trino and call `TABLE(growlerdb.system.search(...))` as above.

## Notes & limitations

- **Read-only.** No tables, DDL, or writes — only the `search` table function. Ingestion is via the
  [Spark connector](../connector/README.md) / the Iceberg changelog, not Trino.
- **Keys, not rows.** The function returns coordinates + score; you JOIN to the Iceberg table for row
  data (or query GrowlerDB's own hydration path). This keeps Iceberg the system of record.
- **gRPC stubs** are generated from the shared `growlerdb.v1` protos in
  [`crates/growlerdb-proto/proto/`](../crates/growlerdb-proto/proto/growlerdb/v1) — one source of truth
  with the Rust server.
- Experimental and unversioned (`0.0.0`); the function signature may change before a stable release.
