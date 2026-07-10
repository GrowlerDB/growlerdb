---
title: Getting started
layout: default
nav_order: 2
---

# Getting started

This tutorial takes you from nothing to your **first search** against a real Iceberg table, using
the local Compose stack (GrowlerDB + MinIO object storage + Apache Polaris catalog + the LGTM
observability stack). Time: ~10 minutes, mostly the first image build.

## Prerequisites

You need **Docker with the Compose v2 plugin** and **[`just`](https://github.com/casey/just)** — the
stack runs entirely in containers, so no language toolchains are required. Run it on a **Linux host
or a VM, or macOS with Docker Desktop** — *not* inside a container (Docker bind mounts won't resolve
there). **~4 GB RAM** is enough.

### Ubuntu / Debian

```sh
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 docker-buildx just git curl
sudo systemctl enable --now docker
# optional: run docker without sudo (log out/in afterwards)
sudo usermod -aG docker "$USER"
```

### macOS

```sh
brew install --cask docker   # Docker Desktop — bundles Compose v2 + buildx; launch it once
brew install just
```

### Then, on either OS — one `/etc/hosts` entry

So the `curl` hydration calls *you* run on the host can reach the in-container object storage by the
name the stored file paths use:

```sh
echo "127.0.0.1 minio" | sudo tee -a /etc/hosts
```

(The console doesn't need this — it talks to the gateway, which reaches MinIO inside the Compose
network. It's only for host-side hydration.)

## 1. Bring up the full stack

From the repo root:

```sh
just stack
```

This builds the GrowlerDB image, brings up MinIO + Polaris, **seeds a sample `growlerdb.docs`
Iceberg table** (3 rows), then starts the control plane, a node, the gateway, and Grafana/LGTM.
The node builds the `docs` index from the table, serves it, and registers with the control plane.

When it settles, the **console** is at <http://localhost:8081> and Grafana at <http://localhost:3000>.

## 2. Your first search (REST)

The gateway serves the Engine API at `:8081`. Search returns ranked **document coordinates**:

```sh
curl -s localhost:8081/v1/search -H 'content-type: application/json' \
  -d '{"query":"title:iceberg","limit":5}'
```

You get the matching keys + scores — no row contents, just the **coordinates**:

```json
{
  "hits": [
    { "coordinates": { "identifier": [{ "name": "id", "value": "doc-2" }] }, "score": 0.814 }
  ],
  "total": 1, "shards_scanned": 1, "shards_total": 1
}
```

Now hydrate the authoritative row from Iceberg by that key:

```sh
curl -s localhost:8081/v1/keys:get -H 'content-type: application/json' \
  -d '{"keys":[{"identifier":[{"name":"id","value":"doc-2"}]}]}'
```

```json
{
  "rows": [
    { "key": { "identifier": [{ "name": "id", "value": "doc-2" }] },
      "fields": { "id": "doc-2", "title": "iceberg search",
                  "body": "fast full text search over apache iceberg" } }
  ]
}
```

That round-trip — **search returns keys, keys hydrate to rows from the lake** — is the core of
GrowlerDB.

## 3. Explore in the console

Open <http://localhost:8081>. Pick the **`docs`** index in the top-left selector, type a query, and
hit **Search**:

![GrowlerDB console — Search: body:search over the docs index returns two hits](img/console-search.png)

> **Tip:** in the console's Lucene box a bare word (`search`) queries the index's *default* field —
> qualify it with a field, e.g. `body:search` or `title:iceberg`, to match. Click a hit to hydrate
> the full row in the drawer.

- **Search & Explore** — run queries, inspect hits, hydrate rows in the drawer, export JSON/CSV.
- **Indexes** — every index with docs / shards / sync lag / backup state; **Create index** points at
  a source table and introspects its schema:

  ![GrowlerDB console — Indexes: the docs index, active, 3 docs, in sync](img/console-indexes.png)

- **Observability** — native SLI panels (query rate/errors/latency, hydration, ingestion lag) with a
  health roll-up; the **Ingestion** tab shows per-index source-head vs. committed-checkpoint lag:

  ![GrowlerDB console — Observability: live SLIs, query-latency chart, and SLI cards](img/console-observability.png)

## 4. Use the OpenSearch adapter (optional)

The stack enables the [OpenSearch-compatible adapter](opensearch-adapter), so OpenSearch clients
work against the same data:

```sh
curl -s localhost:8081/docs/_search -H 'content-type: application/json' \
  -d '{"query":{"match":{"body":"search"}},"size":5}'
```

You get OpenSearch-shaped documents — `_id` from the key, `_source` hydrated from Iceberg:

```json
{
  "hits": { "hits": [
    { "_index": "docs", "_id": "doc-2", "_score": 0.451,
      "_source": { "id": "doc-2", "title": "iceberg search",
                   "body": "fast full text search over apache iceberg" } },
    { "_index": "docs", "_id": "doc-3", "_score": 0.451, "_source": { "id": "doc-3", "...": "..." } }
  ] },
  "_shards": { "total": 1, "successful": 1, "failed": 0, "skipped": 0 }
}
```

So an existing OpenSearch/Elasticsearch client can point at GrowlerDB unchanged.

## 5. See the source in Iceberg with Trino (optional)

GrowlerDB keeps **Iceberg as the system of record** and indexes it. To see that source data directly
— and compare it with what GrowlerDB returns — bring up **Trino** (SQL over the *same* Polaris
catalog + MinIO the seed wrote). It's gated behind the `trino` profile (Trino is a JVM, so it's not
in the base stack):

```sh
docker compose -f deploy/compose/docker-compose.yml --profile trino up -d trino
```

Query the same tables GrowlerDB indexes (`iceberg.<namespace>.<table>`):

```sh
docker compose -f deploy/compose/docker-compose.yml exec trino \
  trino --execute "SELECT id, title, body FROM iceberg.growlerdb.docs ORDER BY id"
```

```
"doc-1","welcome","hello world, welcome to growlerdb"
"doc-2","iceberg search","fast full text search over apache iceberg"
"doc-3","hydration","search returns keys that hydrate authoritative rows"
```

Those are exactly the rows a GrowlerDB search hydrates — `body:iceberg` returns `doc-2` above, and
here you can see the full row in Iceberg. You can also **add a row** straight to the lake:

```sh
docker compose -f deploy/compose/docker-compose.yml exec trino trino --execute \
  "INSERT INTO iceberg.growlerdb.docs VALUES ('doc-4','trino insert','added to iceberg via trino sql')"
```

It's now in Iceberg; a GrowlerDB **reindex** (`POST /v1/index:reindex`) picks it up so the same search
surfaces it — the full **source → index → search** loop, with Trino and GrowlerDB reading one source
of truth.

## 6. Tear down

```sh
just stack-down
```

## Troubleshooting

- **First `just stack` is slow (~10 min).** It compiles the GrowlerDB image once; subsequent starts
  reuse the cached image and take seconds.
- **Search returns `0 results` in the console.** Select the **`docs`** index (top-left) and qualify the
  term with a field — `body:search`, not a bare `search` (a bare term only matches the default field).
- **`keys:get` / hydration errors on the host** (`nodename nor servname` / connection refused): add the
  `127.0.0.1 minio` `/etc/hosts` entry from Prerequisites — host-side hydration reads object storage by
  that name.
- **Ports already in use** (`8081`, `3000`, `9000`): stop the conflicting service or `just stack-down`
  a previous run first.
- **Console shows "Unknown"/degraded health right after start:** the node is still building the `docs`
  index from the table — give it a few seconds and refresh.

## Where to next

- Index your own table: define an index over its columns + key, drop the [index definition](reference)
  in via the console's **Indexes → Create** (it introspects your source schema).
- [Migrate from Elasticsearch/OpenSearch](migration-from-elasticsearch).
- [Deploy on Kubernetes](https://github.com/GrowlerDB/growlerdb/blob/main/deploy/helm/growlerdb/README.md).
