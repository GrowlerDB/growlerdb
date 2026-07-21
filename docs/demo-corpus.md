---
title: Demo corpus (movies)
layout: default
nav_order: 5
parent: Getting started
---

# The movie demo corpus

The seeded demo tables are deliberately tiny (3 + 10 rows) — enough to learn the API, too small to
*feel* retrieval quality. The **opt-in movie corpus** loads a slice of **Wikipedia movie plots**
(CC-BY-SA-4.0; a decade-balanced set of recognizable films, 1980s–2010s) into the lakehouse and
stands up a vector-enabled `movies` index over them — the scale where semantic vs lexical vs hybrid
visibly differ, facets mean something, and an MCP-connected agent has real substance to answer from.

With the stack up (`just stack`):

```sh
just demo-data
```

This downloads the pre-sliced parquet (a GitHub release asset), writes it into Iceberg **first**
(`growlerdb.movies` — the system of record, same lakehouse-first shape as everything else), then
starts `node-movies`, which builds the index and **embeds each plot locally** (bge-small-en-v1.5 on
ONNX Runtime, no API key, no egress) before registering with the control plane — the gateway routes
`{"index":"movies"}` requests to it automatically, and the demo token is already scoped to it. To
keep embedding fast, the vector is built from a short **synopsis** (the first few plot sentences);
the full `plot` is kept for lexical search and reading.

The default is 5000 films — the local ONNX embedder does ~500 docs/s, so build + embed + serve is
about 45 seconds. Watch the build, or change the size:

```sh
docker compose -f deploy/compose/docker-compose.yml --profile stack --profile demo-data logs -f node-movies
DEMO_DATA_SIZE=0 just demo-data      # the full corpus
```

(Air-gapped or re-slicing? Drop a parquet into `deploy/compose/demo-data/local/` and set
`DEMO_DATA_FILE=/local/<name>.parquet`; regenerate a slice with
`deploy/compose/demo-data/build_movies_slice.py`. Re-running `just demo-data` is idempotent — the
table converges to the parquet's rows.)

## Where hybrid earns its keep

Try the three modes on the same information need — *finding films where a machine turns on its
makers*:

```sh
TOKEN=$(curl -s localhost:8081/v1/login -H 'content-type: application/json' \
  -d '{"username":"demo","password":"demo"}' | jq -r .token)

# Lexical: exact terms only — misses films that never spell it out this way.
curl -s localhost:8081/v1/search -H 'content-type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"index":"movies","query":"plot:\"artificial intelligence\"","limit":5}' | jq '.hits[].fields.title'

# Semantic: meaning, not words — finds them however the plot phrases it.
curl -s localhost:8081/v1/search:semantic -H 'content-type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"index":"movies","vector_field":"plot_vec","query_text":"a machine becomes self-aware and turns against its creators","k":5}' | jq '.hits[].fields.title'

# Hybrid: RRF-fuses both — a film strong lexically AND semantically ranks above either alone.
curl -s localhost:8081/v1/search:hybrid -H 'content-type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"index":"movies","vector_field":"plot_vec","query_text":"a machine becomes self-aware and turns against its creators","k":5,"hydrate":true}' | jq '.hits[] | {title: .fields.title, row: .row.title}'
```

The hybrid call also demonstrates **inline hydration** (`"hydrate": true`): each fused hit carries
its authoritative row from Iceberg in the same response. Facet the corpus by field — genre, origin,
or decade:

```sh
curl -s localhost:8081/v1/facets -H 'content-type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"index":"movies","query":"*:*","fields":["genre"],"size":10}' | jq
```

## Ask an agent

This corpus is what makes the MCP hookup (getting-started §7) compelling: connect Claude (or any
MCP client) and ask *"what films in the corpus deal with AI or robots turning on people?"* — the
agent runs hybrid retrieval over the movie plots and answers **grounded in governed rows with
citations**, scoped to what the demo token may see. As always, GrowlerDB never calls an LLM —
retrieval with citations is the product; the model stays yours.
