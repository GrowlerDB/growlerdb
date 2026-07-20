---
title: Demo corpus (arXiv)
layout: default
nav_order: 5
parent: Getting started
---

# The arXiv demo corpus

The seeded demo tables are deliberately tiny (3 + 10 rows) — enough to learn the API, too small to
*feel* retrieval quality. The **opt-in arXiv corpus** loads **~20,000 computer-science
titles+abstracts** (arXiv metadata, CC0; harvested with a 2022+ datestamp floor, so it skews to
modern ML — cs.LG / cs.CV / cs.CL) into the lakehouse and stands up a vector-enabled `arxiv`
index over them — the scale where semantic vs lexical vs hybrid visibly differ, facets mean
something, and an MCP-connected agent has real substance to answer from.

With the stack up (`just stack`):

```sh
just demo-data
```

This downloads the pre-sliced parquet (~10 MB, a GitHub release asset), writes it into Iceberg
**first** (`growlerdb.arxiv` — the system of record, same lakehouse-first shape as everything
else), then starts `node-arxiv`, which builds the index and **embeds every abstract locally**
(bge-small-en-v1.5, no API key, no egress) before registering with the control plane — the gateway
routes `{"index":"arxiv"}` requests to it automatically, and the demo token is already scoped to it.

Embedding ~20k abstracts takes a few minutes on laptop CPU. Watch the build, or shrink it:

```sh
docker compose -f deploy/compose/docker-compose.yml --profile stack --profile demo-data logs -f node-arxiv
DEMO_DATA_SIZE=5000 just demo-data      # smaller slice for slower machines
```

(Air-gapped or re-slicing? Drop a parquet into `deploy/compose/demo-data/local/` and set
`DEMO_DATA_FILE=/local/<name>.parquet`; regenerate a slice with
`deploy/compose/demo-data/build_arxiv_slice.py`. Re-running `just demo-data` is idempotent — the
table converges to the parquet's rows.)

## Where hybrid earns its keep

Try the three modes on the same information need — *finding work on making attention faster*:

```sh
TOKEN=$(curl -s localhost:8081/v1/login -H 'content-type: application/json' \
  -d '{"username":"demo","password":"demo"}' | jq -r .token)

# Lexical: exact terms only — misses papers that never say these words.
curl -s localhost:8081/v1/search -H 'content-type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"index":"arxiv","query":"abstract:\"efficient attention\"","limit":5}' | jq '.hits[].fields.title'

# Semantic: meaning, not words — finds them however they phrase it.
curl -s localhost:8081/v1/search:semantic -H 'content-type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"index":"arxiv","vector_field":"abstract_vec","query_text":"making transformer attention faster and cheaper","k":5}' | jq '.hits[].fields.title'

# Hybrid: RRF-fuses both — a paper strong lexically AND semantically ranks above either alone.
curl -s localhost:8081/v1/search:hybrid -H 'content-type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"index":"arxiv","vector_field":"abstract_vec","query_text":"making transformer attention faster and cheaper","k":5,"hydrate":true}' | jq '.hits[] | {title: .fields.title, row: .row.title}'
```

The hybrid call also demonstrates **inline hydration** (`"hydrate": true`): each fused hit carries
its authoritative row from Iceberg in the same response. Facet the corpus by field:

```sh
curl -s localhost:8081/v1/facets -H 'content-type: application/json' \
  -H "authorization: Bearer $TOKEN" \
  -d '{"index":"arxiv","query":"*:*","fields":["primary_category"],"size":10}' | jq
```

## Ask an agent

This corpus is what makes the MCP hookup (getting-started §7) compelling: connect Claude (or any
MCP client) and ask *"what approaches to speeding up attention does the corpus cover?"* — the agent
runs hybrid retrieval over 20k abstracts and answers **grounded in governed rows with citations**,
scoped to what the demo token may see. As always, GrowlerDB never calls an LLM — retrieval with
citations is the product; the model stays yours.
