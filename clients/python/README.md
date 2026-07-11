# GrowlerDB Python client

A first-party, **dependency-free** Python client for the GrowlerDB Engine API. It
talks to the REST/JSON gateway (`/v1/...`), which mirrors the `growlerdb.v1` gRPC
surface 1:1, so it needs only the Python standard library — no `grpcio`/`protobuf`
build.

> Start a node with the gateway enabled: `growlerdb serve docs --rest-addr 127.0.0.1:8080`

## Install

No package index yet — vendor the `growlerdb/` directory, or add this folder to your
`PYTHONPATH`.

## Usage

```python
from growlerdb import Client, coordinates

client = Client("http://127.0.0.1:8080")

# Search (Lucene/KQL), with a fast-field sort.
res = client.search("body:iceberg", limit=10, sort=[("rank", True)])
for hit in res["hits"]:
    print(hit["coordinates"], hit["score"])

# Suggesters.
client.suggest_prefix("city", "ber")          # autocomplete
client.suggest_fuzzy("city", "berlim", max_edits=1)  # did-you-mean

# Index stats.
client.describe_index()

# Hydrate coordinates back to rows.
client.get_by_key([coordinates({"id": "doc-1"})])
```

Errors come back as `GrowlerError` carrying the HTTP `status` plus the server's
`code`/`message`.

### Authentication

Against a closed (auth-required) gateway, pass a `token=` — an OIDC bearer or a
GrowlerDB API token — which is sent as `Authorization: Bearer <token>` and verified
server-side (identity + roles come from the token, not the client):

```python
client = Client("https://gw.example.com", token=os.environ["GROWLERDB_TOKEN"])
```

The client never self-asserts identity — there is no `principal`/`tenant` header to send.
Identity always comes from the verified token, so a caller cannot impersonate one.

## Coverage / compatibility

| Capability   | Method                          | Endpoint            |
|--------------|---------------------------------|---------------------|
| Search       | `search(...)`                   | `POST /v1/search`        |
| GetByKey     | `get_by_key(keys, columns)`     | `POST /v1/keys:get`      |
| Suggest      | `suggest_prefix` / `suggest_fuzzy` | `POST /v1/suggest`    |
| Admin        | `describe_index(index)`         | `POST /v1/index:describe`|

| Client | Server (`growlerdb.v1`) | Status |
|--------|--------------------------|--------|
| 0.1.0  | M2 (`v1`)                | ✅ Search / GetByKey / Suggest / Admin |

PIT/Export streaming over REST is a server follow-up; this client will gain those
methods when the endpoints land. For native gRPC (when `grpcio` is available for your
Python), generate stubs from `crates/growlerdb-proto/proto/growlerdb/v1/*.proto`.
