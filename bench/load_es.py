"""Dual-index growlerdb.telemetry into Elasticsearch for the benchmark (task-55): read the Iceberg table,
bulk-index with a telemetry-appropriate mapping (message=text, the rest keyword/long). Stdlib HTTP only.
"""
import json
import os
import urllib.request

from pyiceberg.catalog.rest import RestCatalog

ES = os.environ.get("ES_URL", "http://localhost:9200")
INDEX = "telemetry"

catalog = RestCatalog(
    "growlerdb",
    uri=os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog"),
    warehouse=os.environ.get("POLARIS_CATALOG", "growlerdb"),
    credential=os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t"),
    scope="PRINCIPAL_ROLE:ALL",
    **{
        "s3.endpoint": os.environ.get("AWS_ENDPOINT_URL_S3", "http://localhost:9000"),
        "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "s3.path-style-access": "true",
        "header.X-Iceberg-Access-Delegation": "",
    },
)


def req(method, path, body=None):
    data = body.encode() if body else None
    r = urllib.request.Request(f"{ES}{path}", data=data, method=method,
                               headers={"content-type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return resp.status, resp.read().decode()


# Fresh index with an explicit mapping (single shard, no replicas — local bench).
try:
    req("DELETE", f"/{INDEX}")
except Exception:
    pass
mapping = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
    "mappings": {"properties": {
        "ts": {"type": "long"}, "reading": {"type": "long"},
        "message": {"type": "text"},
        "device_id": {"type": "keyword"}, "gateway": {"type": "keyword"},
        "site": {"type": "keyword"}, "firmware": {"type": "keyword"},
        "metric": {"type": "keyword"}, "subsystem": {"type": "keyword"},
        "status": {"type": "keyword"},
    }},
}
req("PUT", f"/{INDEX}", json.dumps(mapping))

table = catalog.load_table("growlerdb.telemetry")
arrow = table.scan().to_arrow()
names = [n for n in arrow.schema.names]
cols = {n: arrow.column(n).to_pylist() for n in names}
total = arrow.num_rows
print(f"loaded {total:,} rows from Iceberg; bulk-indexing into ES…", flush=True)

BATCH = 20000
done = 0
for start in range(0, total, BATCH):
    end = min(start + BATCH, total)
    lines = []
    for i in range(start, end):
        lines.append(json.dumps({"index": {"_id": cols["id"][i]}}))
        lines.append(json.dumps({n: cols[n][i] for n in names if n != "id"}))
    body = "\n".join(lines) + "\n"
    status, _ = req("POST", f"/{INDEX}/_bulk", body)
    done = end
    if done % 200000 == 0 or done == total:
        print(f"  indexed {done:,}/{total:,}", flush=True)

# Force-merge + refresh so query latency reflects a settled index.
req("POST", f"/{INDEX}/_refresh")
req("POST", f"/{INDEX}/_forcemerge?max_num_segments=1")
status, out = req("GET", f"/{INDEX}/_count")
print(f"ES index `{INDEX}` ready: {out}")
