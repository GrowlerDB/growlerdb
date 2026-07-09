"""Top-K document retrieval (task-55 re-run): the IoT-realistic "show me the matching readings" query
(`LIMIT K`), comparing what each engine returns:
  - GrowlerDB coordinates-only  (index `telemetry`, no cached fields)        :8097
  - GrowlerDB cached fields      (index `telemetry_cached`, _source-equiv)    :8098
  - GrowlerDB hydrated           (`telemetry` search + /v1/keys:get from Iceberg)
  - Elasticsearch _source
  - Trino SELECT * … LIMIT K
Reports median/min latency over N reps + the rows returned."""
import json, statistics, time, urllib.request

GDB, GDB_C, ES, TRINO = "http://localhost:8097", "http://localhost:8098", "http://localhost:9200", "http://localhost:8090"
REPS, K = 12, 20
BASE, HOUR = 1_750_000_000_000, 3_600_000
QUERIES = [
    ("needle_gateway (rare)", "gateway:gw-rare-eu7", "gateway = 'gw-rare-eu7'", {"term": {"gateway": "gw-rare-eu7"}}),
    ("text_rare ~bearing", "message:bearing", "message LIKE '%bearing%'", {"match": {"message": "bearing"}}),
    ("filter critical+vibration", "status:critical AND metric:vibration",
     "status='critical' AND metric='vibration'",
     {"bool": {"filter": [{"term": {"status": "critical"}}, {"term": {"metric": "vibration"}}]}}),
    ("text_common ~reading", "message:reading", "message LIKE '%reading%'", {"match": {"message": "reading"}}),
    ("time+filter error 1h", f"status:error AND ts:[{BASE} TO {BASE+HOUR}]",
     f"status='error' AND ts BETWEEN {BASE} AND {BASE+HOUR}",
     {"bool": {"filter": [{"term": {"status": "error"}}, {"range": {"ts": {"gte": BASE, "lte": BASE+HOUR}}}]}}),
]

def post(url, body, headers=None):
    r = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                               headers={"content-type": "application/json", **(headers or {})})
    return json.loads(urllib.request.urlopen(r, timeout=120).read())

def post_raw(url, raw, headers):
    r = urllib.request.Request(url, data=raw.encode(), method="POST", headers=headers)
    return json.loads(urllib.request.urlopen(r, timeout=120).read())

def gdb_docs(base, q):
    t = time.perf_counter()
    r = post(f"{base}/v1/search", {"query": q, "limit": K})
    return len(r.get("hits", [])), (time.perf_counter()-t)*1000

def gdb_hydrate(q):
    t = time.perf_counter()
    r = post(f"{GDB}/v1/search", {"query": q, "limit": K})
    coords = [h["coordinates"] for h in r.get("hits", [])]
    rows = 0
    if coords:
        hy = post(f"{GDB}/v1/keys:get", {"keys": coords, "columns": []})
        rows = len(hy.get("rows", []))
    return rows, (time.perf_counter()-t)*1000

def es_docs(dsl):
    t = time.perf_counter()
    r = post(f"{ES}/telemetry/_search", {"size": K, "query": dsl})
    return len(r["hits"]["hits"]), (time.perf_counter()-t)*1000

def trino_rows(where):
    sql = f"SELECT * FROM iceberg.growlerdb.telemetry WHERE {where} LIMIT {K}"
    t = time.perf_counter()
    r = post_raw(f"{TRINO}/v1/statement", sql, {"X-Trino-User": "bench", "content-type": "text/plain"})
    rows = 0
    while True:
        rows += len(r.get("data") or [])
        nxt = r.get("nextUri")
        if not nxt: break
        r = json.loads(urllib.request.urlopen(nxt, timeout=120).read())
    return rows, (time.perf_counter()-t)*1000

def bench(fn, *a):
    lats, n = [], None
    for _ in range(REPS):
        try:
            n, ms = fn(*a); lats.append(ms)
        except Exception as e:
            return None, f"ERR {str(e)[:30]}"
    return n, (statistics.median(lats), min(lats))

def fmt(res):
    if res is None or isinstance(res, str): return f"{str(res):>13}"
    med, mn = res; return f"{med:6.1f} ({mn:5.1f})"

print(f"top-{K} documents | {'q':28} | {'GDB coords':>14} | {'GDB cached':>14} | {'GDB hydrate':>14} | {'ES _source':>14} | {'Trino rows':>14}")
print("-"*132)
for name, gq, tw, eq in QUERIES:
    _, c = bench(gdb_docs, GDB, gq)
    _, ca = bench(gdb_docs, GDB_C, gq)
    _, h = bench(gdb_hydrate, gq)
    _, e = bench(es_docs, eq)
    _, t = bench(trino_rows, tw)
    print(f"{'':12} | {name:28} | {fmt(c):>14} | {fmt(ca):>14} | {fmt(h):>14} | {fmt(e):>14} | {fmt(t):>14}")
