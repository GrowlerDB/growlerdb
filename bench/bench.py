"""Query benchmark: the same 5 IoT telemetry queries against GrowlerDB (REST), Trino (REST, same
Iceberg table), and Elasticsearch (REST). Reports median + min latency over N reps and the result
count (a correctness cross-check). Run on the host against the published ports."""
import json
import statistics
import time
import urllib.request

GDB = "http://localhost:8097"
ES = "http://localhost:9200"
TRINO = "http://localhost:8090"
REPS = 15
BASE = 1_750_000_000_000
HOUR = 3_600_000

# (name, growlerdb-query, trino-where, es-query-dsl)
QUERIES = [
    ("needle_gateway (rare keyword)",
     "gateway:gw-rare-eu7",
     "gateway = 'gw-rare-eu7'",
     {"term": {"gateway": "gw-rare-eu7"}}),
    ("text_rare (message~bearing)",
     "message:bearing",
     "message LIKE '%bearing%'",
     {"match": {"message": "bearing"}}),
    ("filter (critical+vibration)",
     "status:critical AND metric:vibration",
     "status = 'critical' AND metric = 'vibration'",
     {"bool": {"filter": [{"term": {"status": "critical"}}, {"term": {"metric": "vibration"}}]}}),
    ("text_common (message~reading)",
     "message:reading",
     "message LIKE '%reading%'",
     {"match": {"message": "reading"}}),
    (f"time+filter (error, 1h window)",
     f"status:error AND ts:[{BASE} TO {BASE + HOUR}]",
     f"status = 'error' AND ts BETWEEN {BASE} AND {BASE + HOUR}",
     {"bool": {"filter": [{"term": {"status": "error"}}, {"range": {"ts": {"gte": BASE, "lte": BASE + HOUR}}}]}}),
]


def post(url, body, headers=None):
    r = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                               headers={"content-type": "application/json", **(headers or {})})
    with urllib.request.urlopen(r, timeout=120) as resp:
        return json.loads(resp.read().decode())


def gdb_query(q):
    t = time.perf_counter()
    r = post(f"{GDB}/v1/search", {"query": q, "limit": 0})
    return r.get("total", 0), (time.perf_counter() - t) * 1000


def es_query(dsl):
    t = time.perf_counter()
    r = post(f"{ES}/telemetry/_search", {"size": 0, "track_total_hits": True, "query": dsl})
    return r["hits"]["total"]["value"], (time.perf_counter() - t) * 1000


def trino_query(where):
    sql = f"SELECT count(*) FROM iceberg.growlerdb.telemetry WHERE {where}"
    t = time.perf_counter()
    r = post(f"{TRINO}/v1/statement", sql_text(sql), headers={"X-Trino-User": "bench"})
    count = None
    # Poll nextUri until the query finishes.
    while True:
        if r.get("data"):
            count = r["data"][0][0]
        nxt = r.get("nextUri")
        if not nxt:
            break
        with urllib.request.urlopen(nxt, timeout=120) as resp:
            r = json.loads(resp.read().decode())
    return count or 0, (time.perf_counter() - t) * 1000


def sql_text(sql):  # Trino POST body is the raw SQL, not JSON
    return sql


def post_raw(url, raw, headers):
    r = urllib.request.Request(url, data=raw.encode(), method="POST", headers=headers)
    with urllib.request.urlopen(r, timeout=120) as resp:
        return json.loads(resp.read().decode())


def trino_query2(where):
    sql = f"SELECT count(*) FROM iceberg.growlerdb.telemetry WHERE {where}"
    t = time.perf_counter()
    r = post_raw(f"{TRINO}/v1/statement", sql, {"X-Trino-User": "bench", "content-type": "text/plain"})
    count = None
    while True:
        if r.get("data"):
            count = r["data"][0][0]
        nxt = r.get("nextUri")
        if not nxt:
            break
        with urllib.request.urlopen(nxt, timeout=120) as resp:
            r = json.loads(resp.read().decode())
    return count or 0, (time.perf_counter() - t) * 1000


def bench(fn, arg):
    counts, lats = [], []
    for _ in range(REPS):
        try:
            c, ms = fn(arg)
            counts.append(c)
            lats.append(ms)
        except Exception as e:
            return None, f"ERR {e}"
    return counts[0], (statistics.median(lats), min(lats))


print(f"{'query':34} | {'count':>8} | {'GrowlerDB ms':>16} | {'Trino ms':>16} | {'ES ms':>16}")
print("-" * 104)
for name, gq, tw, eq in QUERIES:
    gc, gl = bench(gdb_query, gq)
    tc, tl = bench(trino_query2, tw)
    ec, el = bench(es_query, eq)

    def fmt(res):
        if res is None or isinstance(res, str):
            return f"{str(res):>16}"
        med, mn = res
        return f"{med:7.1f} (min {mn:5.1f})"
    cnt = gc if gc is not None else (ec if ec is not None else tc)
    print(f"{name:34} | {str(cnt):>8} | {fmt(gl):>16} | {fmt(tl):>16} | {fmt(el):>16}")
