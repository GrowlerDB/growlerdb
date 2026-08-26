#!/usr/bin/env python3
"""GrowlerDB vs Iceberg-alone (Trino) query comparison — the scan baseline (fairness charter axis 1).

Runs equivalent predicates as GrowlerDB search(+hydrate) and as Trino SQL over the SAME Iceberg table,
times both, and reports side-by-side latency — where the index wins (selective predicates) vs where a
scan is comparable. Honest framing: search + PK-hydrate vs table-scan, not a general OLAP benchmark.

Endpoints: GrowlerDB via GATEWAY_URL (its native /v1/search + /v1/keys:get). Trino via TRINO_URL (the
Trino REST API — this is what lets the comparison run inside a driver Job with no kubectl), else
`kubectl exec deploy/trino` on a kubectl-capable host.
"""
import json
import os
import subprocess
import time
import urllib.request

NS = os.environ.get("NAMESPACE", "growlerdb")
GATEWAY = os.environ.get("GATEWAY_URL", "http://gdb-growlerdb-gateway:8080")
TRINO_URL = os.environ.get("TRINO_URL", "")  # e.g. http://trino:8080 — enables the in-cluster HTTP path
INDEX = os.environ.get("INDEX", "http_logs")
TABLE = os.environ.get("TRINO_TABLE", INDEX)  # Iceberg table under iceberg.growlerdb
ITERS = int(os.environ.get("ITERS", "5"))

# FAIRNESS (TASK-343): run this AFTER a compaction pass — on the uncompacted streaming layout (thousands
# of tiny files) Trino pays a pathological planning/open cost unrelated to the engine (compare_run's
# GrowlerDB phase compacts first). http_logs is UNPARTITIONED (hash-routed by request_id) with parquet
# bloom filters on request_id + status (corpus WRITE_PROPERTIES), so equality on those *can* skip row
# groups; everything else is a full scan — the honest worst case for a scan. The unique-key bloom
# (request_id) is Iceberg's best skip but can't be paired: request_id is KEY-ONLY in GrowlerDB (not a
# searchable term), so there is no GrowlerDB point-lookup-by-id query to put opposite it (disclosed).

# (label, GrowlerDB native query, Trino SQL) — equivalent predicates over iceberg.growlerdb.<TABLE>.
PAIRS = [
    ("term status=500 [status bloom]", 'status:"500"',
     f"SELECT request_id FROM {TABLE} WHERE status='500' LIMIT 20"),
    ("term user_id [full scan]", 'user_id:"user_02500"',
     f"SELECT request_id FROM {TABLE} WHERE user_id='user_02500' LIMIT 20"),
    ("text path~checkout [full scan]", "path:checkout",
     f"SELECT request_id FROM {TABLE} WHERE path LIKE '%checkout%' LIMIT 20"),
]


def growlerdb(query):
    body = json.dumps({"index": INDEX, "query": query, "limit": 20}).encode()
    req = urllib.request.Request(f"{GATEWAY}/v1/search", data=body, headers={"content-type": "application/json"})
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=60) as r:
        res = json.loads(r.read())
    # hydrate the hits (the value prop: search returns keys, keys hydrate to rows)
    keys = [h["coordinates"] for h in res.get("hits", [])][:20]
    if keys:
        hb = json.dumps({"keys": keys}).encode()
        hr = urllib.request.Request(f"{GATEWAY}/v1/keys:get", data=hb, headers={"content-type": "application/json"})
        urllib.request.urlopen(hr, timeout=60).read()
    return (time.perf_counter() - t) * 1000.0


def _trino_http(sql, timeout=120):
    """Execute one SQL statement through the Trino REST API (POST /v1/statement, follow nextUri)."""
    req = urllib.request.Request(
        f"{TRINO_URL}/v1/statement", data=sql.encode(),
        headers={"X-Trino-User": "bench", "X-Trino-Catalog": "iceberg", "X-Trino-Schema": "growlerdb"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        page = json.loads(r.read())
    while True:
        if page.get("error"):
            raise RuntimeError(page["error"].get("message", "trino error"))
        nxt = page.get("nextUri")
        if not nxt:
            return
        with urllib.request.urlopen(nxt, timeout=timeout) as r:
            page = json.loads(r.read())


def _trino_exec(sql, timeout=120):
    subprocess.run(
        ["kubectl", "-n", NS, "exec", "deploy/trino", "--", "trino", "--server", "localhost:8080",
         "--catalog", "iceberg", "--schema", "growlerdb", "--execute", sql],
        capture_output=True, text=True, timeout=timeout, check=False)


def trino(sql):
    t = time.perf_counter()
    _trino_http(sql) if TRINO_URL else _trino_exec(sql)
    return (time.perf_counter() - t) * 1000.0


def p50(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2] if xs else 0.0


def main():
    print(f"# GrowlerDB (search+hydrate) vs Trino (Iceberg scan) over iceberg.growlerdb.{TABLE} "
          f"({'REST' if TRINO_URL else 'kubectl-exec'}, {ITERS} iters/query)", flush=True)
    rows = []
    for label, gq, tsql in PAIRS:
        g = [growlerdb(gq) for _ in range(ITERS)]
        t = [trino(tsql) for _ in range(ITERS)]
        row = {"query": label, "growlerdb_p50_ms": round(p50(g), 1), "trino_p50_ms": round(p50(t), 1),
               "speedup_x": round(p50(t) / max(p50(g), 0.1), 1)}
        rows.append(row)
        print(f"{label:34s} GrowlerDB {row['growlerdb_p50_ms']:8.1f}ms  Trino {row['trino_p50_ms']:9.1f}ms  "
              f"({row['speedup_x']}x)", flush=True)
    report = {"index": INDEX, "table": TABLE, "iters": ITERS, "comparisons": rows}
    if os.environ.get("OUT"):
        with open(os.environ["OUT"], "w") as f:
            json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
