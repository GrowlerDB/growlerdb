#!/usr/bin/env python3
"""GrowlerDB vs Iceberg-alone (Trino) query comparison.

Runs equivalent predicates as GrowlerDB search(+hydrate) and as Trino SQL table scans over the SAME
Iceberg table, times both, and reports side-by-side latency. Run at each storage milestone
to show where the index wins (selective predicates / point lookups) vs where a scan is
comparable (full scans).

Runs from a kubectl-capable host: GrowlerDB via GATEWAY_URL (port-forward); Trino via `kubectl exec`.
Honest framing: this is search + PK-hydrate vs table-scan, not a general OLAP benchmark.
"""
import json, os, subprocess, time, urllib.request

NS = os.environ.get("NAMESPACE", "growlerdb")
GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:8080")
INDEX = os.environ.get("INDEX", "http_logs")
# The Iceberg table the SQL scans — defaults to INDEX so a windowed run (http_logs_windowed) compares
# against its own source table, not a hardcoded http_logs.
TABLE = os.environ.get("TRINO_TABLE", INDEX)
ITERS = int(os.environ.get("ITERS", "5"))

# (label, GrowlerDB query, Trino SQL) — equivalent predicates over growlerdb.<TABLE>.
PAIRS = [
    ("term status=404", 'status:"404"', f"SELECT id FROM {TABLE} WHERE status='404' LIMIT 20"),
    ("text request~search", "request:search", f"SELECT id FROM {TABLE} WHERE request LIKE '%search%' LIMIT 20"),
    ("point lookup by id", 'id:"req-500000"', f"SELECT * FROM {TABLE} WHERE id='req-500000'"),
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


def trino(sql):
    t = time.perf_counter()
    subprocess.run(["kubectl", "-n", NS, "exec", "deploy/trino", "--",
                    "trino", "--server", "localhost:8080", "--catalog", "iceberg",
                    "--schema", "growlerdb", "--execute", sql],
                   capture_output=True, text=True, timeout=120)
    return (time.perf_counter() - t) * 1000.0


def p50(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2] if xs else 0.0


def main():
    rows = []
    for label, gq, tsql in PAIRS:
        g = [growlerdb(gq) for _ in range(ITERS)]
        t = [trino(tsql) for _ in range(ITERS)]
        row = {"query": label, "growlerdb_p50_ms": round(p50(g), 1), "trino_p50_ms": round(p50(t), 1),
               "speedup_x": round(p50(t) / max(p50(g), 0.1), 1)}
        rows.append(row)
        print(f"{label:24s} GrowlerDB {row['growlerdb_p50_ms']:8.1f}ms  Trino {row['trino_p50_ms']:9.1f}ms  "
              f"({row['speedup_x']}x)", flush=True)
    report = {"index": INDEX, "table": TABLE, "iters": ITERS, "comparisons": rows}
    if os.environ.get("OUT"):
        with open(os.environ["OUT"], "w") as f:
            json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
