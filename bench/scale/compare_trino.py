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

# FAIRNESS (TASK-343): run this AFTER a compaction pass — on the uncompacted streaming layout
# (thousands of tiny data files) Trino pays a pathological planning/open cost that has nothing to do
# with the engine. And give Iceberg the skips it actually has: bloom filters on id/status (set by the
# corpus's WRITE_PROPERTIES) let equality/point predicates skip row groups, and a `day` predicate
# prunes partitions — the scan analog of GrowlerDB's window pruning. The `[full scan]` pairs are
# deliberately unbounded (no `day`, no bloom-friendly shape) — the honest worst case; the
# `[day-pruned]` pairs add the partition predicate so Iceberg is measured at its best, too.

# (label, GrowlerDB query, Trino SQL) — equivalent predicates over growlerdb.<TABLE>.
STATIC_PAIRS = [
    ("term status=404 [full scan]", 'status:"404"', f"SELECT id FROM {TABLE} WHERE status='404' LIMIT 20"),
    ("text request~search [full scan]", "request:search", f"SELECT id FROM {TABLE} WHERE request LIKE '%search%' LIMIT 20"),
    ("point lookup by id [bloom]", 'id:"req-500000"', f"SELECT * FROM {TABLE} WHERE id='req-500000'"),
]


def pruned_pairs(day):
    """Partition-pruned variants: `day = <day>` restricts Trino to one day-partition — the scan analog
    of GrowlerDB's window pruning, so the comparison isn't index-prune vs full-scan."""
    return [
        (f"term status=404 [day-pruned d{day}]", 'status:"404"',
         f"SELECT id FROM {TABLE} WHERE day={day} AND status='404' LIMIT 20"),
        (f"point lookup by id [day-pruned+bloom d{day}]", 'id:"req-500000"',
         f"SELECT * FROM {TABLE} WHERE day={day} AND id='req-500000'"),
    ]


def resolve_prune_day():
    """The day-partition the pruned pairs target — resolved LIVE (most-recent populated `day`, which
    also mirrors the hot windows GrowlerDB's own recent queries hit) rather than a hardcoded era
    constant that could point at an empty/aged-out partition. $PRUNE_DAY overrides; returns None (→
    skip the pruned pairs) if Trino can't answer, so the run never silently scans an empty partition."""
    env = os.environ.get("PRUNE_DAY")
    if env:
        return int(env)
    out = trino_query(f"SELECT max(day) FROM {TABLE}")
    try:
        return int(out)
    except (TypeError, ValueError):
        return None


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


def _trino_exec(sql, timeout=120):
    return subprocess.run(
        ["kubectl", "-n", NS, "exec", "deploy/trino", "--",
         "trino", "--server", "localhost:8080", "--catalog", "iceberg",
         "--schema", "growlerdb", "--execute", sql],
        capture_output=True, text=True, timeout=timeout)


def trino(sql):
    t = time.perf_counter()
    _trino_exec(sql)
    return (time.perf_counter() - t) * 1000.0


def trino_query(sql):
    """Run a Trino query and return its single scalar stdout value (stripped of quotes), or None."""
    try:
        out = (_trino_exec(sql, timeout=60).stdout or "").strip().strip('"')
    except (subprocess.SubprocessError, OSError):
        return None
    return out or None


def p50(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2] if xs else 0.0


def main():
    prune_day = resolve_prune_day()
    pairs = list(STATIC_PAIRS)
    if prune_day is not None:
        pairs += pruned_pairs(prune_day)
        print(f"# day-pruned pairs target day={prune_day} (most-recent populated partition)", flush=True)
    else:
        print("# skipping day-pruned pairs — could not resolve a populated `day` (set $PRUNE_DAY)", flush=True)
    rows = []
    for label, gq, tsql in pairs:
        g = [growlerdb(gq) for _ in range(ITERS)]
        t = [trino(tsql) for _ in range(ITERS)]
        row = {"query": label, "growlerdb_p50_ms": round(p50(g), 1), "trino_p50_ms": round(p50(t), 1),
               "speedup_x": round(p50(t) / max(p50(g), 0.1), 1)}
        rows.append(row)
        print(f"{label:24s} GrowlerDB {row['growlerdb_p50_ms']:8.1f}ms  Trino {row['trino_p50_ms']:9.1f}ms  "
              f"({row['speedup_x']}x)", flush=True)
    report = {"index": INDEX, "table": TABLE, "iters": ITERS, "prune_day": prune_day, "comparisons": rows}
    if os.environ.get("OUT"):
        with open(os.environ["OUT"], "w") as f:
            json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
