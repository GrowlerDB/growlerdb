#!/usr/bin/env python3
"""Neutral, open-loop, multi-engine query driver for the GrowlerDB-vs-OpenSearch comparison.

Fires the SAME query set (`queries.comparison.json`) at each engine through an identical HTTP path so
neither side gets a home-field harness. Complements `harness.py query` (GrowlerDB-only, closed-loop)
and `compare_trino.py` (the Iceberg-scan baseline — Trino predicates aren't `_search`-shaped, so it
stays there).

Two things it does that the fairness charter requires (see bench/scale/comparison-plan.md):

  * Open-loop arrival rate. Requests are issued at a fixed schedule regardless of when responses come
    back, so a slow server shows up as queue wait in the latency — the closed-loop `harness.py query`
    hides that (coordinated omission). We report BOTH `latency` (scheduled->response, the honest tail)
    and `service_time` (send->response).
  * Per-query-type reporting + a QPS saturation sweep (QPS vs p50/p99, to find the knee).

Engines share the OpenSearch `_search` body for lexical queries. The two documented asymmetries are
handled explicitly: `retrieval` runs in the value-fetch modes each engine actually uses, and
`autocomplete` hits GrowlerDB's native /v1/suggest vs OpenSearch's completion suggester.

Endpoints (env): GROWLERDB_OS_URL (default http://localhost:8081), OPENSEARCH_URL
(default http://localhost:9200), GROWLERDB_TOKEN (optional bearer for the gateway).
"""

import argparse
import concurrent.futures
import copy
import json
import os
import sys
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

from harness import Workload, _percentiles  # reuse workload loading + percentile helper

GROWLERDB_OS_URL = os.environ.get("GROWLERDB_OS_URL", "http://localhost:8081")
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
GROWLERDB_TOKEN = os.environ.get("GROWLERDB_TOKEN", "")

# Lexical kinds share the OpenSearch _search body verbatim across engines.
LEXICAL_KINDS = {"count", "term", "range", "match", "phrase", "boolean", "ip_cidr", "retrieval"}


def _engines(names):
    reg = {
        "growlerdb": {"base": GROWLERDB_OS_URL, "token": GROWLERDB_TOKEN, "suggest": "native"},
        "opensearch": {"base": OPENSEARCH_URL, "token": "", "suggest": "completion"},
    }
    return {n: reg[n] for n in names}


def request_for(engine, cfg, index, q):
    """(url, body) for this engine+query, or None to skip (query n/a for the engine).

    The single source of the two disclosed asymmetries — keep them here, not scattered."""
    kind = q.get("kind", "count")
    if kind in LEXICAL_KINDS:
        return f"{cfg['base']}/{index}/_search", q["body"]
    if kind == "autocomplete":
        if cfg["suggest"] == "native":  # GrowlerDB: native term-dict scan, index carried in the body
            return f"{cfg['base']}/v1/suggest", q["growlerdb_suggest"]
        return f"{cfg['base']}/{index}/_search", q["opensearch_suggest"]  # OpenSearch completion FST
    raise SystemExit(f"query '{q.get('name')}': unknown kind '{kind}'")


def _post(url, body, token):
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=120) as resp:
        resp.read()  # drain; we time the round-trip, not parse cost


def _weighted_plan(queries):
    plan = []
    for q in queries:
        plan.extend([q] * int(q.get("weight", 1)))
    return plan


def _post_json(url, body, token):
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _resolve_value(cfg, index, field):
    """Fetch a live value for `field` via the shared _search path — a real value must exist (seeds vary
    per pod, so it can't be hardcoded). Uses the same neutral endpoint as the benchmark itself. Returns
    None if it can't be read; the caller then skips that query for this engine (never fails the run)."""
    body = {"size": 1, "_source": [field], "query": {"match_all": {}}}
    try:
        payload = _post_json(f"{cfg['base']}/{index}/_search", body, cfg["token"])
    except Exception:  # noqa: BLE001
        return None
    hits = (payload.get("hits") or {}).get("hits") or []
    if not hits:
        return None
    for container in ("_source", "fields"):  # OpenSearch returns _source; tolerate a fields shape too
        d = hits[0].get(container) or {}
        if field in d:
            v = d[field]
            return v[0] if isinstance(v, list) else v
    return None


def _resolve_queries(engine, cfg, index, queries):
    """Per-engine copy of the query set with any `resolve` placeholder filled from a live value. A query
    with `"resolve": {"field": F}` has its `term.F` value replaced by a real F fetched from the engine
    (e.g. the point-lookup on trace_id). Because the run is sequential (one engine per phase), each
    engine resolves its own valid id — a point-lookup's latency doesn't depend on which existing id."""
    out = []
    for q in queries:
        r = q.get("resolve")
        if not r:
            out.append(q)
            continue
        val = _resolve_value(cfg, index, r["field"])
        if val is None:
            print(f"  ! {engine}: could not resolve '{r['field']}' for '{q['name']}' — skipping it",
                  file=sys.stderr)
            continue
        qc = copy.deepcopy(q)
        qc["body"]["query"]["term"][r["field"]] = val
        out.append(qc)
    return out


def run_open_loop(engine, cfg, index, plan, target_qps, duration, max_workers):
    """Issue requests at a fixed `target_qps` schedule (open loop) for `duration` seconds."""
    stats = defaultdict(lambda: {"latency": [], "service": [], "errors": 0, "kind": None})
    interval = 1.0 / target_qps
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    futures = []

    def one(q, scheduled):
        url, body = request_for(engine, cfg, index, q)
        send = time.perf_counter()
        try:
            _post(url, body, cfg["token"])
            recv = time.perf_counter()
            return q["name"], q.get("kind"), (recv - scheduled) * 1e3, (recv - send) * 1e3, False
        except Exception as e:  # noqa: BLE001 — record the failure, keep the schedule going
            recv = time.perf_counter()
            print(f"  ! {engine} {q['name']}: {e}", file=sys.stderr)
            return q["name"], q.get("kind"), (recv - scheduled) * 1e3, (recv - send) * 1e3, True

    start = time.perf_counter()
    end = start + duration
    next_t = start
    i = 0
    while time.perf_counter() < end:
        now = time.perf_counter()
        if next_t > now:
            time.sleep(next_t - now)
        q = plan[i % len(plan)]
        i += 1
        futures.append(ex.submit(one, q, next_t))
        next_t += interval

    for fut in concurrent.futures.as_completed(futures):
        name, kind, latency, service, err = fut.result()
        s = stats[name]
        s["kind"] = kind
        s["latency"].append(latency)
        s["service"].append(service)
        if err:
            s["errors"] += 1
    ex.shutdown(wait=True)
    elapsed = time.perf_counter() - start

    per_query = {}
    for name, s in stats.items():
        per_query[name] = {
            "kind": s["kind"], "count": len(s["latency"]), "errors": s["errors"],
            "latency_ms": _percentiles(s["latency"]),
            "service_ms": _percentiles(s["service"]),
        }
    all_lat = [x for s in stats.values() for x in s["latency"]]
    return {
        "engine": engine, "target_qps": target_qps,
        "achieved_qps": round(len(all_lat) / elapsed, 1) if elapsed else 0.0,
        "duration_s": round(elapsed, 1),
        "overall_latency_ms": _percentiles(all_lat),
        "per_query": per_query,
    }


def run_sweep(engine, cfg, index, plan, rates, duration, max_workers):
    rounds = []
    for r in rates:
        print(f"  sweep {engine} @ {r} qps ...", file=sys.stderr)
        res = run_open_loop(engine, cfg, index, plan, r, duration, max_workers)
        o = res["overall_latency_ms"]
        rounds.append({"target_qps": r, "achieved_qps": res["achieved_qps"],
                       "p50": o["p50"], "p99": o["p99"]})
    return rounds


def cmd_run(args):
    wl = Workload(args.workload)
    # This driver runs the COMPARISON query set, not the default queries.json.
    qfile = wl.dir / "queries.comparison.json"
    queries = json.loads(qfile.read_text())
    engines = _engines(args.engines.split(","))
    index = wl.index_name

    report = {"workload": wl.name, "index": index, "max_workers": args.max_workers, "engines": {}}
    for name, cfg in engines.items():
        print(f"== {name} ({cfg['base']}) ==", file=sys.stderr)
        # Fill any `resolve` placeholder (e.g. point_lookup_trace_id) from a live value on this engine.
        plan = _weighted_plan(_resolve_queries(name, cfg, index, queries))
        entry = run_open_loop(name, cfg, index, plan, args.qps, args.duration, args.max_workers)
        if args.sweep:
            rates = [int(x) for x in args.sweep.split(",")]
            entry["sweep"] = run_sweep(name, cfg, index, plan, rates, args.sweep_duration, args.max_workers)
        report["engines"][name] = entry

    Path(args.out).write_text(json.dumps(report, indent=2))
    _print(report)
    print(f"\nwrote {args.out}")


def _print(r):
    for name, e in r["engines"].items():
        print(f"\n== {name} @ {e['achieved_qps']}/{e['target_qps']} qps "
              f"(overall p50 {e['overall_latency_ms']['p50']:.1f} / p99 {e['overall_latency_ms']['p99']:.1f} ms) ==")
        print(f"{'query':24}{'kind':12}{'n':>6}{'err':>5}{'lat_p50':>9}{'lat_p99':>9}{'svc_p50':>9}{'svc_p99':>9}")
        for qn, s in e["per_query"].items():
            print(f"{qn:24}{(s['kind'] or ''):12}{s['count']:>6}{s['errors']:>5}"
                  f"{s['latency_ms']['p50']:>9.1f}{s['latency_ms']['p99']:>9.1f}"
                  f"{s['service_ms']['p50']:>9.1f}{s['service_ms']['p99']:>9.1f}")
        if "sweep" in e:
            print("  sweep (target->achieved qps : p50/p99 ms):")
            for row in e["sweep"]:
                print(f"    {row['target_qps']:>6} -> {row['achieved_qps']:>7} : "
                      f"{row['p50']:.1f}/{row['p99']:.1f}")


def cmd_selfcheck(args):
    """No network: prove plan-building + per-engine request routing for every kind."""
    wl = Workload(args.workload)
    queries = json.loads((wl.dir / "queries.comparison.json").read_text())
    plan = _weighted_plan(queries)
    print(f"workload {wl.name}: {len(queries)} queries -> plan of {len(plan)} weighted entries")
    fails = []
    for name, cfg in _engines(["growlerdb", "opensearch"]).items():
        for q in queries:
            try:
                url, body = request_for(name, cfg, wl.index_name, q)
                assert url.startswith("http") and isinstance(body, dict)
            except Exception as e:  # noqa: BLE001
                fails.append(f"{name}/{q.get('name')}: {e}")
        # spot-check the autocomplete asymmetry resolves to different endpoints
    ac = [q for q in queries if q.get("kind") == "autocomplete"]
    if ac:
        g = request_for("growlerdb", _engines(["growlerdb"])["growlerdb"], wl.index_name, ac[0])[0]
        o = request_for("opensearch", _engines(["opensearch"])["opensearch"], wl.index_name, ac[0])[0]
        assert g.endswith("/v1/suggest") and o.endswith("/_search"), "autocomplete routing wrong"
        print(f"autocomplete routing OK: growlerdb={g.rsplit('/',1)[1]} opensearch=_search")
    # resolve queries: correct placeholder shape + drop-on-unresolvable (no network needed)
    for q in queries:
        r = q.get("resolve")
        if not r:
            continue
        field = r["field"]
        if field not in q.get("body", {}).get("query", {}).get("term", {}):
            fails.append(f"{q['name']}: resolve field '{field}' is not a term in body")
    resolvers = [q for q in queries if q.get("resolve")]
    if resolvers:
        dropped = _resolve_queries("x", {"base": "http://127.0.0.1:0", "token": ""}, wl.index_name, resolvers)
        if dropped:
            fails.append("unresolvable resolve-queries should be dropped, got " + str(len(dropped)))
        else:
            print(f"resolve OK: {len(resolvers)} live-resolved queries "
                  f"({', '.join(q['name'] for q in resolvers)}); drop-on-unreachable works")
    kinds = sorted({q.get("kind") for q in queries})
    print(f"kinds covered: {kinds}")
    if fails:
        print("FAIL:")
        for f in fails:
            print("  -", f)
        sys.exit(1)
    print("self-check OK")


def main():
    ap = argparse.ArgumentParser(description="Neutral open-loop multi-engine query driver")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run", help="run the comparison query set against each engine")
    p.add_argument("workload")
    p.add_argument("--engines", default="growlerdb,opensearch")
    p.add_argument("--qps", type=int, default=100, help="open-loop target arrival rate")
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--max-workers", type=int, default=64, dest="max_workers",
                   help="client concurrency ceiling; 512 bursts overwhelm an unguarded server (500/429)")
    p.add_argument("--sweep", default="", help="comma QPS list for a saturation sweep, e.g. 50,100,200,400,800")
    p.add_argument("--sweep-duration", type=int, default=20, dest="sweep_duration")
    p.add_argument("--out", default="comparison-report.json")
    p.set_defaults(fn=cmd_run)
    p = sub.add_parser("self-check", help="no-network logic check (routing for every kind)")
    p.add_argument("workload")
    p.set_defaults(fn=cmd_selfcheck)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
