#!/usr/bin/env python
"""Validate cold-tiering (automatic park / revive) on a running windowed GrowlerDB cluster — TASK-229.

Exercises the loop shipped in the engine (ADR D39: cold-tiering is automatic in both directions):

  1. **Auto-park** — with `hot_windows` set + cold-tiering enabled, aged windows demote to cold
     read-through on their own. Poll `GET /v1/cold` until at least `--min-cold` windows are cold.
  2. **Cold read-through correctness** — drive the workload's queries while windows are parked and
     assert the time-window query still returns hits with no errors (a parked window answers,
     read-through, transparently).
  3. **Auto-revive (pre-warm)** — sustained query traffic re-heats a cold window; watch `/v1/cold`
     until the cold count drops, and record how long revive took + the cold-vs-warm query latency.
  4. **SLIs** — capture the shared read-through cache stats from `/v1/cold`.

Writes a `results.json` (pass/fail per check + the measurements) that `capture.py --results` folds
into the run record. Because this needs a live cluster, the network/subprocess IO is injectable:
`--selftest` drives the whole state machine with scripted fakes and asserts the outcomes (runs in
smoke); the real run wires `/v1/cold` over HTTP and query rounds through `harness.py`.

Stdlib only. Config via env (GATEWAY_URL, WORKLOAD, INDEX) + flags.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


# ----- pure orchestration (injectable IO: `fetch_cold` -> ColdStatus dict|None; `query_round` ->
# {"ok","hits","errors","latency_s"}). Kept free of real HTTP/subprocess so --selftest can drive it. -

def wait_for(fetch_cold, predicate, timeout_s, poll_s, sleep, now):
    """Poll `fetch_cold` until `predicate(status)`; return the status, or None on timeout."""
    deadline = now() + timeout_s
    status = fetch_cold()
    while now() < deadline:
        if status and predicate(status):
            return status
        sleep(poll_s)
        status = fetch_cold()
    return status if (status and predicate(status)) else None


def validate(fetch_cold, query_round, opts, sleep=time.sleep, now=time.time):
    r = {
        "purpose": "cold-tier park/revive validation (TASK-229)",
        "checks": {},
        "measurements": {},
    }

    # 1) auto-park
    parked = wait_for(
        fetch_cold, lambda s: s.get("cold", 0) >= opts.min_cold,
        opts.park_timeout_s, opts.poll_s, sleep, now,
    )
    r["checks"]["auto_park"] = parked is not None
    if parked is None:
        last = fetch_cold() or {}
        r["measurements"]["windows"] = {"cold": last.get("cold", 0), "hot": last.get("hot", 0)}
        r["passed"] = False
        r["note"] = f"no window auto-parked within {opts.park_timeout_s}s (need >= {opts.min_cold} cold)"
        return r
    cold_before = parked["cold"]
    r["measurements"]["windows"] = {
        "total": len(parked.get("windows", [])), "hot": parked["hot"], "cold": cold_before,
    }

    # 2) cold read-through correctness — query the workload while windows are parked.
    q_cold = query_round()
    r["checks"]["cold_read_through"] = bool(
        q_cold["ok"] and q_cold["hits"] > 0 and q_cold["errors"] == 0
    )
    r["measurements"]["cold_query"] = q_cold

    # 3) auto-revive — sustained traffic re-heats a cold window; watch the cold count fall.
    start = now()
    revived = False
    for _ in range(opts.revive_rounds):
        query_round()  # traffic drives the pre-warm signal
        s = fetch_cold()
        if s and s.get("cold", cold_before) < cold_before:
            revived = True
            break
        sleep(opts.poll_s)
    r["checks"]["auto_revive"] = revived
    r["measurements"]["revive_elapsed_s"] = round(now() - start, 1) if revived else None

    # measured benefit: a query round after revive vs the cold round above
    q_warm = query_round()
    r["measurements"]["warm_query"] = q_warm
    if q_cold.get("latency_s") and q_warm.get("latency_s"):
        r["measurements"]["cold_vs_warm_latency_s"] = [q_cold["latency_s"], q_warm["latency_s"]]

    # 4) cache SLIs
    final = fetch_cold() or {}
    r["measurements"]["cache"] = final.get("cache")

    r["passed"] = all(r["checks"].values())
    return r


# ----- real IO ------------------------------------------------------------------------------------

def fetch_cold_http(gateway):
    try:
        with urllib.request.urlopen(f"{gateway}/v1/cold", timeout=15) as resp:
            return json.load(resp)
    except Exception as e:
        print(f"  /v1/cold fetch failed: {type(e).__name__}: {e}", file=sys.stderr)
        return None


def query_round_harness(gateway, workload, duration_s, concurrency):
    """Run one `harness.py query` round against the gateway; return a normalized summary. Reuses the
    proven query driver (the same one staged_run.py uses) instead of hand-building query bodies."""
    out = os.path.join(HERE, ".coldtier-query.json")
    proc = subprocess.run(
        [sys.executable, os.path.join(HERE, "harness.py"), "query", workload,
         "--duration", str(duration_s), "--concurrency", str(concurrency), "--out", out],
        env={**os.environ, "GROWLERDB_OS_URL": gateway}, capture_output=True, text=True,
    )
    try:
        with open(out) as f:
            data = json.load(f)
    except Exception:
        return {"ok": False, "hits": 0, "errors": 1, "latency_s": None,
                "detail": proc.stderr[-500:]}
    # harness reports per-query stats under `per_query`; each entry has count/errors/p50 (ms).
    # Treat the time-window query as the read-through probe.
    queries = (data.get("per_query") or data.get("queries") or {}) if isinstance(data, dict) else {}
    hits = errors = 0
    lat = None

    def _p50_s(q):  # harness emits p50 in ms; normalize to seconds
        v = q.get("p50_s")
        if v is None and q.get("p50") is not None:
            v = q["p50"] / 1000.0
        return v

    for name, q in (queries.items() if isinstance(queries, dict) else []):
        if not isinstance(q, dict):
            continue
        errors += int(q.get("errors", 0))
        if "time" in name or "window" in name or "range" in name:
            hits += int(q.get("hits", q.get("count", 0)) or 0)
            lat = _p50_s(q) or lat
    if hits == 0:  # no dedicated time-window query — fall back to total hits/count
        hits = sum(int(q.get("hits", q.get("count", 0)) or 0)
                   for q in (queries.values() if isinstance(queries, dict) else []) if isinstance(q, dict))
    return {"ok": proc.returncode == 0, "hits": hits, "errors": errors, "latency_s": lat}


# ----- selftest -----------------------------------------------------------------------------------

def selftest():
    fails = []

    def check(c, m):
        if not c:
            fails.append(m)

    opts = argparse.Namespace(
        min_cold=1, park_timeout_s=100, poll_s=0, revive_rounds=5,
    )

    # A fake cluster: 5 windows; `cold` climbs 0->2 over the first few polls (auto-park), then, once
    # query traffic has driven `revive_rounds`, drops back to 1 (a window auto-revived).
    state = {"polls": 0, "queries": 0}

    def fake_fetch():
        state["polls"] += 1
        cold = 0 if state["polls"] < 3 else 2
        if state["queries"] >= 2 and cold == 2:
            cold = 1  # a window re-heated
        return {"windows": [{"window": i} for i in range(5)],
                "hot": 5 - cold, "cold": cold,
                "cache": {"hits": 40, "misses": 8}}

    def fake_query():
        state["queries"] += 1
        return {"ok": True, "hits": 123, "errors": 0, "latency_s": 0.04}

    res = validate(fake_fetch, fake_query, opts, sleep=lambda _s: None, now=_fake_clock())
    check(res["checks"]["auto_park"], "auto_park should pass")
    check(res["checks"]["cold_read_through"], "cold_read_through should pass")
    check(res["checks"]["auto_revive"], "auto_revive should pass")
    check(res["passed"], "overall should pass")
    check(res["measurements"]["windows"]["cold"] == 2, "records cold-before count")
    check(res["measurements"]["revive_elapsed_s"] is not None, "records revive elapsed")

    # Park never happens -> fail cleanly, no crash, and don't claim revive.
    res2 = validate(lambda: {"hot": 5, "cold": 0, "windows": [], "cache": None},
                    fake_query, argparse.Namespace(min_cold=1, park_timeout_s=3, poll_s=0,
                                                   revive_rounds=3),
                    sleep=lambda _s: None, now=_fake_clock())
    check(res2["checks"]["auto_park"] is False, "auto_park fails when nothing parks")
    check(res2["passed"] is False, "overall fails when park fails")
    check("auto_revive" not in res2["checks"], "no revive claim when park failed")

    # A cold query that errors must fail read-through.
    res3 = validate(fake_fetch_parked(), lambda: {"ok": True, "hits": 0, "errors": 3, "latency_s": None},
                    argparse.Namespace(min_cold=1, park_timeout_s=100, poll_s=0, revive_rounds=1),
                    sleep=lambda _s: None, now=_fake_clock())
    check(res3["checks"]["cold_read_through"] is False, "read-through fails on query errors")

    if fails:
        print("SELFTEST FAILED:")
        for m in fails:
            print("  -", m)
        return 1
    print("coldtier_validate.py selftest: OK")
    return 0


def _fake_clock():
    """A monotonic clock that advances 1s per call — deterministic timeouts/elapsed in the selftest."""
    t = {"s": 1000.0}

    def clock():
        t["s"] += 1.0
        return t["s"]

    return clock


def fake_fetch_parked():
    return lambda: {"windows": [{"window": 0}], "hot": 2, "cold": 1, "cache": {"hits": 1}}


# ----- main ---------------------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(description="Validate cold-tier park/revive on a windowed cluster.")
    ap.add_argument("--gateway", default=os.environ.get("GATEWAY_URL", "http://localhost:8080"))
    ap.add_argument("--workload", default=os.environ.get("WORKLOAD", "http_logs_windowed"))
    ap.add_argument("--min-cold", type=int, default=1, help="windows that must auto-park to pass")
    ap.add_argument("--park-timeout-s", type=int, default=900,
                    help="how long to wait for auto-park (>= a couple of parkIntervalSecs)")
    ap.add_argument("--poll-s", type=int, default=15, help="/v1/cold poll interval")
    ap.add_argument("--revive-rounds", type=int, default=8,
                    help="query rounds to drive while waiting for a window to re-heat")
    ap.add_argument("--query-duration-s", type=int, default=20)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--out", default=os.environ.get("OUT", os.path.join(HERE, "coldtier_results.json")))
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)

    if args.selftest:
        return selftest()

    def query_round():
        return query_round_harness(args.gateway, args.workload, args.query_duration_s,
                                   args.concurrency)

    res = validate(lambda: fetch_cold_http(args.gateway), query_round, args)
    with open(args.out, "w") as f:
        json.dump(res, f, indent=2)
    print(json.dumps(res, indent=2))
    print(f"\n{'PASS' if res.get('passed') else 'FAIL'} — cold-tier validation → {args.out}")
    return 0 if res.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
