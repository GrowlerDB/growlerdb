#!/usr/bin/env python3
"""End-to-end ingest-freshness harness: wall-clock from a source Iceberg commit until a query returns
that row, on ONE clock for every engine (fairness charter). Appends a sentinel row (unique user_id;
request_id is key-only), then polls each engine concurrently until it appears. See comparison-plan.md."""

import argparse
import json
import os
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path

from harness import Workload, _percentiles

GROWLERDB_OS_URL = os.environ.get("GROWLERDB_OS_URL", "http://localhost:8081")
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
GROWLERDB_TOKEN = os.environ.get("GROWLERDB_TOKEN", "")
SENTINEL_FIELD = "user_id"  # KEYWORD in GrowlerDB, keyword in OpenSearch — searchable in both


def _engines(names):
    reg = {
        "growlerdb": {"base": GROWLERDB_OS_URL, "token": GROWLERDB_TOKEN},
        "opensearch": {"base": OPENSEARCH_URL, "token": ""},
    }
    return {n: reg[n] for n in names}


def _hits(cfg, index, token):
    """Number of docs whose sentinel field == token, via the OpenSearch-shaped _search on each engine."""
    body = {"query": {"term": {SENTINEL_FIELD: token}}, "size": 0}
    headers = {"content-type": "application/json"}
    if cfg["token"]:
        headers["authorization"] = f"Bearer {cfg['token']}"
    req = urllib.request.Request(f"{cfg['base']}/{index}/_search", data=json.dumps(body).encode(),
                                 method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode())
    total = payload.get("hits", {}).get("total", 0)
    return total.get("value", 0) if isinstance(total, dict) else total  # ES-7 vs ES-6 total shape


def _poll_until_visible(name, cfg, index, token, commit_t, timeout, interval, out):
    """Poll one engine until the sentinel appears; store lag_ms (or None on timeout) in out[name]."""
    deadline = commit_t + timeout
    while time.perf_counter() < deadline:
        try:
            if _hits(cfg, index, token) >= 1:
                out[name] = (time.perf_counter() - commit_t) * 1e3
                return
        except Exception:  # noqa: BLE001 — engine not ready / transient; keep polling
            pass
        time.sleep(interval)
    out[name] = None  # timed out


def _write_sentinel(mod, table, model, rng, token):
    """Append a single sentinel row (user_id=token) through the corpus write path, returning the commit
    instant (perf_counter after tbl.append = the snapshot is committed). ts is stamped NOW for the lag basis."""
    import pyarrow as pa

    schema = mod._schema()
    catalog = mod._catalog()
    cols = mod._rows(1, model, rng)
    cols[SENTINEL_FIELD] = [token]
    cols["ts"] = [int(time.time())]  # stamp NOW so end-to-end lag is measured from this commit
    tbl = catalog.load_table(table)
    tbl.append(pa.table(cols, schema=schema))
    return time.perf_counter()


def cmd_run(args):
    import random

    wl = Workload(args.workload)
    mod = wl.corpus_module()
    if mod is None or not all(hasattr(mod, a) for a in ("_catalog", "_schema", "_rows", "_build_model")):
        raise SystemExit(f"workload '{wl.name}': corpus.py lacks the _catalog/_schema/_rows/_build_model helpers")
    table = wl.meta.get("corpus", {}).get("table", f"growlerdb.{wl.name}")
    index = wl.index_name
    engines = _engines(args.engines.split(","))

    # Sentinel content need not be reproducible (the user_id is overwritten with a unique token), so an
    # unseeded RNG is fine; build the corpus model once (its ip/user pools are expensive to rebuild).
    rng = random.Random()
    model = mod._build_model(rng)

    per_engine = {n: [] for n in engines}
    timeouts = {n: 0 for n in engines}
    for i in range(args.iterations):
        token = f"sentinel-{i}-{uuid.uuid4().hex[:8]}"
        commit_t = _write_sentinel(mod, table, model, rng, token)
        out = {}
        threads = [threading.Thread(target=_poll_until_visible,
                                    args=(n, cfg, index, token, commit_t, args.timeout, args.interval, out))
                   for n, cfg in engines.items()]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        line = []
        for n in engines:
            lag = out.get(n)
            if lag is None:
                timeouts[n] += 1
                line.append(f"{n}=TIMEOUT")
            else:
                per_engine[n].append(lag)
                line.append(f"{n}={lag/1000:.2f}s")
        print(f"  sentinel {i+1}/{args.iterations} {token}: " + "  ".join(line), file=sys.stderr)
        time.sleep(args.gap)

    report = {"workload": wl.name, "index": index, "table": table,
              "iterations": args.iterations, "timeout_s": args.timeout, "engines": {}}
    for n in engines:
        report["engines"][n] = {"samples": len(per_engine[n]), "timeouts": timeouts[n],
                                "lag_ms": _percentiles(per_engine[n])}
    Path(args.out).write_text(json.dumps(report, indent=2))
    print(f"\n== freshness ({args.iterations} sentinels) ==")
    print(f"{'engine':12}{'n':>4}{'timeouts':>10}{'p50_s':>9}{'p99_s':>9}{'max_s':>9}")
    for n, s in report["engines"].items():
        p = s["lag_ms"]
        print(f"{n:12}{s['samples']:>4}{s['timeouts']:>10}{p['p50']/1000:>9.2f}{p['p99']/1000:>9.2f}{p['max']/1000:>9.2f}")
    print(f"\nwrote {args.out}")


def cmd_selfcheck(args):
    """No network/Iceberg: validate poll-query construction, lag math, and the corpus write path. The
    corpus-row assertions mirror _write_sentinel, so a helper rename fails here offline, not in-cluster."""
    import random

    wl = Workload(args.workload)
    # the sentinel field must be a searchable (non-key-only) field in the index
    paths = [m["path"] for m in wl.index["mapping"]["fields"]]
    assert SENTINEL_FIELD in paths, f"{SENTINEL_FIELD} not an indexed field in {wl.name}"
    body = {"query": {"term": {SENTINEL_FIELD: "sentinel-x"}}, "size": 0}
    assert json.dumps(body)  # serializable
    # lag math: percentiles of a known set
    p = _percentiles([100.0, 200.0, 300.0])
    assert p["p50"] == 200.0 and p["max"] == 300.0, p

    # corpus write path: the helpers _write_sentinel/cmd_run call must exist and compose.
    mod = wl.corpus_module()
    missing = [a for a in ("_catalog", "_schema", "_rows", "_build_model") if not hasattr(mod, a)]
    assert not missing, f"corpus.py missing helpers {missing} (would break _write_sentinel)"
    rng = random.Random(0)
    cols = mod._rows(1, mod._build_model(rng), rng)
    assert all(len(v) == 1 for v in cols.values()), "corpus _rows(1, ...) must yield length-1 columns"
    cols[SENTINEL_FIELD] = ["sentinel-0-abc"]
    cols["ts"] = [1699920000]
    assert cols[SENTINEL_FIELD] == ["sentinel-0-abc"] and len(cols["ts"]) == 1
    try:  # if pyarrow is present, prove pa.table(cols, schema=_schema()) would accept these columns
        import pyarrow  # noqa: F401
        assert set(cols) == {f.name for f in mod._schema()}, "corpus columns != schema fields"
        schema_checked = True
    except ImportError:
        schema_checked = False
    print(f"self-check OK: sentinel field '{SENTINEL_FIELD}' indexed; poll body + lag math valid; "
          f"corpus write path composes (schema match {'checked' if schema_checked else 'skipped — no pyarrow'})")


def main():
    ap = argparse.ArgumentParser(description="End-to-end ingest-freshness harness (one clock, all engines)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run", help="inject sentinels + measure per-engine end-to-end lag")
    p.add_argument("workload")
    p.add_argument("--engines", default="growlerdb,opensearch")
    p.add_argument("--iterations", type=int, default=10)
    p.add_argument("--timeout", type=float, default=120.0, help="per-sentinel visibility timeout (s)")
    p.add_argument("--interval", type=float, default=0.25, help="poll interval (s)")
    p.add_argument("--gap", type=float, default=2.0, help="pause between sentinels (s)")
    p.add_argument("--out", default="freshness-report.json")
    p.set_defaults(fn=cmd_run)
    p = sub.add_parser("self-check", help="no-network logic check")
    p.add_argument("workload")
    p.set_defaults(fn=cmd_selfcheck)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
