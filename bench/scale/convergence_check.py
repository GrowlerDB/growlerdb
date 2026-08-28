#!/usr/bin/env python3
"""Source->index convergence check: assert an engine matches the Iceberg source.

At steady state (after ingest drains) the engine's live doc count must equal the source's DISTINCT-id
count. Two engines, one target:

  * ``--engine growlerdb`` (default) — index live-doc count == source DISTINCT id, plus a
    sample-integrity check: a page of real hits each hydrates from Iceberg (the key->row invariant).
  * ``--engine opensearch`` — OpenSearch ``_count`` == source DISTINCT id. OpenSearch dedups by
    ``_id`` (= request_id, set by Data Prepper ``identifier_columns``), so the count IS the distinct
    target and there is no separate hydrate step to sample.

Why DISTINCT, not raw rows: both engines collapse duplicate PKs last-write-wins, so raw source rows
exceed doc count whenever the source has duplicate ids (e.g. an OOM-restarted generator re-emitting
its id sequence). Comparing to the raw ``total-records`` metric is therefore *dup-fooled*; the
authoritative target is ``COUNT(DISTINCT request_id)`` over the same Iceberg table.

Source count path (in priority order): ``TRINO_URL`` (the Trino REST API, reachable in-cluster — this
is what lets the check run inside a driver Job with no kubectl); else ``kubectl exec deploy/trino``
(a kubectl-capable host); else ``TRINO=0`` falls back to the raw ``growlerdb_source_records`` metric
(clearly flagged dup-UNSAFE). Exits non-zero on failure so it gates the run's phases.
"""
import argparse
import json
import os
import subprocess
import time
import urllib.parse
import urllib.request

GATEWAY = os.environ.get("GATEWAY_URL", "http://gdb-growlerdb-gateway:8080")
OPENSEARCH = os.environ.get("OPENSEARCH_URL", "http://opensearch:9200")
TRINO_URL = os.environ.get("TRINO_URL", "")  # e.g. http://trino:8080 — enables the in-cluster HTTP path
PROM = os.environ.get("PROM_URL", "http://prometheus:9090")
NS = os.environ.get("NAMESPACE", "growlerdb")
INDEX = os.environ.get("INDEX", "http_logs")
TABLE = os.environ.get("TABLE", "http_logs")  # Trino table under the iceberg.growlerdb schema
ID_COL = os.environ.get("ID_COL", "request_id")  # http_logs PK (key-only; OpenSearch _id)
SAMPLE = int(os.environ.get("SAMPLE", "50"))
TOLERANCE = int(os.environ.get("TOLERANCE", "0"))  # rows; >0 allows in-flight lag
USE_TRINO = os.environ.get("TRINO", "1") != "0"


def _post(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def _get_json(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def prom(expr):
    r = _get_json(f"{PROM}/api/v1/query?query=" + urllib.parse.quote(expr))["data"]["result"]
    return float(r[0]["value"][1]) if r else 0.0


# --- source DISTINCT-id count -------------------------------------------------------------------

def _trino_http_scalar(sql):
    """Run one SQL statement through the Trino REST API and return its single scalar cell.

    POST /v1/statement returns a chain of pages linked by nextUri; follow them, collecting `data`
    rows, until the chain ends (or errors). In-cluster this needs no kubectl — the whole point, so
    a driver Job can compute the distinct count itself."""
    req = urllib.request.Request(
        f"{TRINO_URL}/v1/statement", data=sql.encode(),
        headers={"X-Trino-User": "bench", "X-Trino-Catalog": "iceberg", "X-Trino-Schema": "growlerdb"})
    rows, nxt = [], None
    with urllib.request.urlopen(req, timeout=120) as r:
        page = json.loads(r.read())
    while True:
        if page.get("error"):
            raise SystemExit(f"Trino error: {page['error'].get('message')}")
        rows.extend(page.get("data") or [])
        nxt = page.get("nextUri")
        if not nxt:
            break
        with urllib.request.urlopen(nxt, timeout=120) as r:
            page = json.loads(r.read())
    return rows[-1][0] if rows and rows[-1] else None


def _trino_exec_scalar(sql):
    """Same count via `kubectl exec deploy/trino` — for a kubectl-capable host without TRINO_URL."""
    out = subprocess.run(
        ["kubectl", "-n", NS, "exec", "deploy/trino", "--", "trino", "--server", "localhost:8080",
         "--catalog", "iceberg", "--schema", "growlerdb", "--output-format", "CSV",
         "--execute", sql],
        capture_output=True, text=True, timeout=300)
    digits = [ln.strip().strip('"') for ln in out.stdout.splitlines() if ln.strip().strip('"').isdigit()]
    if not digits:
        raise SystemExit(f"could not parse a DISTINCT count from Trino: {out.stdout!r} {out.stderr[-300:]!r}")
    return digits[-1]


def source_distinct():
    """The authoritative target: COUNT(DISTINCT request_id) over the Iceberg table (dup-safe)."""
    if not USE_TRINO:
        return int(prom("max(growlerdb_source_records)")), "raw-metric-DUP-UNSAFE"
    sql = f"SELECT COUNT(DISTINCT {ID_COL}) FROM {TABLE}"
    if TRINO_URL:
        return int(_trino_http_scalar(sql)), "trino-http-distinct"
    return int(_trino_exec_scalar(sql)), "trino-exec-distinct"


# --- per-engine index doc count -----------------------------------------------------------------

def growlerdb_search(query, limit=1):
    return _post(f"{GATEWAY}/v1/search", {"index": INDEX, "query": query, "limit": limit})


def growlerdb_count():
    """GrowlerDB's live doc count = the match-all `total` the gateway serves."""
    return int(growlerdb_search("*", limit=0).get("total", 0))


def opensearch_count():
    """OpenSearch live doc count via `_count` (== DISTINCT request_id: it dedups by _id)."""
    return int(_post(f"{OPENSEARCH}/{INDEX}/_count", {"query": {"match_all": {}}}).get("count", 0))


def growlerdb_sample(n):
    """Take a page of real hits and hydrate each coordinate — the key->row invariant. Keyed on
    coordinates, not an id term query: request_id is key-only (not searchable), so `id:"..."` can't
    sample. Net loss is caught by count convergence; this checks that live hits still hydrate."""
    hits = growlerdb_search("*", limit=n).get("hits", [])
    coords = [h["coordinates"] for h in hits if "coordinates" in h]
    checked, mismatch = 0, 0
    for c in coords:
        checked += 1
        rows = _post(f"{GATEWAY}/v1/keys:get", {"keys": [c]}).get("rows", [])
        if not rows:
            mismatch += 1
    return {"checked": checked, "hydrate_mismatch": mismatch,
            "note": "duplicate-by-id check n/a (request_id is key-only, not searchable)"}


def main():
    global OPENSEARCH
    ap = argparse.ArgumentParser(description="Source->index convergence check (count == source DISTINCT id)")
    ap.add_argument("--engine", choices=["growlerdb", "opensearch"], default="growlerdb")
    ap.add_argument("--opensearch-url", default=OPENSEARCH, help="OpenSearch base URL (opensearch engine)")
    ap.add_argument("--wait-timeout", type=float, default=0.0,
                    help="seconds to poll for count convergence before failing (0 = one-shot check). "
                         "Makes this a real gate: OpenSearch CDC / GrowlerDB indexing drains to the "
                         "frozen source over time.")
    ap.add_argument("--poll", type=float, default=15.0, help="poll interval while waiting (s)")
    args = ap.parse_args()
    OPENSEARCH = args.opensearch_url
    doc_count = opensearch_count if args.engine == "opensearch" else growlerdb_count

    # Source is frozen (generator stopped) so DISTINCT id is stable — compute it once, then poll the
    # engine's live doc count up to it.
    src, src_method = source_distinct()
    deadline = time.time() + args.wait_timeout
    while True:
        idx = doc_count()
        delta = src - idx
        count_ok = abs(delta) <= TOLERANCE
        if count_ok or time.time() >= deadline:
            break
        print(f"waiting for convergence: {args.engine} {idx:,} / {src:,} (behind {delta:,})", flush=True)
        time.sleep(args.poll)

    if args.engine == "opensearch":
        sample = {"checked": 0, "note": "OpenSearch dedups by _id; count convergence is the check"}
        sample_ok = True
    else:
        sample = growlerdb_sample(SAMPLE)
        sample_ok = sample["hydrate_mismatch"] == 0
    verdict = {
        "engine": args.engine, "index": INDEX,
        "source_distinct_ids": src, "source_count_method": src_method,
        "doc_count": idx, "rows_behind": delta,
        "count_convergence": "PASS" if count_ok else f"FAIL (delta={delta}, tol={TOLERANCE})",
        "sample": sample,
        "sample_integrity": "PASS" if sample_ok else "FAIL",
        "result": "PASS" if (count_ok and sample_ok) else "FAIL",
    }
    print(json.dumps(verdict, indent=2), flush=True)
    raise SystemExit(0 if verdict["result"] == "PASS" else 1)


if __name__ == "__main__":
    main()
