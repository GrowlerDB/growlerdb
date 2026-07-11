#!/usr/bin/env python3
"""Source→index convergence check: assert GrowlerDB matches Iceberg.

At steady state (after ingest drains) two things must hold:
  1. Count convergence — the index's live doc count == the source's DISTINCT-id count.
  2. No dup/loss — a sample of real indexed ids each resolves to exactly one doc that hydrates to a
     row.

Why DISTINCT, not raw rows: GrowlerDB collapses duplicate PKs last-write-wins, so raw source rows
exceed index docs whenever the source has duplicate ids (e.g. an OOM-restarted generator re-emitting
its id sequence). Comparing to the raw `total-records` metric is therefore *dup-fooled*; the
authoritative target is `COUNT(DISTINCT id)`, queried from Trino over the same Iceberg table. (This
mirrors the k8s drain gate `deploy/k8s/streaming/convergence-gate.sh`, which uses spark-sql for the
same distinct count; this script adds the sample+hydrate integrity check and the staged-protocol
JSON verdict.)

Index doc count = the gateway's match-all `total` (what search actually serves), i.e. GrowlerDB's
own live count — no dependency on the external scale-test exporter. `growlerdb_index_docs` (native)
drives the live convergence graph; this point check reads `total` directly.

Runs from a kubectl-capable host: gateway via GATEWAY_URL (port-forward or in-cluster), Trino via
`kubectl exec deploy/trino`. Exits non-zero on failure so it gates the staged protocol. Set TRINO=0
to fall back to the raw `growlerdb_source_records` metric (clearly flagged dup-UNSAFE) when Trino
isn't deployed.
"""
import json, os, subprocess, urllib.parse, urllib.request

GATEWAY = os.environ.get("GATEWAY_URL", "http://gdb-growlerdb-gateway:8080")
PROM = os.environ.get("PROM_URL", "http://prometheus:9090")
NS = os.environ.get("NAMESPACE", "growlerdb")
INDEX = os.environ.get("INDEX", "http_logs")
TABLE = os.environ.get("TABLE", "http_logs")  # Trino table under the iceberg.growlerdb schema
ID_COL = os.environ.get("ID_COL", "id")
SAMPLE = int(os.environ.get("SAMPLE", "50"))
TOLERANCE = int(os.environ.get("TOLERANCE", "0"))  # rows; >0 allows in-flight lag
USE_TRINO = os.environ.get("TRINO", "1") != "0"


def _post(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def prom(expr):
    r = _get_json(f"{PROM}/api/v1/query?query=" + urllib.parse.quote(expr))["data"]["result"]
    return float(r[0]["value"][1]) if r else 0.0


def _get_json(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def search(query, limit=1, offset=0):
    body = {"index": INDEX, "query": query, "limit": limit}
    if offset:
        body["offset"] = offset
    return _post(f"{GATEWAY}/v1/search", body)


def index_total():
    """GrowlerDB's live doc count = the match-all `total` the gateway serves."""
    return int(search("*", limit=0).get("total", 0))


def source_distinct():
    """The authoritative target: COUNT(DISTINCT id) over the Iceberg table via Trino (dup-safe)."""
    if not USE_TRINO:
        return int(prom("max(growlerdb_source_records)")), "raw-metric-DUP-UNSAFE"
    out = subprocess.run(
        ["kubectl", "-n", NS, "exec", "deploy/trino", "--", "trino", "--server", "localhost:8080",
         "--catalog", "iceberg", "--schema", "growlerdb", "--output-format", "CSV",
         "--execute", f"SELECT COUNT(DISTINCT {ID_COL}) FROM {TABLE}"],
        capture_output=True, text=True, timeout=300)
    digits = [ln.strip().strip('"') for ln in out.stdout.splitlines() if ln.strip().strip('"').isdigit()]
    if not digits:
        raise SystemExit(f"could not parse a DISTINCT count from Trino: {out.stdout!r} {out.stderr[-300:]!r}")
    return int(digits[-1]), "trino-distinct"


def sample_ids(n):
    """Dataset-agnostic: take real ids straight from a match-all page (no id-format assumptions)."""
    hits = search("*", limit=n).get("hits", [])
    ids = []
    for h in hits:
        for f in h.get("coordinates", {}).get("fields", []):
            if f.get("name") == ID_COL:
                ids.append(f.get("value"))
    return ids


def main():
    idx = index_total()
    src, src_method = source_distinct()
    delta = src - idx
    count_ok = abs(delta) <= TOLERANCE

    # Sample real ids: each must resolve to exactly one doc and hydrate to a row.
    dup, missing, mismatch, checked = 0, 0, 0, 0
    for rid in sample_ids(SAMPLE):
        res = search(f'{ID_COL}:"{rid}"', limit=5)
        hits = res.get("hits", [])
        if len(hits) == 0:
            missing += 1
            continue
        if len(hits) > 1:
            dup += 1
        checked += 1
        rows = _post(f"{GATEWAY}/v1/keys:get", {"keys": [hits[0]["coordinates"]]}).get("rows", [])
        if not rows:
            mismatch += 1

    sample_ok = dup == 0 and missing == 0 and mismatch == 0
    verdict = {
        "index": INDEX,
        "source_distinct_ids": src, "source_count_method": src_method,
        "index_docs": idx, "rows_behind": delta,
        "count_convergence": "PASS" if count_ok else f"FAIL (delta={delta}, tol={TOLERANCE})",
        "sample": {"checked": checked, "duplicates": dup, "missing": missing, "hydrate_mismatch": mismatch},
        "sample_integrity": "PASS" if sample_ok else "FAIL",
        "result": "PASS" if (count_ok and sample_ok) else "FAIL",
    }
    print(json.dumps(verdict, indent=2), flush=True)
    raise SystemExit(0 if verdict["result"] == "PASS" else 1)


if __name__ == "__main__":
    main()
