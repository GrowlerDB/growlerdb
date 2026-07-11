#!/usr/bin/env python3
"""Staged multi-scale test driver.

Steps ingest rate and storage size, capturing the full metric set at each milestone so the scale
questions can be answered with graphs + a results table. Runs from a kubectl-capable host (Mac / CI
runner); talks to the in-cluster Prometheus + gateway via a port-forward (or in-cluster URLs).

  INGEST STEP-UPS   : set generator BATCH/SLEEP_S -> target records/s; record keep-up + lag + resources.
  STORAGE MILESTONES: grow source to 1/10/100 GB; at each freeze ingest, run the query load + the
                      convergence check, and snapshot query/hydration latency, index:source, resources.

Reachable scales are measured; 100k rec/s + 1 TB are extrapolated in analysis (see scale-test-plan).
Outputs results.json (milestone x metric). This is the orchestration; it does not itself fit/plot.
"""
import json, os, subprocess, time, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
NS = os.environ.get("NAMESPACE", "growlerdb")
PROM = os.environ.get("PROM_URL", "http://localhost:9090")
GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:8080")
INDEX = os.environ.get("INDEX", "http_logs")
WORKLOAD = os.environ.get("WORKLOAD", "http_logs")  # which query mix harness.py drives
CONCURRENCY = os.environ.get("CONCURRENCY", "16")
# (records/s target, BATCH, SLEEP_S) — reachable steps on the interim cluster; 100k is modeled.
# BATCH is the generator's per-append rows = one Iceberg SNAPSHOT = the connector's commit size (the
# connector cuts only at snapshot boundaries, so it can't sub-divide a snapshot). Commit latency is
# ~O(snapshot) — write p95 ~880ms @10k-row snapshots vs ~4.5s @150k — so KEEP BATCH bounded (≤ the
# connector's 50k maxCommitRows) and hit the rate with a shorter SLEEP_S, rather than a huge BATCH
# (a 300k BATCH self-inflicts ~9.5s p99 commits).
INGEST_STEPS = [(1000, 10000, 10), (10000, 30000, 3)]
STORAGE_GB = [float(x) for x in os.environ.get("STORAGE_GB", "1,10,100").split(",")]
ROW_BYTES = 28.0  # measured http_logs bytes/row; milestone target rows = GB / ROW_BYTES


def kubectl(*args):
    return subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True).stdout.strip()


def prom(expr):
    r = json.load(urllib.request.urlopen(f"{PROM}/api/v1/query?query=" + urllib.parse.quote(expr)))
    res = r["data"]["result"]
    return float(res[0]["value"][1]) if res else 0.0


def prom_by(expr, label):
    """A vector query keyed by `label` -> {label_value: float} (empty if the metric is absent)."""
    r = json.load(urllib.request.urlopen(f"{PROM}/api/v1/query?query=" + urllib.parse.quote(expr)))
    return {s["metric"].get(label, "?"): float(s["value"][1]) for s in r["data"]["result"]}


def set_ingest(batch, sleep_s):
    kubectl("set", "env", "deploy/growlerdb-generator", f"BATCH={batch}", f"SLEEP_S={sleep_s}")


# Generator replica count for resume: parallel generators sustain higher ingest; freeze scales to 0,
# resume restores to $GENERATORS (default 1).
GENERATORS = int(os.environ.get("GENERATORS", "1"))


def freeze_ingest():
    kubectl("scale", "deploy/growlerdb-generator", "--replicas=0")


def resume_ingest():
    kubectl("scale", "deploy/growlerdb-generator", f"--replicas={GENERATORS}")


def snapshot():
    """One capture of the metric set — GrowlerDB-native metrics (no external exporter dependency)."""
    return {
        "source_records": prom("max(growlerdb_source_records)"),
        "source_bytes": prom("max(growlerdb_source_bytes)"),
        "index_bytes": prom("sum(growlerdb_index_bytes)"),
        "index_docs": prom("sum(growlerdb_index_docs)"),
        "rows_behind": prom("max(growlerdb_source_records) - sum(growlerdb_index_docs)"),
        "ingest_rate_rps": prom("deriv(growlerdb_source_records[2m])"),
        "index_rate_dps": prom("sum(rate(growlerdb_ingested_docs_total[2m]))"),
        "query_p95_s": prom("histogram_quantile(0.95,sum(rate(growlerdb_query_duration_seconds_bucket[2m]))by(le))"),
        "hydration_p95_s": prom("histogram_quantile(0.95,sum(rate(growlerdb_hydration_duration_seconds_bucket[2m]))by(le))"),
        "node_cpu_cores": prom("sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[2m]))"),
        "index_source_ratio": prom("sum(growlerdb_index_bytes) / max(growlerdb_source_bytes)"),
        # Per-component index bytes: term/postings/positions/fieldnorms (the inverted index), fast,
        # store, locator, other — sums to index_bytes, so a ratio change is attributable to the
        # structure that moved (positions dropped, key terms shrunk, ...).
        "index_bytes_component": prom_by("sum by (component) (growlerdb_index_bytes_component)", "component"),
        # Measurement context: a size sample between merges carries superseded docs (NoMergePolicy —
        # purged only at compaction), so record the delete debt + segment count alongside; a milestone
        # with high debt overstates the steady-state footprint.
        "segments_live": prom("sum(growlerdb_segments_live)"),
        "index_deleted_docs": prom("sum(growlerdb_index_deleted_docs)"),
    }


def run_loadgen(seconds=180):
    """Drive the query mix against the gateway with the proven `harness.py query` driver and return
    its JSON report (per-query p50/p95/p99, errors, throughput). Reuses the same driver the validation
    runs use — no separate in-cluster loadgen image to build/maintain (an in-cluster Job that shells
    the same harness is a later, more-representative option; from the port-forward host is fine at
    these scales). Runs against GATEWAY (a port-forward or in-cluster URL) via GROWLERDB_OS_URL."""
    out = os.path.join(HERE, ".staged-loadgen.json")
    r = subprocess.run(
        ["python", os.path.join(HERE, "harness.py"), "query", WORKLOAD,
         "--duration", str(seconds), "--concurrency", CONCURRENCY, "--out", out],
        env={**os.environ, "GROWLERDB_OS_URL": GATEWAY}, capture_output=True, text=True)
    try:
        return json.loads(open(out).read())
    except (OSError, ValueError):
        return {"error": "loadgen produced no report", "stderr": r.stderr[-400:]}


def run_trino(seconds_label):
    """GrowlerDB-vs-Iceberg(Trino) comparison at this milestone — skipped if Trino isn't deployed.
    Delegates to compare_trino.py (same equivalent-predicate pairs), writing its result to a
    temp OUT this reads back. Honest framing: search+PK-hydrate vs table-scan, not general OLAP."""
    if not kubectl("get", "deploy", "trino", "--ignore-not-found"):
        return {"skipped": "trino not deployed"}
    out = os.path.join(HERE, ".staged-trino.json")
    subprocess.run(
        ["python", os.path.join(HERE, "compare_trino.py")],
        env={**os.environ, "GATEWAY_URL": GATEWAY, "INDEX": INDEX, "OUT": out}, capture_output=True, text=True)
    try:
        return json.loads(open(out).read())
    except (OSError, ValueError):
        return {"error": "trino comparison produced no report"}


def main():
    results = {"ingest_steps": [], "storage_milestones": []}

    # --- ingest step-ups: does GrowlerDB keep up? ---
    for target, batch, sleep_s in INGEST_STEPS:
        set_ingest(batch, sleep_s)
        time.sleep(240)  # let the rate settle
        s = snapshot()
        s["target_rps"] = target
        # Keep-up = indexing matches ingestion (backlog steady/draining), NOT rows_behind < target:
        # rows_behind is a row count and the connector commits in BATCH-sized chunks, so a single
        # steady batch (e.g. 30k) would trip a `< target` test as a false "not keeping up". If
        # index_rate >= ingest_rate the backlog isn't growing → keeping up; also record lag in seconds
        # for context (rows_behind / ingest_rate) rather than a bare count.
        s["lag_seconds"] = round(s["rows_behind"] / max(s["ingest_rate_rps"], 1), 1)
        s["keeps_up"] = s["index_rate_dps"] >= s["ingest_rate_rps"] * 0.98
        results["ingest_steps"].append(s)
        print(f"ingest {target}/s: index_rate={s['index_rate_dps']:.0f}/s rows_behind={s['rows_behind']:.0f}", flush=True)

    # --- storage milestones: query perf at each size ---
    resume_ingest()
    for gb in STORAGE_GB:
        target_rows = gb * 1e9 / ROW_BYTES
        while prom("max(growlerdb_source_records)") < target_rows:
            print(f"  waiting for {gb} GB ({prom('max(growlerdb_source_records)'):.0f}/{target_rows:.0f} rows)", flush=True)
            time.sleep(60)
        freeze_ingest()
        time.sleep(120)  # let indexing drain so the milestone converges
        load = run_loadgen(180)
        trino = run_trino(gb)
        conv = subprocess.run(["python", os.path.join(os.path.dirname(__file__), "convergence_check.py")],
                              env={**os.environ, "TOLERANCE": "0"}, capture_output=True, text=True)
        m = {"target_gb": gb, "snapshot": snapshot(), "load": load, "trino": trino,
             "convergence_pass": conv.returncode == 0}
        results["storage_milestones"].append(m)
        print(f"milestone {gb} GB: query_p95={m['snapshot']['query_p95_s']*1000:.1f}ms "
              f"ratio={m['snapshot']['index_source_ratio']:.2f}x converged={m['convergence_pass']} "
              f"delete_debt={m['snapshot']['index_deleted_docs']:.0f} "
              f"segments={m['snapshot']['segments_live']:.0f}", flush=True)
        resume_ingest()

    with open(os.environ.get("OUT", "results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(json.dumps(results, indent=2), flush=True)


if __name__ == "__main__":
    main()
