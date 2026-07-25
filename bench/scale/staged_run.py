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
import json, os, subprocess, sys, time, urllib.parse, urllib.request

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
# Baseline 5 GB (raw uncompressed) — Run-7-comparable scale on the honest raw basis (TASK-347).
# ~26M http_logs rows; at the ~8.9k docs/s single-node ceiling that's ~50 min of ingest, so raise
# GENERATORS to parallelize a real run. Override with STORAGE_GB=... for a quick/cheap pass.
STORAGE_GB = [float(x) for x in os.environ.get("STORAGE_GB", "5,10,100").split(",")]
# Milestones + index:source are sized against the UNCOMPRESSED raw-corpus size (the OSB / ES-benchmark
# convention), NOT the compressed parquet on-disk size. `growlerdb_source_bytes` is Iceberg's
# `total-files-size` (compressed) — that basis SHIFTS under storage config that doesn't change the
# logical data (parquet↔orc, zstd↔snappy, dictionary/RLE), so numbers stop being comparable across
# runs. Raw uncompressed is stable (TASK-342).
#
# Ground truth = the generator's own uncompressed byte count, but the raw COUNTER
# (`growlerdb_gen_raw_bytes_total`) is per-pod and RESETS whenever the generator restarts —
# `set_ingest`/`freeze`/`resume` all restart it — so summing it undercounts the cumulative corpus
# (TASK-344; Run 8 read 0.80 GB vs ≈1.12 GB true). So take the generator's mean uncompressed
# bytes/row (raw_bytes / rows — a RATIO, invariant to the counter resetting since both reset
# together) and multiply by the cumulative `source_records` (Iceberg, never resets). RAW_ROW_BYTES is
# the fallback mean for runs whose generator predates the metric (http_logs ≈ 140, OSB ≈ 128).
# STORAGE_GB is GB of uncompressed corpus.
RAW_ROW_BYTES = float(os.environ.get("RAW_ROW_BYTES", "140"))


def raw_source_bytes():
    """Uncompressed corpus size, restart-durable (TASK-344): the generator's mean bytes/row (a ratio,
    so unaffected by its per-pod counter resetting on restart) × the cumulative `source_records`.
    Falls back to RAW_ROW_BYTES × rows when the generator metric is absent (older runs). Returns bytes."""
    rows = prom("sum(growlerdb_gen_rows_total)")
    raw = prom("sum(growlerdb_gen_raw_bytes_total)")
    src = prom("max(growlerdb_source_records)")
    mean_bytes_per_row = (raw / rows) if rows > 0 else RAW_ROW_BYTES
    return src * mean_bytes_per_row


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
    snap = {
        "source_records": prom("max(growlerdb_source_records)"),
        "source_bytes": prom("max(growlerdb_source_bytes)"),  # COMPRESSED parquet on-disk (total-files-size)
        "index_bytes": prom("sum(growlerdb_index_bytes)"),
        "index_docs": prom("sum(growlerdb_index_docs)"),
        "rows_behind": prom("max(growlerdb_source_records) - sum(growlerdb_index_docs)"),
        "ingest_rate_rps": prom("deriv(growlerdb_source_records[2m])"),
        "index_rate_dps": prom("sum(rate(growlerdb_ingested_docs_total[2m]))"),
        "query_p95_s": prom("histogram_quantile(0.95,sum(rate(growlerdb_query_duration_seconds_bucket[2m]))by(le))"),
        "hydration_p95_s": prom("histogram_quantile(0.95,sum(rate(growlerdb_hydration_duration_seconds_bucket[2m]))by(le))"),
        "node_cpu_cores": prom("sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[2m]))"),
        # index:source vs COMPRESSED parquet — kept for continuity, but config-dependent (see below).
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
    # UNCOMPRESSED-raw basis (TASK-342): the stable, config-independent headline. raw_source_bytes =
    # rows × RAW_ROW_BYTES; index_source_ratio_raw = index_bytes / raw_source_bytes (index vs the
    # logical corpus, not vs the codec-dependent parquet footprint). compression_ratio exposes how
    # much of the "index:source vs compressed" number is just parquet doing its job.
    snap["raw_source_bytes"] = raw_source_bytes()
    snap["index_source_ratio_raw"] = (
        snap["index_bytes"] / snap["raw_source_bytes"] if snap["raw_source_bytes"] else 0.0
    )
    snap["compression_ratio"] = (
        snap["raw_source_bytes"] / snap["source_bytes"] if snap["source_bytes"] else 0.0
    )
    return snap


def run_loadgen(seconds=180):
    """Drive the query mix against the gateway with the proven `harness.py query` driver and return
    its JSON report (per-query p50/p95/p99, errors, throughput). Reuses the same driver the validation
    runs use — no separate in-cluster loadgen image to build/maintain (an in-cluster Job that shells
    the same harness is a later, more-representative option; from the port-forward host is fine at
    these scales). Runs against GATEWAY (a port-forward or in-cluster URL) via GROWLERDB_OS_URL."""
    out = os.path.join(HERE, ".staged-loadgen.json")
    r = subprocess.run(
        [sys.executable, os.path.join(HERE, "harness.py"), "query", WORKLOAD,
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
        [sys.executable, os.path.join(HERE, "compare_trino.py")],
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
        target_bytes = gb * 1e9  # GB of UNCOMPRESSED corpus, from the generator's own counter (TASK-342)
        while raw_source_bytes() < target_bytes:
            print(f"  waiting for {gb:g} GB uncompressed ({raw_source_bytes() / 1e9:.2f}/{gb:g} GB)", flush=True)
            time.sleep(60)
        freeze_ingest()
        time.sleep(120)  # let indexing drain so the milestone converges
        load = run_loadgen(180)
        trino = run_trino(gb)
        conv = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "convergence_check.py")],
                              env={**os.environ, "TOLERANCE": "0"}, capture_output=True, text=True)
        m = {"target_gb": gb, "snapshot": snapshot(), "load": load, "trino": trino,
             "convergence_pass": conv.returncode == 0}
        results["storage_milestones"].append(m)
        print(f"milestone {gb} GB: query_p95={m['snapshot']['query_p95_s']*1000:.1f}ms "
              f"idx:src(raw)={m['snapshot']['index_source_ratio_raw']:.2f}x "
              f"(vs-compressed={m['snapshot']['index_source_ratio']:.2f}x) converged={m['convergence_pass']} "
              f"delete_debt={m['snapshot']['index_deleted_docs']:.0f} "
              f"segments={m['snapshot']['segments_live']:.0f}", flush=True)
        resume_ingest()

    with open(os.environ.get("OUT", "results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(json.dumps(results, indent=2), flush=True)


if __name__ == "__main__":
    main()
