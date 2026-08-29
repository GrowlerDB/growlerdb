#!/usr/bin/env python3
"""Orchestrate the GrowlerDB-vs-OpenSearch comparison run: sequential-on-full-cluster (fairness charter)
— generate the corpus once, benchmark OpenSearch, then GrowlerDB, on the same table. OpenSearch ingests
FIRST, on the uncompacted table (Data Prepper's Iceberg CDC times out bulk-writing a whole compacted file);
GrowlerDB then cold-syncs the same uncompacted layout (fair T1) and compacts for its ts-pruned hydration.
Load drivers run IN-CLUSTER as Jobs (port-forwarded drivers report garbage); `--plan`/`--self-check`
run nothing. Assumes the cluster is up (scale-up.sh). See bench/scale/comparison-plan.md."""

import argparse
import json
import os
import string
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
DRIVER_TEMPLATE = REPO / "deploy" / "k8s" / "comparison" / "driver-job.template.yaml"
MAINTENANCE_YAML = REPO / "deploy" / "k8s" / "streaming" / "maintenance.yaml"
MAINTENANCE_CRONJOB = "growlerdb-iceberg-maintenance"  # name in maintenance.yaml (NOT growlerdb-maintenance)
RENDER_S3 = REPO / "deploy" / "k8s" / "render-s3.sh"  # S3-target renderer (s3-target.env: minio|hetzner)

ROW_BYTES = 400  # ~uncompressed bytes/row (see synthetic-corpus.md) — target_rows = target_gb*1e9/ROW_BYTES
SCALES = {"smoke": 1, "shakedown": 10, "full": 50}  # GB; smoke = fast full-flow validation

# Phase timeouts (s) scale with data volume; the slow steps at 50 GB are the CDC initial load (hours)
# and Spark compaction. conv_*_wait is the in-Job convergence --wait-timeout; the Job gets +CONV_MARGIN.
CONV_MARGIN = 900
# conv_gdb_wait covers a from-COLD connector backfill of the settled table (phase_gdb_coldsync), sized
# like conv_os_wait — both are full initial syncs at the connector's ~20k docs/s ceiling (~1.7h at 50 GB).
SCALE_TIMEOUTS = {
    "smoke":     {"conv_gdb_wait": 600,   "conv_os_wait": 900,   "compact": 900,  "query_job": 1200, "fresh_job": 900},
    "shakedown": {"conv_gdb_wait": 2400,  "conv_os_wait": 3000,  "compact": 1800, "query_job": 3600, "fresh_job": 1800},
    "full":      {"conv_gdb_wait": 10800, "conv_os_wait": 14400, "compact": 5400, "query_job": 5400, "fresh_job": 3600},
}
GEN_BATCH = int(os.environ.get("GEN_BATCH", "25000"))    # generator rows/commit (override demo 10)
GEN_SLEEP_S = int(os.environ.get("GEN_SLEEP_S", "1"))    # seconds between commits (override demo 5)

NS = os.environ.get("NAMESPACE", "growlerdb")
GIT_REF = os.environ.get("GIT_REF", "feat/os-comparison-bench")  # branch the driver Jobs clone
DRIVER_IMAGE = os.environ.get("DRIVER_IMAGE", "python:3.12-slim")
INDEX = os.environ.get("INDEX", "http_logs")
TABLE = os.environ.get("TABLE", "http_logs")  # Trino table under iceberg.growlerdb
ID_COL = os.environ.get("ID_COL", "request_id")  # http_logs PK (key-only; OpenSearch _id)
PROM_URL = os.environ.get("PROM_URL", "http://localhost:9090")  # capture-only (read-only metrics pull)

RESULT_BEGIN, RESULT_END = "<<<RESULT_JSON", "RESULT_JSON>>>"
DRY = False


def log(msg):
    print(f"[compare_run] {msg}", flush=True)


def sh(cmd, extra_env=None, check=True):
    """Run a shell command; in --plan mode just print it."""
    if DRY:
        print(f"  $ {cmd}")
        return 0
    env = {**os.environ, **(extra_env or {})}
    log(f"$ {cmd}")
    return subprocess.run(cmd, shell=True, env=env, check=check).returncode


def kubectl(args, input_text=None, check=True, capture=False):
    """Run `kubectl -n NS <args>`; supports stdin (apply -f -) and captured stdout."""
    cmd = ["kubectl", "-n", NS, *args]
    if DRY:
        print(f"  $ {' '.join(cmd)}" + ("  (with piped manifest)" if input_text else ""))
        return ""
    r = subprocess.run(cmd, input=input_text, text=True, check=check,
                       capture_output=capture)
    return (r.stdout or "") if capture else ""


def render_s3(path):
    """Render an S3-templated manifest against the active object-store target (deploy/k8s/render-s3.sh
    → s3-target.env; resolves S3_PROFILE=minio|hetzner). Returns the substituted YAML text."""
    return subprocess.run([str(RENDER_S3), str(path)], text=True, check=True,
                          capture_output=True).stdout


# --- in-cluster driver Jobs ---------------------------------------------------------------------

def render_driver_job(name, driver_cmd, pip):
    """Substitute the driver-Job template. `driver_cmd` MUST be a single line (block-scalar safety)."""
    if "\n" in driver_cmd:
        raise SystemExit(f"driver_cmd for job '{name}' must be single-line, got:\n{driver_cmd}")
    text = string.Template(DRIVER_TEMPLATE.read_text()).safe_substitute(
        JOB_NAME=name, NAMESPACE=NS, GIT_REF=GIT_REF, IMAGE=DRIVER_IMAGE, PIP=pip,
        DRIVER_CMD=driver_cmd)
    if "${" in text:
        raise SystemExit(f"driver-job.template.yaml: unresolved placeholder rendering '{name}'")
    return text


def wait_job(name, timeout_s):
    """Poll a Job until it succeeds or fails. Returns True on success."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        out = kubectl(["get", "job", name, "-o",
                       "jsonpath={.status.succeeded} {.status.failed}"], check=False, capture=True)
        succeeded, _, failed = out.strip().partition(" ")
        if succeeded.strip() not in ("", "0"):
            return True
        if failed.strip() not in ("", "0"):
            return False
        time.sleep(10)
    log(f"job/{name} did not finish within {timeout_s}s")
    return False


def _extract_result(logs, local_out):
    """Pull the marker-delimited JSON out of the pod logs and write it to `local_out`."""
    if RESULT_BEGIN not in logs or RESULT_END not in logs:
        log(f"no result markers in job logs — not writing {local_out}")
        return False
    payload = logs.split(RESULT_BEGIN, 1)[1].split(RESULT_END, 1)[0].strip()
    try:
        json.loads(payload)  # validate before persisting
    except json.JSONDecodeError as e:
        log(f"result JSON invalid ({e}); leaving {local_out} unwritten")
        return False
    Path(local_out).write_text(payload)
    log(f"wrote {local_out} from job logs")
    return True


def run_driver_job(name, core_cmd, pip="pyyaml", result_path=None, local_out=None,
                   timeout_s=3600, check=True):
    """Render + apply an in-cluster driver Job, wait for it, collect results from its logs. When
    result_path/local_out are given the pod cats that file between markers for host-side persistence."""
    if result_path:
        driver_cmd = (f"{core_cmd} ; rc=$? ; echo '{RESULT_BEGIN}' ; "
                      f"cat {result_path} 2>/dev/null ; echo '{RESULT_END}' ; exit $rc")
    else:
        driver_cmd = core_cmd
    manifest = render_driver_job(name, driver_cmd, pip)

    if DRY:
        print(f"  # driver Job '{name}' (image {DRIVER_IMAGE}, clone {GIT_REF}, pip: {pip})")
        print(f"  $ kubectl -n {NS} delete job {name} --ignore-not-found")
        print(f"  $ kubectl -n {NS} apply -f -   # rendered from driver-job.template.yaml, runs:")
        print(f"      {driver_cmd}")
        print(f"  $ kubectl -n {NS} wait job/{name} (poll succeeded/failed, timeout {timeout_s}s)")
        if local_out:
            print(f"  $ kubectl -n {NS} logs job/{name} --tail=-1  -> {local_out}")
        return True

    kubectl(["delete", "job", name, "--ignore-not-found"], check=False)
    kubectl(["apply", "-f", "-"], input_text=manifest)
    ok = wait_job(name, timeout_s)
    logs = kubectl(["logs", f"job/{name}", "--tail=-1"], check=False, capture=True)
    print(logs, flush=True)
    if local_out and result_path:
        _extract_result(logs, local_out)
    if not ok and check:
        raise SystemExit(f"driver job '{name}' failed (see logs above)")
    return ok


# --- generate -----------------------------------------------------------------------------------

def trino_count(sql):
    """Scalar count via `kubectl exec deploy/trino` — used from the kubectl-capable orchestrator host
    (the driver Jobs use the Trino REST API instead). Returns an int or None."""
    if DRY:
        print(f"  $ kubectl -n {NS} exec deploy/trino -- trino ... --execute {sql!r}")
        return None
    out = kubectl(["exec", "deploy/trino", "--", "trino", "--server", "localhost:8080",
                   "--catalog", "iceberg", "--schema", "growlerdb", "--output-format", "CSV",
                   "--execute", sql], check=False, capture=True)
    digits = [ln.strip().strip('"') for ln in out.splitlines() if ln.strip().strip('"').isdigit()]
    return int(digits[-1]) if digits else None


def wait_source_rows(target_rows, timeout_s=6 * 3600):
    """Block until the Iceberg source has >= target_rows (raw COUNT(*) via Trino), then scale the
    generator to 0. Dups don't matter here — this only gates corpus SIZE (convergence uses DISTINCT)."""
    if DRY:
        print(f"  # poll `SELECT COUNT(*) FROM {TABLE}` (kubectl exec trino) until >= {target_rows:,}, "
              f"then scale the generator to 0")
        return
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = trino_count(f"SELECT COUNT(*) FROM {TABLE}") or 0
        log(f"source rows ~{rows:,} / {target_rows:,}")
        if rows >= target_rows:
            break
        time.sleep(30)
    sh(f"kubectl -n {NS} scale deploy/growlerdb-generator --replicas=0", check=False)


def phase_generate(target_rows):
    log(f"PHASE generate — fill Iceberg source to ~{target_rows:,} rows (SPAN_DAYS=7)")
    # The generator template defaults to BATCH=10/SLEEP_S=5 (demo pace ~2 rows/s). Crank it to a
    # scale rate for the run (like staged_run.py does) before waiting for the target.
    sh(f"kubectl -n {NS} set env deploy/growlerdb-generator BATCH={GEN_BATCH} SLEEP_S={GEN_SLEEP_S}", check=False)
    wait_source_rows(target_rows)


# --- GrowlerDB cold-sync (ingest axis) ----------------------------------------------------------

def phase_gdb_coldsync(scale):
    """Scale OpenSearch down, deploy the GrowlerDB serving stack against the still-UNCOMPACTED table, and
    measure the cold full sync. Fair T1: both engines ingest the same small-file layout; compaction (which
    GrowlerDB's ts-pruned hydration needs) runs later in phase_growlerdb, once both have ingested. Nodes
    come up EMPTY (DEFINE_ONLY); the connector backfills."""
    log("PHASE gdb_coldsync — scale OpenSearch down, bring up GrowlerDB on the uncompacted table, measure the cold full sync")
    t = SCALE_TIMEOUTS[scale]
    sh(f"kubectl -n {NS} scale statefulset opensearch --replicas=0", check=False)
    sh(f"kubectl -n {NS} scale deploy data-prepper --replicas=0", check=False)
    sh(f"STAGE=serving DEFINE_ONLY=true NAMESPACE={NS} {REPO}/deploy/k8s/scale-up.sh")
    # Time to convergence (index docs == source DISTINCT id) IS the ingest headline (docs/s =
    # source_count / elapsed); started_epoch scopes the metrics window to the backfill. Gates the query phase.
    start = time.time()
    run_driver_job("conv-gdb",
                   f"ID_COL={ID_COL} TABLE={TABLE} INDEX={INDEX} python convergence_check.py "
                   f"--engine growlerdb --wait-timeout {t['conv_gdb_wait']} --poll 15",
                   timeout_s=t["conv_gdb_wait"] + CONV_MARGIN)
    elapsed = time.time() - start
    log(f"GrowlerDB cold-sync converged in {elapsed:.0f}s")
    capture(scale, "GrowlerDB cold-sync", params={"cold_sync_secs": f"{elapsed:.0f}"}, started_epoch=start)


# --- GrowlerDB query phase ----------------------------------------------------------------------

def compact_source(scale, deadline):
    """Compact the source so the Trino/Iceberg baseline reads a fair layout (not thousands of tiny
    streaming files): apply the maintenance CronJob, trigger a one-shot Job, raise its deadline, wait."""
    log(f"compact — deploy the maintenance CronJob + trigger a one-shot compaction (deadline {deadline}s)")
    job = f"compact-{scale}"
    # Render the maintenance Spark job against the active object-store target (S3_PROFILE) before apply,
    # so compaction reads/writes the same store (MinIO default | Hetzner) as every other component.
    maint = render_s3(MAINTENANCE_YAML)
    kubectl(["apply", "-f", "-"], input_text=maint, check=False)
    kubectl(["delete", "job", job, "--ignore-not-found"], check=False)
    kubectl(["create", "job", f"--from=cronjob/{MAINTENANCE_CRONJOB}", job], check=False)
    kubectl(["patch", "job", job, "--type", "merge",
             "-p", json.dumps({"spec": {"activeDeadlineSeconds": deadline}})], check=False)
    if not DRY:
        wait_job(job, timeout_s=deadline + 300)


def phase_growlerdb(scale):
    log("PHASE growlerdb — compact, warmup, query matrix, freshness, capture")
    t = SCALE_TIMEOUTS[scale]
    # Convergence already passed in phase_gdb_coldsync. Here: compact (ts-clustered files) for a fair
    # Trino baseline AND so the store-less PREDICATE hydration prunes by ts stats, then warm the index.
    compact_source(scale, t["compact"])
    # Warmup (best-effort): a short query pass so plan-cache / object-store warmup happens OFF the
    # clock. Results discarded (no result_path).
    run_driver_job(
        "warmup-gdb",
        "python compare_query.py run http_logs --engines growlerdb --qps 50 --duration 30 --out /tmp/out.json",
        timeout_s=t["fresh_job"], check=False)
    run_driver_job(
        "query-gdb",
        "python compare_query.py run http_logs --engines growlerdb "
        "--qps 200 --duration 120 --sweep 50,100,200,400,800 --sweep-duration 30 --out /tmp/out.json",
        result_path="/tmp/out.json", local_out=f"{HERE}/gdb-query.json", timeout_s=t["query_job"])
    # Trino/Iceberg-scan baseline (fairness axis 1): search+hydrate vs table scan over the same
    # post-compaction table. Best-effort (check=False) — an informative baseline, not a gate.
    run_driver_job(
        "trino-gdb",
        f"OUT=/tmp/out.json INDEX={INDEX} TRINO_TABLE={TABLE} python compare_trino.py",
        result_path="/tmp/out.json", local_out=f"{HERE}/gdb-trino.json", timeout_s=t["query_job"], check=False)
    run_driver_job(
        "fresh-gdb",
        "python compare_freshness.py run http_logs --engines growlerdb --iterations 20 --out /tmp/out.json",
        pip="pyyaml pyiceberg pyarrow", result_path="/tmp/out.json",
        local_out=f"{HERE}/gdb-freshness.json", timeout_s=t["fresh_job"], check=False)
    capture(scale, "GrowlerDB", f"{HERE}/gdb-query.json", f"{HERE}/gdb-freshness.json")


# --- OpenSearch cold-sync + query phase ---------------------------------------------------------
# OpenSearch runs FIRST, on the UNCOMPACTED table: Data Prepper's Iceberg CDC source reads a whole
# compacted ~128 MB file as one giant initial-load partition whose bulk-write to OS times out (poison-
# pill retry, OS idle), so compaction MUST follow the CDC load. GrowlerDB — which needs the sort-
# clustered compaction for its ts-pruned hydration — ingests next and compacts in phase_growlerdb.

def phase_os_coldsync(scale):
    """Bring up OpenSearch + Data Prepper on the UNCOMPACTED table and measure the CDC cold sync (the ingest
    counterpart to GrowlerDB's connector backfill). Runs before any compaction — Data Prepper's Iceberg CDC
    chokes on compacted large files (see the section note)."""
    log("PHASE os_coldsync — bring up OpenSearch + Data Prepper on the uncompacted table, measure the CDC cold sync")
    t = SCALE_TIMEOUTS[scale]
    sh(f"NAMESPACE={NS} {REPO}/deploy/k8s/comparison/up.sh")
    # Convergence gate: OpenSearch _count == source DISTINCT id (CDC initial load complete). That load
    # is the run's slowest step (hours at 50 GB); timed + captured like phase_gdb_coldsync for symmetry.
    start = time.time()
    run_driver_job("conv-os",
                   f"ID_COL={ID_COL} TABLE={TABLE} INDEX={INDEX} python convergence_check.py "
                   f"--engine opensearch --wait-timeout {t['conv_os_wait']} --poll 30",
                   timeout_s=t["conv_os_wait"] + CONV_MARGIN)
    elapsed = time.time() - start
    log(f"OpenSearch CDC cold-sync converged in {elapsed:.0f}s")
    capture(scale, "OpenSearch cold-sync", params={"cold_sync_secs": f"{elapsed:.0f}"}, started_epoch=start)


def phase_opensearch(scale):
    """OpenSearch query matrix + freshness (already converged in phase_os_coldsync). No compaction needed —
    OS queries its own Lucene index, independent of the Iceberg file layout."""
    log("PHASE opensearch — query matrix, freshness, capture")
    t = SCALE_TIMEOUTS[scale]
    run_driver_job(
        "query-os",
        "python compare_query.py run http_logs --engines opensearch "
        "--qps 200 --duration 120 --sweep 50,100,200,400,800 --sweep-duration 30 --out /tmp/out.json",
        result_path="/tmp/out.json", local_out=f"{HERE}/os-query.json", timeout_s=t["query_job"])
    run_driver_job(
        "fresh-os",
        "python compare_freshness.py run http_logs --engines opensearch --iterations 20 --out /tmp/out.json",
        pip="pyyaml pyiceberg pyarrow", result_path="/tmp/out.json",
        local_out=f"{HERE}/os-freshness.json", timeout_s=t["fresh_job"], check=False)
    capture(scale, "OpenSearch", f"{HERE}/os-query.json", f"{HERE}/os-freshness.json")


# --- capture (host-side; read-only Prometheus pull) ---------------------------------------------

def capture(scale, phase_name, query_json="", freshness_json="", params=None, started_epoch=None):
    """Fold a phase's result JSONs + a Prometheus metrics window into a run dir + ledger row. Host-side
    (read-only PROM_URL scrape, port-forward OK); `started_epoch` scopes the window, `params` records scalars."""
    a = [f"--purpose 'comparison {scale} — {phase_name} phase'"]
    if query_json:
        a.append(f"--comparison {query_json}")
    if freshness_json:
        a.append(f"--freshness {freshness_json}")
    if started_epoch is not None:
        a.append(f"--started-epoch {started_epoch:.0f}")
    for k, v in (params or {}).items():
        a.append(f"--param {k}={v}")
    # sys.executable, not "python": host `/bin/sh` often has only python3 on PATH (macOS), so a bare
    # `python` silently no-ops capture. Driver-Job core_cmds keep `python` (python:3.12-slim has it).
    sh(f"{sys.executable} {HERE}/capture.py " + " ".join(a), {"PROM_URL": PROM_URL}, check=False)


def phase_finalize():
    log("PHASE finalize — push corpus + result artifacts to the bucket (if HETZNER_S3_* is set)")
    if os.environ.get("HETZNER_S3_BUCKET"):
        sh(f"bash {HERE}/artifacts.sh push", check=False)
    else:
        log("HETZNER_S3_* not set — skipping artifact push (create the bucket + set creds to enable)")


# Order matters (run-all iterates insertion order): generate the settled corpus → OpenSearch cold-sync +
# query on the UNCOMPACTED table (Data Prepper's Iceberg CDC chokes on compacted large files) → GrowlerDB
# cold-sync (both engines ingest the same small-file layout = fair T1) → compact + GrowlerDB query (its
# ts-pruned hydration needs the sort-clustered compaction) → finalize. phase_gdb_coldsync scales
# OpenSearch down first; phase_growlerdb owns the single compaction.
PHASES = {"generate": phase_generate, "os_coldsync": phase_os_coldsync, "opensearch": phase_opensearch,
          "gdb_coldsync": phase_gdb_coldsync, "growlerdb": phase_growlerdb, "finalize": phase_finalize}


# --- offline self-check -------------------------------------------------------------------------

def self_check():
    """Render the driver Job offline and validate it (no cluster). Exercises the template + wrapping."""
    fails = []
    cmd = ("python compare_query.py run http_logs --engines growlerdb --qps 200 --out /tmp/out.json"
           " ; rc=$? ; echo 'x' ; exit $rc")
    manifest = render_driver_job("query-gdb", cmd, "pyyaml")
    if "${" in manifest:
        fails.append("unresolved placeholder in rendered manifest")
    for needle in (f"namespace: {NS}", GIT_REF, "gdb-growlerdb-gateway:8080",
                   "opensearch:9200", "trino:8080", "name: query-gdb"):
        if needle not in manifest:
            fails.append(f"expected {needle!r} in manifest")
    try:
        import yaml  # optional — validate real YAML when available
        docs = list(yaml.safe_load_all(manifest))
        assert docs and docs[0]["kind"] == "Job", "rendered doc is not a Job"
    except ImportError:
        print("(pyyaml not installed — skipped strict YAML parse; text checks only)")
    except Exception as e:  # noqa: BLE001
        fails.append(f"YAML parse: {e}")
    # multi-line driver_cmd must be rejected
    try:
        render_driver_job("bad", "line1\nline2", "pyyaml")
        fails.append("multi-line driver_cmd was not rejected")
    except SystemExit:
        pass
    if fails:
        print("SELF-CHECK FAIL:")
        for f in fails:
            print("  -", f)
        sys.exit(1)
    print("self-check OK: driver Job renders, endpoints wired, single-line guard holds")


def main():
    global DRY
    ap = argparse.ArgumentParser(description="Orchestrate the GrowlerDB-vs-OpenSearch comparison run")
    ap.add_argument("--scale", choices=SCALES, default="shakedown")
    ap.add_argument("--phase", choices=["all", *PHASES], default="all", help="run one phase (resume)")
    ap.add_argument("--plan", action="store_true", help="print the ordered steps/commands, run nothing")
    ap.add_argument("--self-check", action="store_true", help="render + validate the driver Job offline, exit")
    args = ap.parse_args()
    if args.self_check:
        self_check()
        return
    DRY = args.plan
    target_rows = int(SCALES[args.scale] * 1e9 / ROW_BYTES)

    log(f"scale={args.scale} (~{SCALES[args.scale]} GB, ~{target_rows:,} rows), phase={args.phase}, "
        f"plan={args.plan}, ns={NS}, git_ref={GIT_REF}")
    order = list(PHASES) if args.phase == "all" else [args.phase]
    for name in order:
        fn = PHASES[name]
        # generate takes the row target, finalize takes nothing, every other phase takes the scale.
        fn(target_rows) if name == "generate" else (fn() if name == "finalize" else fn(args.scale))
    log("done" if not DRY else "plan complete (nothing executed)")


if __name__ == "__main__":
    main()
