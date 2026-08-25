#!/usr/bin/env python3
"""Orchestrate the GrowlerDB-vs-OpenSearch comparison run end to end.

Sequential-on-full-cluster (fairness charter): generate the corpus once into the shared Iceberg table,
benchmark GrowlerDB on the whole cluster, tear it down, then benchmark OpenSearch (Data Prepper CDC)
on the same table. Each phase drives already-validated tools; this script only sequences them, waits
on the right conditions, and captures artifacts.

Assumes the cluster is provisioned (deploy/iac `terraform apply`) and the GrowlerDB stack is up
(deploy/k8s/scale-up.sh, which also starts the generator, deps, observability, and Trino). Run
`--plan` first to print the ordered steps + commands without touching anything.

Endpoints reach the cluster via port-forwards this script opens; the sub-tools read them from
GROWLERDB_OS_URL / OPENSEARCH_URL / PROM_URL. Scale: `--scale shakedown` (10 GB) proves the flow
cheaply before `--scale full` (50 GB).
"""

import argparse
import contextlib
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
ROW_BYTES = 400  # ~uncompressed bytes/row (see synthetic-corpus.md) — target_rows = target_gb*1e9/ROW_BYTES
SCALES = {"shakedown": 10, "full": 50}

NS = os.environ.get("NAMESPACE", "growlerdb")
GATEWAY_URL = os.environ.get("GROWLERDB_OS_URL", "http://localhost:8081")
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
PROM_URL = os.environ.get("PROM_URL", "http://localhost:9090")

DRY = False


def log(msg):
    print(f"[compare_run] {msg}", flush=True)


def sh(cmd, extra_env=None, check=True):
    """Run a command; in --plan mode just print it."""
    if DRY:
        print(f"  $ {cmd}")
        return 0
    env = {**os.environ, **(extra_env or {})}
    log(f"$ {cmd}")
    return subprocess.run(cmd, shell=True, env=env, check=check).returncode


@contextlib.contextmanager
def port_forward(svc, local, remote):
    """kubectl port-forward for the duration of a phase (no-op in --plan)."""
    if DRY:
        print(f"  $ kubectl -n {NS} port-forward svc/{svc} {local}:{remote}  (background)")
        yield
        return
    p = subprocess.Popen(shlex.split(f"kubectl -n {NS} port-forward svc/{svc} {local}:{remote}"),
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(3)  # let the tunnel establish
        yield
    finally:
        p.terminate()


def wait_source_rows(target_rows, timeout_s=6 * 3600):
    """Block until the Iceberg source has >= target_rows (generator filling it), then stop generating.
    Uses the growlerdb_source_records Prometheus gauge scraped from the connector/source."""
    if DRY:
        print(f"  # wait until source rows >= {target_rows:,} (Prometheus growlerdb_source_records), "
              f"then scale the generator to 0")
        return
    deadline = time.time() + timeout_s
    q = "http://%s/api/v1/query?query=sum(growlerdb_source_records)" % PROM_URL.split("//")[-1]
    while time.time() < deadline:
        import json
        import urllib.request
        try:
            with urllib.request.urlopen(q, timeout=15) as r:
                res = json.load(r)["data"]["result"]
                rows = float(res[0]["value"][1]) if res else 0
        except Exception:  # noqa: BLE001
            rows = 0
        log(f"source rows ~{int(rows):,} / {target_rows:,}")
        if rows >= target_rows:
            break
        time.sleep(30)
    sh(f"kubectl -n {NS} scale deploy/growlerdb-generator --replicas=0", check=False)


def phase_generate(target_rows):
    log(f"PHASE generate — fill Iceberg source to ~{target_rows:,} rows (SPAN_DAYS=7)")
    # scale-up.sh started the generator; ensure the span is 7d and wait for the target, then stop.
    # (SPAN_DAYS is baked into the generator env by scale-up.sh; set it there before bring-up.)
    wait_source_rows(target_rows)


def phase_growlerdb(scale):
    log("PHASE growlerdb — converge, compact, query matrix, Trino, freshness, capture")
    sh(f"python {HERE}/convergence_check.py")  # index docs == source DISTINCT id
    # compaction: the maintenance CronJob targets growlerdb.http_logs; trigger it now for a fair Trino.
    sh(f"kubectl -n {NS} create job --from=cronjob/growlerdb-maintenance compact-{scale} || true", check=False)
    with port_forward("gdb-growlerdb-gateway", 8081, 8081), port_forward("prometheus", 9090, 9090):
        sh(f"python {HERE}/compare_query.py run http_logs --engines growlerdb "
           f"--qps 200 --duration 120 --sweep 50,100,200,400,800 --sweep-duration 30 "
           f"--out {HERE}/gdb-query.json", {"GROWLERDB_OS_URL": GATEWAY_URL})
        sh(f"python {HERE}/compare_trino.py", check=False)
        sh(f"python {HERE}/compare_freshness.py run http_logs --engines growlerdb --iterations 20 "
           f"--out {HERE}/gdb-freshness.json", {"GROWLERDB_OS_URL": GATEWAY_URL})
    sh(f"python {HERE}/capture.py --purpose 'comparison {scale} — GrowlerDB phase' "
       f"--comparison {HERE}/gdb-query.json --freshness {HERE}/gdb-freshness.json", check=False)


def phase_transition():
    log("PHASE transition — scale GrowlerDB serving down, bring up OpenSearch + Data Prepper")
    sh(f"kubectl -n {NS} scale statefulset --all --replicas=0 -l app.kubernetes.io/component=node", check=False)
    sh(f"NAMESPACE={NS} {REPO}/deploy/k8s/comparison/up.sh")


def phase_opensearch(scale):
    log("PHASE opensearch — wait for CDC initial load convergence, query matrix, freshness, capture")
    with port_forward("opensearch", 9200, 9200):
        # convergence: OpenSearch doc count == source DISTINCT id (Data Prepper initial load complete)
        sh(f"python {HERE}/convergence_check.py --engine opensearch --opensearch-url {OPENSEARCH_URL}", check=False)
        sh(f"python {HERE}/compare_query.py run http_logs --engines opensearch "
           f"--qps 200 --duration 120 --sweep 50,100,200,400,800 --sweep-duration 30 "
           f"--out {HERE}/os-query.json", {"OPENSEARCH_URL": OPENSEARCH_URL})
        sh(f"python {HERE}/compare_freshness.py run http_logs --engines opensearch --iterations 20 "
           f"--out {HERE}/os-freshness.json", {"OPENSEARCH_URL": OPENSEARCH_URL})
    sh(f"python {HERE}/capture.py --purpose 'comparison {scale} — OpenSearch phase' "
       f"--comparison {HERE}/os-query.json --freshness {HERE}/os-freshness.json", check=False)


def phase_finalize():
    log("PHASE finalize — push corpus + result artifacts to the bucket (if HETZNER_S3_* is set)")
    if os.environ.get("HETZNER_S3_BUCKET"):
        sh(f"bash {HERE}/artifacts.sh push", check=False)
    else:
        log("HETZNER_S3_* not set — skipping artifact push (create the bucket + set creds to enable)")


PHASES = {"generate": phase_generate, "growlerdb": phase_growlerdb, "transition": phase_transition,
          "opensearch": phase_opensearch, "finalize": phase_finalize}


def main():
    global DRY
    ap = argparse.ArgumentParser(description="Orchestrate the GrowlerDB-vs-OpenSearch comparison run")
    ap.add_argument("--scale", choices=SCALES, default="shakedown")
    ap.add_argument("--phase", choices=["all", *PHASES], default="all", help="run one phase (resume)")
    ap.add_argument("--plan", action="store_true", help="print the ordered steps/commands, run nothing")
    args = ap.parse_args()
    DRY = args.plan
    target_rows = int(SCALES[args.scale] * 1e9 / ROW_BYTES)

    log(f"scale={args.scale} (~{SCALES[args.scale]} GB, ~{target_rows:,} rows), phase={args.phase}, "
        f"plan={args.plan}, ns={NS}")
    order = list(PHASES) if args.phase == "all" else [args.phase]
    for name in order:
        fn = PHASES[name]
        fn(target_rows) if name == "generate" else (fn(args.scale) if name in ("growlerdb", "opensearch") else fn())
    log("done" if not DRY else "plan complete (nothing executed)")


if __name__ == "__main__":
    main()
