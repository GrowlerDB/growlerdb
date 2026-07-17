#!/usr/bin/env python
"""Automated pre-teardown capture for a scale / cluster-validation run.

Prometheus + Loki are in-cluster and ephemeral, so anything not captured before `terraform destroy`
dies with the cluster. This collects, into one timestamped run directory:

  - metric time-series (Prometheus `query_range` over the run window) — the graph set the write-up
    needs, as JSON (more useful than static images, and diff-able);
  - pod log streams (Loki) for the connector / hot node / gateway;
  - the harness results.json + the recorded run cost;
  - optional, bounded dashboard screenshots (Grafana render API) — opt-in, capped, never committed;
  - an `audit.json`: purpose, timestamps, duration, and the run parameters.

**What's durable vs heavy.** The heavy artifacts live under `bench/scale/runs/<run>/`, which is
**gitignored** — the durable, reviewable record committed to git is the bounded `RUNLOG.md` ledger
(one compact row per run) plus each run's `audit.json` (which travels with the artifacts / any
upload). Screenshots are the one thing that can grow large without much value, so they are opt-in
(`--screenshots`), bounded (count + a total-size budget), and always out of git.

Stdlib only (urllib), matching staged_run.py. Config via env (PROM_URL / LOKI_URL / GRAFANA_URL,
NAMESPACE, WORKLOAD, INDEX, IMAGE_TAG, ...) plus flags. `--dry-run` skips all network/cluster access
(writes the audit + ledger from parameters alone); `--selftest` exercises the whole file/ledger path
in a temp dir and exits non-zero on any mismatch — run it in smoke.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_RUNS_ROOT = os.path.join(HERE, "runs")
LEDGER = os.path.join(HERE, "RUNLOG.md")
LEDGER_HEADER = (
    "# Scale / cluster-validation run log\n\n"
    "Append-only ledger of validation runs — the durable, git-committed record. One compact row per\n"
    "run; the heavy artifacts (metric/log dumps, screenshots) live under the gitignored\n"
    "`bench/scale/runs/<run>/` and are captured by `capture.py`.\n\n"
    "| Started (UTC) | Duration | Purpose | Parameters | Result summary | Artifact dir |\n"
    "| --- | --- | --- | --- | --- | --- |\n"
)

# Metric series dumped over the run window (name -> PromQL). Mirrors the scale-test-plan capture list:
# doc growth, query/hydration latency, ingest/index throughput, the write-path trio, lag, index bytes,
# the locator-heal signals, cold-tier cache, and node CPU. Missing metrics just dump empty — the
# capture is best-effort and never fails on one absent series.
RANGE_METRICS = {
    "source_records": "max(growlerdb_source_records)",
    "index_docs": "sum(growlerdb_index_docs)",
    "query_latency_p50": "histogram_quantile(0.50,sum(rate(growlerdb_query_duration_seconds_bucket[2m]))by(le))",
    "query_latency_p95": "histogram_quantile(0.95,sum(rate(growlerdb_query_duration_seconds_bucket[2m]))by(le))",
    "query_latency_p99": "histogram_quantile(0.99,sum(rate(growlerdb_query_duration_seconds_bucket[2m]))by(le))",
    "hydration_latency_p95": "histogram_quantile(0.95,sum(rate(growlerdb_hydration_duration_seconds_bucket[2m]))by(le))",
    "ingest_rate_rps": "deriv(growlerdb_source_records[2m])",
    "index_rate_dps": "sum(rate(growlerdb_ingested_docs_total[2m]))",
    "write_latency_p95": "histogram_quantile(0.95,sum(rate(growlerdb_write_duration_seconds_bucket[2m]))by(le))",
    "write_queue_depth": "max(growlerdb_write_queue_depth)",
    "ingest_lag_ms": "max(growlerdb_ingest_lag_ms)",
    "index_bytes": "sum(growlerdb_index_bytes)",
    "stale_locators": "max(growlerdb_stale_locators_total)",
    "locator_remapped_rows": "max(growlerdb_locator_remapped_rows_total)",
    "cold_cache_hit_ratio": "sum(rate(growlerdb_cold_cache_hits_total[5m]))/clamp_min(sum(rate(growlerdb_cold_cache_lookups_total[5m])),1)",
    "node_cpu_cores": 'sum(rate(node_cpu_seconds_total{mode!="idle"}[2m]))',
}

# Loki log streams to dump (filename -> LogQL selector).
LOG_STREAMS = {
    "connector": '{app="growlerdb-connector"}',
    "node": '{app="growlerdb-node"}',
    "gateway": '{app="growlerdb-gateway"}',
}


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def human_duration(secs):
    secs = int(secs)
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def slug(text, maxlen=48):
    out = "".join(c.lower() if c.isalnum() else "-" for c in text)
    while "--" in out:
        out = out.replace("--", "-")
    return out.strip("-")[:maxlen] or "run"


def dir_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def sh(*args):
    """Best-effort shell capture — returns stripped stdout, or "" on any failure."""
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=15)
        return r.stdout.strip() if r.returncode == 0 else ""
    except Exception:
        return ""


def git_info():
    return {
        "commit": sh("git", "-C", HERE, "rev-parse", "--short", "HEAD"),
        "dirty": bool(sh("git", "-C", HERE, "status", "--porcelain")),
    }


def collect_params(extra, dry):
    """Run parameters for the audit: env-derived, plus best-effort cluster state (skipped in dry
    mode), plus any `--params k=v` overrides (which win)."""
    ns = os.environ.get("NAMESPACE", "growlerdb")
    p = {
        "workload": os.environ.get("WORKLOAD", ""),
        "index": os.environ.get("INDEX", ""),
        "namespace": ns,
        "image_tag": os.environ.get("IMAGE_TAG", ""),
        "shards": os.environ.get("SHARDS", os.environ.get("shard_count", "")),
        "generators": os.environ.get("GENERATORS", ""),
    }
    if not dry:
        nodes = sh("kubectl", "get", "nodes", "-o", "name")
        if nodes:
            p["cluster_nodes"] = len(nodes.splitlines())
        img = sh(
            "kubectl", "-n", ns, "get", "statefulset", "-l",
            "app.kubernetes.io/component=node", "-o",
            "jsonpath={.items[0].spec.template.spec.containers[0].image}",
        )
        if img:
            p["node_image"] = img
        # Cold-tier config actually deployed on the node (ties the audit to reality, not intent).
        for var in ("GROWLERDB_PARK_INTERVAL_SECS", "GROWLERDB_BACKUP_BUCKET"):
            val = sh(
                "kubectl", "-n", ns, "get", "statefulset", "-l",
                "app.kubernetes.io/component=node", "-o",
                "jsonpath={.items[0].spec.template.spec.containers[0].env[?(@.name=='" + var + "')].value}",
            )
            if val:
                p[var.lower()] = val
    p.update(extra)
    return {k: v for k, v in p.items() if v != ""}


def prom_range(prom_url, expr, start, end, step):
    q = urllib.parse.urlencode(
        {"query": expr, "start": f"{start:.3f}", "end": f"{end:.3f}", "step": step}
    )
    with urllib.request.urlopen(f"{prom_url}/api/v1/query_range?{q}", timeout=30) as r:
        return json.load(r)


def last_value(prom_json):
    """The final scalar of a single-series Prometheus range result, or None."""
    try:
        return float(prom_json["data"]["result"][0]["values"][-1][1])
    except (KeyError, IndexError, ValueError, TypeError):
        return None


def capture_metrics(prom_url, start, end, step, out_dir):
    """Dump each metric's time-series to metrics/<name>.json. Returns {name: status} and a few
    headline last-values for the ledger summary."""
    mdir = os.path.join(out_dir, "metrics")
    os.makedirs(mdir, exist_ok=True)
    status, headline = {}, {}
    for name, expr in RANGE_METRICS.items():
        try:
            data = prom_range(prom_url, expr, start, end, step)
            with open(os.path.join(mdir, f"{name}.json"), "w") as f:
                json.dump(data, f)
            n = len(data.get("data", {}).get("result", []))
            status[name] = "ok" if n else "empty"
            if name in ("index_docs", "query_latency_p95", "cold_cache_hit_ratio"):
                lv = last_value(data)
                if lv is not None:
                    headline[name] = lv
        except Exception as e:
            status[name] = f"error: {type(e).__name__}"
    return status, headline


def capture_logs(loki_url, start_ns, end_ns, out_dir, limit):
    ldir = os.path.join(out_dir, "logs")
    os.makedirs(ldir, exist_ok=True)
    status = {}
    for name, selector in LOG_STREAMS.items():
        try:
            q = urllib.parse.urlencode(
                {"query": selector, "start": start_ns, "end": end_ns,
                 "limit": limit, "direction": "forward"}
            )
            with urllib.request.urlopen(f"{loki_url}/loki/api/v1/query_range?{q}", timeout=30) as r:
                data = json.load(r)
            lines = []
            for stream in data.get("data", {}).get("result", []):
                for _ts, line in stream.get("values", []):
                    lines.append(line)
            with open(os.path.join(ldir, f"{name}.log"), "w") as f:
                f.write("\n".join(lines))
            status[name] = f"{len(lines)} lines"
        except Exception as e:
            status[name] = f"error: {type(e).__name__}"
    return status


def capture_screenshots(grafana_url, out_dir, max_shots, from_ms, to_ms):
    """Opt-in, bounded dashboard images via Grafana's render API — always under the gitignored run dir,
    never committed. Best-effort: a missing image-renderer / auth just yields fewer shots + a note."""
    render_urls = [u for u in os.environ.get("GRAFANA_RENDER_URLS", "").split(",") if u.strip()]
    if not render_urls:
        return {"skipped": "set GRAFANA_RENDER_URLS (comma-separated Grafana /render/... panel URLs)"}
    sdir = os.path.join(out_dir, "screenshots")
    os.makedirs(sdir, exist_ok=True)
    token = os.environ.get("GRAFANA_TOKEN", "")
    got = 0
    for i, url in enumerate(render_urls[:max_shots]):
        full = url if url.startswith("http") else f"{grafana_url}{url}"
        sep = "&" if "?" in full else "?"
        full = f"{full}{sep}from={from_ms}&to={to_ms}"
        req = urllib.request.Request(full)
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                with open(os.path.join(sdir, f"panel-{i:02d}.png"), "wb") as f:
                    f.write(r.read())
            got += 1
        except Exception:
            pass
    return {"rendered": got, "requested": min(len(render_urls), max_shots),
            "capped_at": max_shots}


def write_manifest(out_dir):
    lines, total = [], 0
    for root, _dirs, files in os.walk(out_dir):
        for f in sorted(files):
            if f == "MANIFEST.txt":
                continue
            fp = os.path.join(root, f)
            sz = os.path.getsize(fp)
            total += sz
            lines.append(f"{sz:>12}  {os.path.relpath(fp, out_dir)}")
    lines.append(f"{total:>12}  TOTAL")
    with open(os.path.join(out_dir, "MANIFEST.txt"), "w") as f:
        f.write("\n".join(lines) + "\n")
    return total


def ledger_row(audit):
    p = audit["parameters"]
    param_str = " ".join(f"{k}={v}" for k, v in p.items()) or "—"
    hl = audit.get("headline", {})
    summary_bits = []
    if "index_docs" in hl:
        summary_bits.append(f"docs={hl['index_docs']:.0f}")
    if "query_latency_p95" in hl:
        summary_bits.append(f"p95={hl['query_latency_p95']*1000:.0f}ms")
    if "cold_cache_hit_ratio" in hl:
        summary_bits.append(f"cold_hit={hl['cold_cache_hit_ratio']:.2f}")
    if audit.get("run_cost"):
        summary_bits.append(f"cost={audit['run_cost']}")
    summary = ", ".join(summary_bits) or "—"

    def cell(s):  # keep the markdown table intact
        return str(s).replace("|", "\\|").replace("\n", " ")

    return (
        f"| {audit['started_at']} | {audit['duration_human']} | {cell(audit['purpose'])} "
        f"| {cell(param_str)} | {cell(summary)} | `{audit['artifact_dir']}` |\n"
    )


def append_ledger(ledger_path, row):
    if not os.path.exists(ledger_path):
        with open(ledger_path, "w") as f:
            f.write(LEDGER_HEADER)
    with open(ledger_path, "a") as f:
        f.write(row)


def build_audit(args, started, ended, params, captured, headline, artifact_rel):
    return {
        "purpose": args.purpose,
        "started_at": iso(started),
        "ended_at": iso(ended),
        "duration_s": int(ended - started),
        "duration_human": human_duration(ended - started),
        "git": git_info(),
        "parameters": params,
        "endpoints": {"prometheus": args.prom, "loki": args.loki or None,
                      "grafana": args.grafana or None},
        "captured": captured,
        "headline": headline,
        "run_cost": args.cost or None,
        "artifact_dir": artifact_rel,
    }


def run(args):
    now = time.time()
    started = args.started_epoch if args.started_epoch else now - args.window_min * 60
    ended = now
    window_min = max(1, int((ended - started) / 60))

    stamp = datetime.fromtimestamp(ended, timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    run_name = f"{stamp}__{slug(args.purpose)}"
    out_dir = os.path.join(args.out_root, run_name)
    os.makedirs(out_dir, exist_ok=True)
    # Reference the artifacts relative to bench/scale (clean `runs/<name>` for the default root);
    # if a caller points --out-root elsewhere, fall back to the bare run name rather than a `../..` chain.
    artifact_rel = os.path.relpath(out_dir, HERE)
    if artifact_rel.startswith(".."):
        artifact_rel = run_name

    params = collect_params(dict(args.param or []), args.dry_run)
    params.setdefault("window_min", window_min)

    captured, headline = {}, {}
    if args.dry_run:
        captured["mode"] = "dry-run (no metrics/logs/screenshots captured)"
    else:
        m_status, headline = capture_metrics(args.prom, started, ended, args.step, out_dir)
        captured["metrics"] = m_status
        if args.loki:
            captured["logs"] = capture_logs(
                args.loki, int(started * 1e9), int(ended * 1e9), out_dir, args.log_limit
            )
        else:
            captured["logs"] = {"skipped": "set LOKI_URL to dump pod logs"}
        if args.results and os.path.exists(args.results):
            dest = os.path.join(out_dir, "results.json")
            with open(args.results) as src, open(dest, "w") as dst:
                dst.write(src.read())
            captured["results_json"] = True
        if args.screenshots:
            captured["screenshots"] = capture_screenshots(
                args.grafana, out_dir, args.max_screenshots,
                int(started * 1000), int(ended * 1000),
            )

    audit = build_audit(args, started, ended, params, captured, headline, artifact_rel)
    with open(os.path.join(out_dir, "audit.json"), "w") as f:
        json.dump(audit, f, indent=2)

    total = write_manifest(out_dir)
    audit["total_bytes"] = total
    budget = args.max_run_mb * 1024 * 1024
    over_budget = total > budget
    if over_budget:
        print(
            f"WARNING: run artifacts are {total/1e6:.1f} MB, over the {args.max_run_mb} MB budget "
            f"(screenshots={'on' if args.screenshots else 'off'}). Consider --screenshots off or a "
            f"tighter --max-screenshots; heavy dumps are gitignored but still cost disk/upload.",
            file=sys.stderr,
        )

    append_ledger(args.ledger, ledger_row(audit))

    print(f"captured → {out_dir}  ({total/1e6:.1f} MB)")
    print(f"ledger   → {os.path.relpath(args.ledger, HERE)} (+1 row)")
    return 0 if not over_budget or args.allow_over_budget else 2


def selftest():
    """Exercise the whole capture/ledger/audit path (no cluster) and assert the outputs."""
    import tempfile

    fails = []

    def check(cond, msg):
        if not cond:
            fails.append(msg)

    check(slug("Cold-tier validation!! (run 3)") == "cold-tier-validation-run-3", "slug")
    check(human_duration(3785) == "1h03m", f"human_duration hour: {human_duration(3785)}")
    check(human_duration(125) == "2m05s", f"human_duration min: {human_duration(125)}")
    check(human_duration(9) == "9s", "human_duration sec")

    with tempfile.TemporaryDirectory() as tmp:
        ledger = os.path.join(tmp, "RUNLOG.md")
        args = argparse.Namespace(
            purpose="cold-tier validation | on-cluster", prom="http://x", loki="", grafana="",
            step="15s", window_min=63, started_epoch=None, param=[("shards", "6"), ("nodes", "6")],
            results=None, screenshots=False, max_screenshots=12, max_run_mb=200,
            log_limit=5000, cost="$4.20", dry_run=True, out_root=os.path.join(tmp, "runs"),
            ledger=ledger, allow_over_budget=False,
        )
        rc = run(args)
        check(rc == 0, f"run rc {rc}")
        runs = os.listdir(os.path.join(tmp, "runs"))
        check(len(runs) == 1, f"one run dir, got {runs}")
        audit = json.load(open(os.path.join(tmp, "runs", runs[0], "audit.json")))
        check(audit["purpose"] == "cold-tier validation | on-cluster", "audit purpose")
        check(audit["duration_human"] == "1h03m", f"audit duration {audit['duration_human']}")
        check(audit["parameters"].get("shards") == "6", "audit param override")
        check(audit["run_cost"] == "$4.20", "audit cost")
        check(os.path.exists(os.path.join(tmp, "runs", runs[0], "MANIFEST.txt")), "manifest")
        led = open(ledger).read()
        check("| Started (UTC) |" in led and "| --- |" in led, "ledger header present")
        # The pipe in the purpose must be escaped so it can't break the markdown table.
        check("cold-tier validation \\| on-cluster" in led, "ledger escapes pipes")
        check("shards=6 nodes=6" in led, "ledger params")
        check("cost=$4.20" in led, "ledger cost")
        # A second run appends exactly one more row (append-only, header written once).
        run(args)
        led2 = open(ledger).read()
        check(led2.count("| cold-tier validation ") == 2, "second run appends one row")
        check(led2.count("| Started (UTC) |") == 1, "header written once")

    if fails:
        print("SELFTEST FAILED:")
        for m in fails:
            print("  -", m)
        return 1
    print("capture.py selftest: OK")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="Capture a scale/cluster-validation run's artifacts.")
    ap.add_argument("--purpose", help="what this run was for (goes in the audit + ledger)")
    ap.add_argument("--window-min", type=int, default=60,
                    help="capture the last N minutes of metrics/logs (default 60)")
    ap.add_argument("--started-epoch", type=float, default=None,
                    help="explicit run start (unix seconds); overrides --window-min for the window")
    ap.add_argument("--param", action="append", type=lambda kv: tuple(kv.split("=", 1)),
                    metavar="K=V", help="extra audit parameter (repeatable)")
    ap.add_argument("--results", default=os.environ.get("RESULTS", ""),
                    help="harness results.json to fold into the run dir")
    ap.add_argument("--cost", default=os.environ.get("RUN_COST", ""), help="recorded run cost")
    ap.add_argument("--screenshots", action="store_true",
                    help="also render bounded Grafana dashboard images (opt-in; out of git)")
    ap.add_argument("--max-screenshots", type=int, default=12)
    ap.add_argument("--max-run-mb", type=int, default=200, help="warn if the run dir exceeds this")
    ap.add_argument("--allow-over-budget", action="store_true",
                    help="exit 0 even if over --max-run-mb (default exits 2)")
    ap.add_argument("--log-limit", type=int, default=5000, help="max Loki lines per stream")
    ap.add_argument("--step", default="15s", help="Prometheus query_range step")
    ap.add_argument("--out-root", default=DEFAULT_RUNS_ROOT)
    ap.add_argument("--ledger", default=LEDGER)
    ap.add_argument("--prom", default=os.environ.get("PROM_URL", "http://localhost:9090"))
    ap.add_argument("--loki", default=os.environ.get("LOKI_URL", ""))
    ap.add_argument("--grafana", default=os.environ.get("GRAFANA_URL", ""))
    ap.add_argument("--dry-run", action="store_true",
                    help="skip all network/cluster access; write audit + ledger from params only")
    ap.add_argument("--selftest", action="store_true", help="self-check the tool and exit")
    args = ap.parse_args(argv)

    if args.selftest:
        return selftest()
    if not args.purpose:
        ap.error("--purpose is required (unless --selftest)")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
