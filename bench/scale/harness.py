#!/usr/bin/env python3
"""Dataset-agnostic scale-test harness (task-159).

A *workload* is a self-contained directory under `workloads/<name>/` defined by three things — the
OpenSearch-Benchmark "workload" contract (see okf/quality/scale-test-plan.md):

  1. index.yaml    — the GrowlerDB index definition (schema / field mapping)
  2. corpus        — a loader (download a public corpus, or generate) that writes rows to Iceberg
  3. queries.json  — the query mix (OpenSearch `_search` DSL bodies + weights)

The driver is dataset-agnostic: adding Wikipedia / MS MARCO / a synthetic corpus is a new workload
directory, not a change here. GrowlerDB is queried through its OpenSearch-compatible `_search`
adapter (`gateway --opensearch`); ingest is via Iceberg (the connector indexes the table), not the
OpenSearch `_bulk` API.

Commands:
  list                       list available workloads
  validate <workload>        parse + sanity-check a workload (no cluster needed)
  load <workload>            provision the corpus into Iceberg + create the index
  query <workload>           run the query mix, report p50/p95/p99 + throughput
  run <workload>             load then query

Endpoints come from the environment (see README): GROWLERDB_OS_URL (gateway `_search`), plus the
POLARIS_*/AWS_* catalog+S3 vars used by the corpus loaders.
"""

import argparse
import importlib.util
import json
import os
import statistics
import sys
import threading
import time
import urllib.request
from pathlib import Path

WORKLOADS_DIR = Path(__file__).parent / "workloads"
OS_URL = os.environ.get("GROWLERDB_OS_URL", "http://localhost:8081")


# --- workload loading ---------------------------------------------------------------------------

def _load_yaml(path):
    import yaml  # deferred: only needed when acting on a workload

    with open(path) as f:
        return yaml.safe_load(f)


class Workload:
    def __init__(self, name):
        self.name = name
        self.dir = WORKLOADS_DIR / name
        if not self.dir.is_dir():
            raise SystemExit(f"unknown workload '{name}' (see `harness.py list`)")
        self.meta = _load_yaml(self.dir / "workload.yaml")
        self.index = _load_yaml(self.dir / (self.meta.get("index") or "index.yaml"))
        with open(self.dir / (self.meta.get("queries") or "queries.json")) as f:
            self.queries = json.load(f)

    @property
    def index_name(self):
        return self.index["name"]

    def validate(self):
        problems = []
        if self.index.get("name") != self.meta.get("name"):
            problems.append(
                f"workload.yaml name '{self.meta.get('name')}' != index.yaml name '{self.index.get('name')}'"
            )
        if not self.index.get("mapping", {}).get("fields"):
            problems.append("index.yaml has no mapping.fields")
        if not isinstance(self.queries, list) or not self.queries:
            problems.append("queries.json must be a non-empty list")
        for i, q in enumerate(self.queries):
            if "name" not in q or "body" not in q:
                problems.append(f"query #{i} missing 'name' or 'body'")
        corpus = self.meta.get("corpus", {})
        if corpus.get("type") not in ("download", "generate"):
            problems.append("corpus.type must be 'download' or 'generate'")
        return problems

    def corpus_module(self):
        path = self.dir / (self.meta.get("corpus", {}).get("module") or "corpus.py")
        if not path.exists():
            return None
        spec = importlib.util.spec_from_file_location(f"corpus_{self.name}", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod


# --- commands -----------------------------------------------------------------------------------

def cmd_list(_):
    for d in sorted(p.name for p in WORKLOADS_DIR.iterdir() if (p / "workload.yaml").exists()):
        meta = _load_yaml(WORKLOADS_DIR / d / "workload.yaml")
        print(f"{d:14} {meta.get('description', '')}")


def cmd_validate(args):
    wl = Workload(args.workload)
    problems = wl.validate()
    print(f"workload: {wl.name}  index: {wl.index_name}  queries: {len(wl.queries)}  "
          f"corpus: {wl.meta.get('corpus', {}).get('type')}")
    if problems:
        print("INVALID:")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print("valid")


def cmd_load(args):
    wl = Workload(args.workload)
    mod = wl.corpus_module()
    if mod is None or not hasattr(mod, "load"):
        raise SystemExit(
            f"workload '{wl.name}' has no corpus.py with a load(); provision the corpus per its README"
        )
    table = wl.meta.get("corpus", {}).get("table", f"growlerdb.{wl.name}")
    print(f"loading corpus for '{wl.name}' -> {table} (fraction={args.fraction}) ...")
    n = mod.load(table=table, fraction=args.fraction)
    print(f"loaded ~{n} rows. Now create + build the index:")
    print(f"  growlerdb index create -f {wl.dir / 'index.yaml'}   # then run the connector to index {table}")


def _percentiles(xs):
    xs = sorted(xs)
    def pct(p):
        if not xs:
            return 0.0
        k = min(len(xs) - 1, int(round(p / 100.0 * (len(xs) - 1))))
        return xs[k]
    return {"p50": pct(50), "p95": pct(95), "p99": pct(99),
            "min": xs[0] if xs else 0.0, "max": xs[-1] if xs else 0.0}


def _search(index, body):
    req = urllib.request.Request(
        f"{OS_URL}/{index}/_search",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"content-type": "application/json"},
    )
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=120) as resp:
        payload = json.loads(resp.read().decode())
    latency_ms = (time.perf_counter() - t) * 1000.0
    return latency_ms, payload


def cmd_query(args):
    wl = Workload(args.workload)
    index = wl.index_name
    # Weighted round of queries.
    plan = []
    for q in wl.queries:
        plan.extend([q] * int(q.get("weight", 1)))

    results = {q["name"]: [] for q in wl.queries}
    lock = threading.Lock()
    stop_at = time.perf_counter() + args.duration
    counter = {"n": 0}

    def worker():
        i = 0
        while time.perf_counter() < stop_at:
            q = plan[i % len(plan)]
            i += 1
            try:
                latency_ms, _ = _search(index, q["body"])
            except Exception as e:  # noqa: BLE001 — record failures, keep the load going
                latency_ms = -1.0
                with lock:
                    print(f"  ! {q['name']}: {e}", file=sys.stderr)
            with lock:
                results[q["name"]].append(latency_ms)
                counter["n"] += 1

    print(f"querying '{index}' via {OS_URL} — {args.concurrency} workers x {args.duration}s ...")
    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker) for _ in range(args.concurrency)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    elapsed = time.perf_counter() - t0

    per_query = {}
    for name, lats in results.items():
        ok = [x for x in lats if x >= 0]
        per_query[name] = {"count": len(lats), "errors": len(lats) - len(ok), **_percentiles(ok)}
    report = {
        "workload": wl.name,
        "index": index,
        "endpoint": OS_URL,
        "concurrency": args.concurrency,
        "duration_s": round(elapsed, 1),
        "total_queries": counter["n"],
        "throughput_qps": round(counter["n"] / elapsed, 1) if elapsed else 0.0,
        "per_query": per_query,
    }
    Path(args.out).write_text(json.dumps(report, indent=2))
    _print_report(report)
    print(f"\nwrote {args.out}")


def _print_report(r):
    print(f"\n== {r['workload']} @ {r['throughput_qps']} qps ({r['total_queries']} queries / {r['duration_s']}s) ==")
    print(f"{'query':32} {'n':>7} {'err':>4} {'p50':>8} {'p95':>8} {'p99':>8}  (ms)")
    for name, s in r["per_query"].items():
        print(f"{name:32} {s['count']:>7} {s['errors']:>4} {s['p50']:>8.1f} {s['p95']:>8.1f} {s['p99']:>8.1f}")


def cmd_run(args):
    cmd_load(args)
    cmd_query(args)


# --- k8s deploy rendering (task-214) --------------------------------------------------------------

STREAMING_DIR = Path(__file__).resolve().parents[2] / "deploy" / "k8s" / "streaming"


def cmd_render(args):
    """Render the k8s streaming manifests for a workload — the generator (its corpus.py mounted,
    `stream()` driven) and the connector (--table/--identifier/--fields/--index derived from
    index.yaml, --nodes sized to --shards). One workload definition drives the whole deploy:
    deploy/k8s/scale-up.sh WORKLOAD=<name> consumes this."""
    import string

    import yaml

    wl = Workload(args.workload)
    key = wl.index.get("key", {})
    identifier = key.get("identifier_fields", [])
    if not identifier:
        raise SystemExit(f"workload '{wl.name}': index.yaml key.identifier_fields is required to stream")
    if key.get("partition_fields"):
        raise SystemExit(
            f"workload '{wl.name}': key.partition_fields streaming isn't wired yet — "
            "the connector's partition routing needs a --partition arg in the template"
        )
    corpus_path = wl.dir / (wl.meta.get("corpus", {}).get("module") or "corpus.py")
    mod = wl.corpus_module()
    if mod is None or not hasattr(mod, "stream"):
        raise SystemExit(
            f"workload '{wl.name}': {corpus_path.name} has no stream(table, batch, sleep_s) — "
            "the streaming generator runs the workload's own corpus module"
        )
    table = wl.meta.get("corpus", {}).get("table", f"growlerdb.{wl.name}")
    # The connector carries the KEY + every indexed column; source-only columns hydrate on demand.
    fields, seen = [], set()
    for f in identifier + [m["path"] for m in wl.index["mapping"]["fields"]]:
        if f not in seen:
            seen.add(f)
            fields.append(f)
    nodes = ",".join(
        f"gdb-growlerdb-node-{i}.gdb-growlerdb-node-headless.{args.namespace}.svc.cluster.local:50051"
        for i in range(args.shards)
    )
    subs = {
        "NAMESPACE": args.namespace,
        "TABLE": table,
        "INDEX": wl.index_name,
        "IDENTIFIER": ",".join(identifier),
        "FIELDS": ",".join(fields),
        "NODES": nodes,
        "GENERATORS": args.generators,
        # ConfigMap block scalar: the corpus source, indented under `corpus.py: |`.
        "CORPUS_PY": "".join(f"    {line}".rstrip() + "\n" for line in corpus_path.read_text().splitlines()).rstrip("\n"),
    }
    out_dir = Path(args.out) if args.out else (Path(__file__).parent / ".render" / wl.name)
    out_dir.mkdir(parents=True, exist_ok=True)
    for template in ("generator", "connector"):
        text = string.Template((STREAMING_DIR / f"{template}.template.yaml").read_text()).safe_substitute(subs)
        if "${" in text:
            raise SystemExit(f"{template}.template.yaml: unresolved placeholder after render")
        list(yaml.safe_load_all(text))  # loud parse check before anything is applied
        (out_dir / f"{template}.yaml").write_text(text)
    # Shell-consumable facts for scale-up.sh (table gate, helm def/name flags, verify index).
    (out_dir / "workload.env").write_text(
        f"TABLE={table}\nINDEX={wl.index_name}\nINDEX_DEF={wl.dir / 'index.yaml'}\n"
    )
    print(f"rendered {out_dir}/{{generator.yaml,connector.yaml,workload.env}} "
          f"(table={table} index={wl.index_name} shards={args.shards})")


def main():
    ap = argparse.ArgumentParser(description="GrowlerDB scale-test harness (pluggable workloads)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    p = sub.add_parser("validate"); p.add_argument("workload"); p.set_defaults(fn=cmd_validate)
    p = sub.add_parser("load"); p.add_argument("workload")
    p.add_argument("--fraction", type=float, default=1.0, help="fraction of the corpus to ingest (0-1)")
    p.set_defaults(fn=cmd_load)
    for name in ("query", "run"):
        p = sub.add_parser(name); p.add_argument("workload")
        p.add_argument("--duration", type=int, default=60, help="query phase seconds")
        p.add_argument("--concurrency", type=int, default=8)
        p.add_argument("--fraction", type=float, default=1.0)
        p.add_argument("--out", default="scale-report.json")
        p.set_defaults(fn=cmd_run if name == "run" else cmd_query)
    p = sub.add_parser("render", help="render the k8s streaming manifests for a workload (task-214)")
    p.add_argument("workload")
    p.add_argument("--shards", type=int, default=6, help="shard count (--nodes list size)")
    p.add_argument("--namespace", default="growlerdb")
    p.add_argument("--generators", type=int, default=1,
                   help="generator pod replicas — parallelize ingest (task-231; disjoint ids per pod)")
    p.add_argument("--out", default=None, help="output dir (default bench/scale/.render/<workload>/)")
    p.set_defaults(fn=cmd_render)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
