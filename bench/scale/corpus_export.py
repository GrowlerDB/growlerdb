#!/usr/bin/env python3
"""Export the synthetic http_logs corpus as gzipped NDJSON shards — the portable "raw logs" artifact
archived to object storage alongside the Iceberg tables.

Reuses the workload's `corpus.py` recipe, so the NDJSON is the same corpus the Iceberg load/stream
produces. One shard per seed (matching the k8s parallel-generator sharding: each generator pod owns a
distinct BENCH_SEED / disjoint data). Deterministic: same seeds -> byte-identical shards.

Usage: python corpus_export.py --rows-per-shard N --seeds 42,43,44 --out-dir ./ndjson [--span-days 7]
"""

import argparse
import gzip
import json
import os
import random
from pathlib import Path

from harness import Workload


def _write_shard(mod, path, rows, seed, batch=50_000):
    rng = random.Random(seed)
    model = mod._build_model(rng)
    written = 0
    with gzip.open(path, "wt", encoding="utf-8") as f:
        while written < rows:
            n = min(batch, rows - written)
            cols = mod._rows(n, model, rng)
            keys = list(cols)
            for i in range(n):
                f.write(json.dumps({k: cols[k][i] for k in keys}, separators=(",", ":")))
                f.write("\n")
            written += n
    return written, path.stat().st_size


def main():
    ap = argparse.ArgumentParser(description="Export synthetic http_logs as gzipped NDJSON shards")
    ap.add_argument("--rows-per-shard", type=int, default=1_000_000)
    ap.add_argument("--seeds", default="42", help="comma-separated seeds; one shard per seed")
    ap.add_argument("--out-dir", default="ndjson")
    ap.add_argument("--span-days", type=int, default=None, help="override SPAN_DAYS for this export")
    args = ap.parse_args()

    if args.span_days is not None:
        os.environ["SPAN_DAYS"] = str(args.span_days)  # corpus.py reads SPAN_DAYS at import
    mod = Workload("http_logs").corpus_module()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    seeds = [int(s) for s in args.seeds.split(",")]

    total_rows, total_bytes = 0, 0
    for seed in seeds:
        path = out / f"http_logs-seed{seed}.ndjson.gz"
        rows, size = _write_shard(mod, path, args.rows_per_shard, seed)
        total_rows += rows
        total_bytes += size
        print(f"  {path.name}: {rows:,} rows, {size / 1e6:.1f} MB gz")
    print(f"exported {total_rows:,} rows across {len(seeds)} shard(s) → {out} "
          f"({total_bytes / 1e6:.1f} MB gz, span_days={mod.SPAN_DAYS})")


if __name__ == "__main__":
    main()
