#!/usr/bin/env python3
"""Distribution-validation report for the synthetic http_logs corpus.

Generates a sample from the workload's own `corpus.py` recipe and reports the distribution shape, so
the realism claims in `bench/scale/synthetic-corpus.md` can be checked (and regressions caught). Runs
offline — no cluster, no Iceberg. Usage: `python corpus_stats.py [--rows N] [--seed S] [--json out]`.
"""

import argparse
import json
import random
import time
from collections import Counter

from harness import Workload


def _pcts(xs, ps=(50, 90, 99)):
    xs = sorted(xs)
    out = {}
    for p in ps:
        k = min(len(xs) - 1, int(round(p / 100.0 * (len(xs) - 1))))
        out[f"p{p}"] = xs[k] if xs else 0
    out["max"] = xs[-1] if xs else 0
    return out


def _share(counter, total, top):
    return round(100.0 * sum(c for _, c in counter.most_common(top)) / total, 1) if total else 0.0


def main():
    ap = argparse.ArgumentParser(description="Validation report for the synthetic http_logs corpus")
    ap.add_argument("--rows", type=int, default=300_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--json", default="")
    args = ap.parse_args()

    mod = Workload("http_logs").corpus_module()
    rng = random.Random(args.seed)
    model = mod._build_model(rng)

    status_c, method_c, path_c, ua_c, ip_c, user_c = (Counter() for _ in range(6))
    hour_c, wday_c, kind_status = Counter(), Counter(), {}
    sizes, rts, rt_5xx, rt_ok, nonzero_sizes = [], [], [], [], []
    n_done = 0
    raw_bytes = 0  # exact uncompressed corpus size = the compact-NDJSON serialization corpus_export.py writes
    first_row = None
    while n_done < args.rows:
        b = min(50_000, args.rows - n_done)
        cols = mod._rows(b, model, rng)
        if first_row is None:
            first_row = {k: cols[k][0] for k in cols}
        for i in range(b):
            # Match corpus_export.py byte-for-byte (compact separators + trailing newline) so
            # `raw_row_bytes` is the true uncompressed corpus row size, the ONLY valid "source" basis
            # for index:source ratios — never the compressed parquet intermediary. See synthetic-corpus.md.
            raw_bytes += len(json.dumps({k: cols[k][i] for k in cols}, separators=(",", ":")).encode()) + 1
            st = cols["status"][i]; p = cols["path"][i]; sz = cols["response_size"][i]; rt = cols["response_time_ms"][i]
            status_c[st] += 1; method_c[cols["method"][i]] += 1; path_c[p] += 1
            ua_c[cols["user_agent"][i]] += 1; ip_c[cols["client_ip"][i]] += 1; user_c[cols["user_id"][i]] += 1
            t = cols["ts"][i]; hour_c[time.gmtime(t).tm_hour] += 1; wday_c[time.gmtime(t).tm_wday] += 1
            kind = mod.path_kind(p); kind_status.setdefault(kind, Counter())[st] += 1
            rts.append(rt); (rt_5xx if int(st) >= 500 else rt_ok).append(rt)
            if sz:
                sizes.append(sz); nonzero_sizes.append(sz)
        n_done += b

    tot = n_done
    rep = {
        "rows": tot, "seed": args.seed, "span_days": mod.SPAN_DAYS,
        "raw_row_bytes": round(raw_bytes / tot, 1),  # avg uncompressed corpus row; the raw index:source basis
        "cardinality": {"path": len(path_c), "client_ip": len(ip_c), "user_id": len(user_c),
                        "user_agent": len(ua_c), "status": len(status_c)},
        "status_pct": {k: round(100.0 * v / tot, 2) for k, v in status_c.most_common()},
        "method_pct": {k: round(100.0 * v / tot, 2) for k, v in method_c.most_common()},
        "path_zipf": {"top1_pct": _share(path_c, tot, 1), "top5_pct": _share(path_c, tot, 5),
                      "top10_pct": _share(path_c, tot, 10), "top3_paths": [p for p, _ in path_c.most_common(3)]},
        "client_ip_concentration": {"distinct": len(ip_c), "top1_pct": _share(ip_c, tot, 1),
                                    "top100_pct": _share(ip_c, tot, 100), "top1000_pct": _share(ip_c, tot, 1000)},
        "user_activity": {"distinct": len(user_c), "top1_pct": _share(user_c, tot, 1),
                          "top100_pct": _share(user_c, tot, 100)},
        "response_size_bytes": {**_pcts(nonzero_sizes), "zero_pct_304s": round(100.0 * (tot - len(nonzero_sizes)) / tot, 1)},
        "response_time_ms": _pcts(rts),
        "latency_correlation": {"mean_ms_5xx": round(sum(rt_5xx) / len(rt_5xx), 1) if rt_5xx else 0,
                                "mean_ms_non5xx": round(sum(rt_ok) / len(rt_ok), 1) if rt_ok else 0},
        "diurnal_hour_pct": {h: round(100.0 * hour_c[h] / tot, 2) for h in range(24)},
        "weekday_pct": {d: round(100.0 * wday_c.get(d, 0) / tot, 2) for d in range(7)},
        "kind_status_pct": {kind: {s: round(100.0 * c / sum(cc.values()), 1) for s, c in cc.most_common(4)}
                            for kind, cc in kind_status.items()},
        "first_row": first_row,
    }
    if args.json:
        with open(args.json, "w") as f:
            json.dump(rep, f, indent=2)

    print(f"== synthetic http_logs corpus — validation report ({tot:,} rows, seed {args.seed}, span {mod.SPAN_DAYS}d) ==\n")
    print(f"raw uncompressed row: {rep['raw_row_bytes']} B/row (compact NDJSON) "
          f"-> raw corpus = rows * {rep['raw_row_bytes']} B; the ONLY valid index:source basis (not parquet)")
    print(f"cardinality: {rep['cardinality']}")
    print(f"\nstatus %: {rep['status_pct']}")
    print(f"  -> 200 share: {rep['status_pct'].get('200')}%  (target ~85-90%)")
    print(f"method %: {rep['method_pct']}")
    print(f"\npath Zipf: top1={rep['path_zipf']['top1_pct']}% top5={rep['path_zipf']['top5_pct']}% "
          f"top10={rep['path_zipf']['top10_pct']}%  hottest={rep['path_zipf']['top3_paths']}")
    print(f"client_ip: {len(ip_c):,} distinct; top100={rep['client_ip_concentration']['top100_pct']}% "
          f"top1000={rep['client_ip_concentration']['top1000_pct']}% (heavy hitters)")
    print(f"user_id: {len(user_c):,} distinct; top100={rep['user_activity']['top100_pct']}% (Zipf activity)")
    print(f"\nresponse_size B: {rep['response_size_bytes']}")
    print(f"response_time ms: {rep['response_time_ms']}")
    print(f"latency corr: 5xx mean {rep['latency_correlation']['mean_ms_5xx']}ms vs "
          f"non-5xx {rep['latency_correlation']['mean_ms_non5xx']}ms (5xx should be slower)")
    peak = max(rep["diurnal_hour_pct"], key=rep["diurnal_hour_pct"].get)
    trough = min(rep["diurnal_hour_pct"], key=rep["diurnal_hour_pct"].get)
    print(f"\ndiurnal: peak hour {peak} ({rep['diurnal_hour_pct'][peak]}%) vs trough hour {trough} "
          f"({rep['diurnal_hour_pct'][trough]}%)")
    print(f"weekday %: {rep['weekday_pct']} (0=Mon; weekend should dip)")
    print(f"\nkind->status %: {rep['kind_status_pct']}")
    if args.json:
        print(f"\nwrote {args.json}")


if __name__ == "__main__":
    main()
