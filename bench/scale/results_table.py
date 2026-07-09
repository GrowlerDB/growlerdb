#!/usr/bin/env python3
"""Staged-run results table + extrapolation (task-185 AC#3/#4).

Reads the `results.json` that staged_run.py writes and emits (to stdout, Markdown):
  1. an INGEST step-up table (rate vs keep-up + resources),
  2. a STORAGE milestone table (size vs query/hydration latency, index:source, resources, Trino),
  3. an EXTRAPOLATION block projecting the scales the interim cluster can't reach (1 TB, 100k rec/s)
     from a linear least-squares fit of the measured points, each clearly labelled measured vs modeled
     with a ± band from the fit residual (task-79 convention).

Pure post-processing — no cluster, no deps. `python results_table.py results.json`.
"""
import json, sys

GB = 1e9
TB = 1e12


def linfit(xs, ys):
    """Ordinary least squares y = a*x + b over the measured points. Returns (a, b, r2, sigma) where
    sigma is the residual standard deviation (the ± band for a projection). Needs >=2 distinct xs."""
    n = len(xs)
    if n < 2 or len(set(xs)) < 2:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    a = sxy / sxx
    b = my - a * mx
    resid = [y - (a * x + b) for x, y in zip(xs, ys)]
    ss_res = sum(r * r for r in resid)
    ss_tot = sum((y - my) ** 2 for y in ys)
    r2 = 1 - ss_res / ss_tot if ss_tot else 1.0
    sigma = (ss_res / (n - 2)) ** 0.5 if n > 2 else abs(resid[0]) if resid else 0.0
    return a, b, r2, sigma


def project(xs, ys, at, unit=""):
    """One 'measured range → modeled point' line for the extrapolation block."""
    fit = linfit(xs, ys)
    if not fit:
        return f"  (need ≥2 distinct measured points to project{(' ' + unit) if unit else ''})"
    a, b, r2, sigma = fit
    y = a * at + b
    band = 1.96 * sigma  # ~95% from the residual spread — honest, not a formal CI
    return (f"  modeled @ {_h(at)}: {y:,.1f} ± {band:,.1f}{unit}  "
            f"(linear fit, n={len(xs)}, R²={r2:.3f}; measured {_h(min(xs))}–{_h(max(xs))})")


def _h(x):
    """Human bytes/rate for axis labels. Dual-use: byte sizes get MB/GB/TB, rates (≤100k here) get k."""
    if x >= TB:
        return f"{x / TB:g} TB"
    if x >= GB:
        return f"{x / GB:g} GB"
    if x >= 1e6:
        return f"{x / 1e6:g} MB"
    if x >= 1000:
        return f"{x / 1000:g}k"
    return f"{x:g}"


def ingest_table(steps):
    print("## Ingest step-ups (keep-up vs rate)\n")
    print("| target rec/s | index rec/s | rows behind | keeps up | node CPU cores |")
    print("|---:|---:|---:|:--:|---:|")
    for s in steps:
        print(f"| {s.get('target_rps', 0):,.0f} | {s.get('index_rate_dps', 0):,.0f} "
              f"| {s.get('rows_behind', 0):,.0f} | {'✅' if s.get('keeps_up') else '❌'} "
              f"| {s.get('node_cpu_cores', 0):.1f} |")
    print()


def storage_table(ms):
    print("## Storage milestones (perf vs size)\n")
    print("| source | index | ratio | query p95 | hydration p95 | CPU cores | converged | Trino best speedup |")
    print("|---:|---:|---:|---:|---:|---:|:--:|---:|")
    for m in ms:
        s = m.get("snapshot", {})
        tr = m.get("trino", {})
        cmps = tr.get("comparisons", []) if isinstance(tr, dict) else []
        best = max((c.get("speedup_x", 0) for c in cmps), default=None)
        best_s = f"{best:g}x" if best is not None else ("skipped" if tr.get("skipped") else "—")
        print(f"| {_h(s.get('source_bytes', 0))} | {_h(s.get('index_bytes', 0))} "
              f"| {s.get('index_source_ratio', 0):.2f}x | {s.get('query_p95_s', 0) * 1000:.1f} ms "
              f"| {s.get('hydration_p95_s', 0) * 1000:.1f} ms | {s.get('node_cpu_cores', 0):.1f} "
              f"| {'✅' if m.get('convergence_pass') else '❌'} | {best_s} |")
    print()


def extrapolation(data):
    print("## Extrapolation (measured → modeled)\n")
    ms = data.get("storage_milestones", [])
    src = [m["snapshot"].get("source_bytes", 0) for m in ms]
    if any(src):
        print("**Index bytes → 1 TB source** (is the index:source ratio stable at scale?)")
        print(project(src, [m["snapshot"].get("index_bytes", 0) for m in ms], TB, unit=" B"))
        print("\n**Query p95 → 1 TB source** (does latency stay sub-linear?)")
        print(project(src, [m["snapshot"].get("query_p95_s", 0) * 1000 for m in ms], TB, unit=" ms"))
    steps = data.get("ingest_steps", [])
    rps = [s.get("target_rps", 0) for s in steps]
    if any(rps):
        print("\n**Node CPU → 100k rec/s** (headroom for the modeled top rate)")
        print(project(rps, [s.get("node_cpu_cores", 0) for s in steps], 100_000, unit=" cores"))
    print("\n_Measured points are real; the modeled points are a linear projection with a ±1.96σ "
          "residual band — treat as order-of-magnitude, not a guarantee (task-79)._")


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "results.json"
    data = json.load(open(path))
    print(f"# Staged scale-run results\n\n_Source: `{path}`_\n")
    if data.get("ingest_steps"):
        ingest_table(data["ingest_steps"])
    if data.get("storage_milestones"):
        storage_table(data["storage_milestones"])
    extrapolation(data)


if __name__ == "__main__":
    main()
