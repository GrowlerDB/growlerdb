---
type: Concept
title: Source-health diagnostics
description: Diagnostic gauges that flag a source Iceberg table needing maintenance (compaction / expire_snapshots) — the remedy stays the user's, outside GrowlerDB.
tags: [system, observability, source, maintenance, scale]
resource: /crates/growlerdb-telemetry
timestamp: 2026-07-04T14:22:00
---

# Source-health diagnostics

GrowlerDB reads the source table O(files) on the query path — scan planning **and** hydration both
walk the current snapshot's data files. So a source table that accumulates **small files** or a long
**snapshot history** silently slows GrowlerDB down, with nothing in GrowlerDB's own metrics pointing
at the real cause.

GrowlerDB is **never** responsible for maintaining the source table — compaction, `expire_snapshots`,
and orphan cleanup are the user's job, outside GrowlerDB ([D30](/system/decisions/d30-layered-locator.md)).
Its job is **observability**: emit gauges that let an operator *diagnose* that the source wants
maintenance. It never runs that maintenance itself. This closes
[scale ceiling #2](/quality/known-limitations/scale-ceilings.md): the deliverable is
diagnostic metrics, not doing the work.

## The gauges

Emitted per index by the control-plane ingestion sampler (the same tick that refreshes
`growlerdb_ingest_lag_ms`), read from the current snapshot's `total-*` summary properties plus the
retained-snapshot count — **metadata only, no scan**. All are labelled `{index}`.

| Metric | Meaning | Elevated ⇒ |
|---|---|---|
| `growlerdb_source_data_files` | Data files in the current snapshot | The O(files) scan-planning/hydration cost; a steady climb is the primary read-slowdown driver. |
| `growlerdb_source_avg_file_bytes` | Mean data-file size (`bytes / data_files`) | **Low and falling** ⇒ many small files — the source wants **compaction**. This is the small-file signal. |
| `growlerdb_source_bytes` | Total data-file bytes | The size denominator behind the average; context for growth. |
| `growlerdb_source_delete_files` | Delete files in the current snapshot | Merge-on-read overhead — every read reconciles deletes; a high count wants compaction to rewrite them away. |
| `growlerdb_source_snapshots` | Retained snapshot count | Unbounded growth ⇒ fat table metadata — the source wants **`expire_snapshots`**. |
| `growlerdb_source_records` | Rows in the current snapshot | Scale context (pairs with `data_files` for rows-per-file). |

## Reading them (a runbook)

- **Queries slowing as the source grows** → check `growlerdb_source_data_files` (rising) and
  `growlerdb_source_avg_file_bytes` (falling). A low average with a high file count is the classic
  small-file problem: the user should run Iceberg compaction (`rewrite_data_files`) on the source.
- **Metadata/planning overhead** → `growlerdb_source_snapshots` climbing without bound means snapshot
  history is never expired; the user should schedule `expire_snapshots`.
- **Merge-on-read cost** → a high `growlerdb_source_delete_files` means reads pay to apply deletes;
  compaction rewrites them into the data files.
- Alert thresholds are workload-dependent — set them per table against the observed baseline (e.g.
  "avg file size dropped below X for 1h", "snapshot count above N"). GrowlerDB ships the gauges, not
  the thresholds.

In every case **the fix is the user's**, applied to the source table with ordinary Iceberg
maintenance — GrowlerDB only surfaces the signal.

## Limits

- The gauges come from the snapshot `summary` an Iceberg writer populates by convention
  (`total-data-files`, `total-files-size`, `total-delete-files`, `total-records`). A writer that
  omits a property makes that gauge read 0 — the values are diagnostic, not authoritative accounting.
- **Manifest / metadata file byte sizes are not exposed** by the Iceberg reader API GrowlerDB uses
  (the manifest list is a path only), so there is no direct "metadata bytes" gauge; the snapshot
  count and total data bytes are the cheap proxies for metadata weight. Computing exact manifest
  sizes would require extra reads — deliberately out of scope for a zero-scan diagnostic.
- `growlerdb_source_avg_file_bytes` is the cheap small-file signal; a precise sub-threshold
  *file-count ratio* would require reading manifests (the scan plan) rather than the summary, which
  this diagnostic avoids by design.

## See also

The instrumentation itself is the [observability](/system/observability.md) concern; the user-facing
dashboards/alerts are the [observability feature](/product/functional/observability.md). GrowlerDB's
own maintenance (index compaction, orphan reclamation — distinct from the source's) is the
[compactor / maintenance](/system/runtime/components/compactor-maintenance.md) component.
