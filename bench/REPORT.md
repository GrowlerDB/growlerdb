# GrowlerDB — quick performance assessment

A **ballpark** assessment to adjust now, not the proper pre-GA benchmark. Run against the local
`just stack` (Polaris + MinIO) on a **4 GiB / 2-CPU dev VM** (lima) — absolute numbers are
small-scale/cache-warm; the **ratios + the findings are the signal**.

Covers **1,000,000 readings** and a **top-K documents** comparison — the IoT-realistic *"show me the
matching readings"* query, not just a count — so GrowlerDB's coordinate→hydrate model is measured
fairly.

## Setup

- **Dataset:** 1,000,000 synthetic IoT telemetry readings in Iceberg `growlerdb.telemetry`
  (`bench/gen_telemetry.py`):
  `ts, device_id, gateway, site, firmware, metric, subsystem, status, reading, message` (free text),
  with injected needles (`gateway=gw-rare-eu7` ×12, `message~bearing` ×12).
- **Engines:** **GrowlerDB** (embedded `serve`, REST) — two indexes: `telemetry` (coordinates only) and
  `telemetry_cached` (display fields `cached: true` — the ES `_source` equivalent). **Trino 470** (queries
  the *same* Iceberg table — the scan baseline). **Elasticsearch 8.15** (same 1M dual-indexed).
  Indexes warm; 12–15 reps/query.
- Reproduce: `bench/` (`gen_telemetry.py`, `telemetry.yaml`, `telemetry_cached.yaml`, `load_es.py`,
  `bench.compose.yml`, `bench.py` = count, `bench_topk.py` = documents, `run_topk.sh`).

## A. Count / filter latency (median ms)

| query | hits | **GrowlerDB** | Trino (scan) | Elasticsearch |
|---|---:|---:|---:|---:|
| needle — rare keyword `gateway` | 12 | **1.7** | 225 | 5.7 |
| full-text rare `message~bearing` | 12 | **1.7** | 295 | 5.4 |
| filter `status=critical AND metric=vibration` | 15,459 | **3.8** | 156 | 5.7 |
| full-text common `message~reading` | 199,900 | **2.0** | 262 | 5.9 |
| time+filter `status=error` in a 1h window | 772 | **3.1** | 177 | 6.7 |

GrowlerDB ≈ 1.7–3.8 ms · ES ≈ 5–7 ms · Trino ≈ 156–295 ms. **GrowlerDB ~50–170× faster than Trino**
(index lookup vs full Iceberg scan), ~2–3× faster than ES. At 1M, Trino's scan grows with the data
(~225–295 ms) while the index lookups stay flat — **Trino scales with rows; the index doesn't.**

## B. Top-20 **documents** — "show me the matching readings" (median ms)

| query | GDB coords | **GDB cached** | GDB hydrate (Iceberg) | ES `_source` | Trino `SELECT *` |
|---|---:|---:|---:|---:|---:|
| needle (rare) | 1.6 | **1.9** | 93 | 7.5 | 153 |
| text ~bearing | 1.7 | **1.5** | 82 | 7.3 | 244 |
| filter | 6.2 | **6.5** | 139 | 10.0 | 259 |
| text ~reading | 2.8 | **3.1** | 81 | 7.7 | 155 |
| time+filter | 5.4 | **4.9** | 187 | 7.3 | 200 |

This is the honest picture for actually *returning readings* — GrowlerDB has three return modes:

1. **Cached display fields ≈ coordinates (≈ free).** Returning the telemetry fields from the index adds
   ~nothing over coordinates (~1.5–6.5 ms) and is **faster than ES `_source`** (7–10 ms) — embedded
   vs networked. For the common telemetry case (you read the reading summary), GrowlerDB is fast.
2. **Hydration from Iceberg = 80–190 ms.** Fetching the *authoritative governed row* is the real
   cost (the coordinate→hydrate model): ~15–50× the cached path. But it's **locator-targeted** (reads
   only the K matching rows via the PK locator), so it's still **~2× faster than Trino's scan**
   (150–260 ms) — and unlike ES `_source`, it's the live lakehouse row, not a search-time copy.

**Takeaway:** GrowlerDB wins decisively for *filter + display* (cached fields). For *authoritative
full-fidelity retrieval* it pays an Iceberg round-trip ES avoids — so cache the display fields you
serve hot, and reserve hydration for governed/audit reads.

## C. Other measures

- **Streaming build validated at 1M:** both indexes built cleanly (`telemetry`, `telemetry_cached`)
  on this 4 GiB VM, where a whole-table read would OOM-kill above ~500k.
- **Index footprint (1M, Tantivy index):** `telemetry` (no cached) **93 MB** · `telemetry_cached` **176 MB** ·
  ES **155 MB**. GrowlerDB's *plain* index is more compact than ES (no `_source`); with all display
  fields cached it's comparable. (GrowlerDB's `aux.redb` PK→Iceberg locator is extra, on top.)

## Findings to adjust now

- **A — the index/scan win is large and scales the right way.** 50–170× faster than Trino, flat
  where Trino grows. Validated at 1M.
- **B — the build streams** (bounded chunks) and indexes 1M+ without OOM.
- **C — hydration cost is the real "return the readings" tax.** 80–190 ms per top-K page from Iceberg.
  Mitigations: **cache the hot display fields** (≈ free, shown above); a future read-through/columnar
  hydration cache; and the cold-tier read path already serves cold data without a full
  restore. Worth a hydration-throughput line in the proper assessment.
- **D — ES `_source` beats GrowlerDB hydration on raw latency** (but not governance). If sub-10 ms
  *authoritative* doc retrieval is a requirement, that's a gap; cached fields close it for display.

## For the proper (pre-GA) assessment

- Real hardware (≥32 GiB) → **tens of millions** of events; concurrency / QPS under load; p95/p99;
  warm vs cold cache; **cold-tier read-through latency**; **hydration throughput** at K.
- Apples-to-apples ES/OpenSearch at scale (shards, `_source` on/off) + a Spark/Trino full-text
  baseline. Validate the cost model (index %, $/mo) with clean at-scale numbers.
