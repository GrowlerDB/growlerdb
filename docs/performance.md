---
title: Performance (directional)
layout: default
nav_order: 9
---

# Performance (directional)
{: .no_toc }

1. TOC
{:toc}

> **Directional, not the formal suite.** These numbers come from a **ballpark assessment on a small
> dev VM** (4 GiB / 2 vCPU, lima), not the pre-1.0 at-scale benchmark. Absolute latencies are
> small-scale and cache-warm — **the ratios and the findings are the signal, not the milliseconds.**
> The formal staged suite (real hardware, tens of millions of rows, QPS/p95/p99, cold-tier
> read-through, hydration throughput) is tracked on the [roadmap](roadmap). Reproduce everything here
> from [`bench/`](https://github.com/GrowlerDB/growlerdb/tree/main/bench) in the repo.

## What was measured

- **Dataset:** 1,000,000 synthetic IoT telemetry readings in an Iceberg table
  (`ts, device_id, gateway, site, firmware, metric, subsystem, status, reading, message` free text),
  with injected needles (a rare `gateway` value ×12, a rare `message` term ×12).
- **Engines:** **GrowlerDB** (embedded `serve`, REST) in two flavours — `telemetry` (coordinates
  only) and `telemetry_cached` (display fields cached, the Elasticsearch `_source` equivalent);
  **Elasticsearch 8.15** (same 1M, dual-indexed); **Trino 470** querying the *same* Iceberg table (the
  scan baseline). Indexes warm; 12–15 reps per query.

This covers the IoT-realistic *"show me the matching readings"* query — top-K **documents**, not just a
count — so GrowlerDB's coordinate → hydrate model is measured fairly.

## Filter / count latency

Median ms, lower is better:

| Query | Hits | **GrowlerDB** | Elasticsearch | Trino (scan) |
|---|---:|---:|---:|---:|
| Needle — rare keyword `gateway` | 12 | **1.7** | 5.7 | 225 |
| Full-text rare `message ~ bearing` | 12 | **1.7** | 5.4 | 295 |
| Filter `status=critical AND metric=vibration` | 15,459 | **3.8** | 5.7 | 156 |
| Full-text common `message ~ reading` | 199,900 | **2.0** | 5.9 | 262 |
| Time + filter `status=error` in a 1h window | 772 | **3.1** | 6.7 | 177 |

GrowlerDB ≈ 1.7–3.8 ms · Elasticsearch ≈ 5–7 ms · Trino ≈ 156–295 ms — GrowlerDB is **~2–3× faster
than Elasticsearch** and **~50–170× faster than a Trino scan** on filtered search. At 1M rows Trino's
scan grows with the data while the index lookups stay flat: **Trino scales with rows; the index
doesn't.**

## Top-K documents — "show me the matching readings"

GrowlerDB has three return modes. Median ms for the top-20 documents:

| Query | GDB coords | **GDB cached** | GDB hydrate (Iceberg) | ES `_source` | Trino `SELECT *` |
|---|---:|---:|---:|---:|---:|
| Needle (rare) | 1.6 | **1.9** | 93 | 7.5 | 153 |
| Text `~ bearing` | 1.7 | **1.5** | 82 | 7.3 | 244 |
| Filter | 6.2 | **6.5** | 139 | 10.0 | 259 |
| Text `~ reading` | 2.8 | **3.1** | 81 | 7.7 | 155 |
| Time + filter | 5.4 | **4.9** | 187 | 7.3 | 200 |

1. **Cached display fields ≈ coordinates (≈ free).** Returning telemetry fields straight from the
   index adds almost nothing over coordinates (~1.5–6.5 ms) and is **faster than ES `_source`** (7–10
   ms) — embedded vs networked. For the common case (you read a reading summary), GrowlerDB is fast.
2. **Hydration from Iceberg = 80–190 ms.** Fetching the *authoritative governed row* is the real cost
   of the coordinate → hydrate model (~15–50× the cached path). But it is **locator-targeted** — it
   reads only the K matching rows via the PK locator — so it is still **~2× faster than Trino's scan**
   (150–260 ms), and unlike ES `_source` it is the live lakehouse row, not a search-time copy.

**Takeaway:** GrowlerDB wins decisively for *filter + display* (cached fields). For *authoritative,
full-fidelity retrieval* it pays an Iceberg round-trip that Elasticsearch avoids — so cache the display
fields you serve hot, and reserve hydration for governed/audit reads.

## Footprint & build

- **Streaming build validated at 1M:** both indexes built cleanly on the 4 GiB VM, where a
  whole-table read would OOM-kill above ~500k rows — the connector streams bounded chunks.
- **Index footprint (1M, Tantivy):** `telemetry` (no cached) **93 MB** · `telemetry_cached` **176 MB**
  · Elasticsearch **155 MB**. GrowlerDB's *plain* index is more compact than ES (no `_source`); with
  all display fields cached it is comparable. (The `aux.redb` PK → Iceberg locator is extra, on top.)

## Honest limitations

- **ES `_source` beats GrowlerDB hydration on raw latency** (but not governance). If sub-10 ms
  *authoritative* single-doc retrieval is a hard requirement, that is a gap today; cached display
  fields close it for the display case.
- The **cold-tier read path** already serves cold data without a full restore, but its at-scale
  read-through latency is part of the formal suite, not measured here.
- These are single-node, cache-warm, small-VM numbers. The formal suite (real hardware, tens of
  millions of events, concurrency/QPS, p95/p99, cold vs warm cache, hydration throughput at K, an
  apples-to-apples ES/OpenSearch-at-scale and Spark/Trino full-text baseline) is the pre-1.0
  deliverable on the [roadmap](roadmap).
