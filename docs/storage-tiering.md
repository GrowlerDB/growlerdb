---
title: Storage & tiering
layout: default
nav_order: 11
---

# Storage tiering: hot vs cold (and when to use which)
{: .no_toc }

1. TOC
{:toc}

---

GrowlerDB keeps indexes on **local NVMe** for low-latency search, and can **tier** older,
immutable data to **object storage** to cut steady-state cost. Tiering is powerful for the right
workload and counterproductive for the wrong one — this page is the decision guide.

> Status: **shipped.** Time-windowing, **automatic in-place parking**, and **read-through serving
> from object storage** are live and validated on a windowed k3s cluster. A cold window stays
> **queryable** — a query that touches it is served read-through (slower, never unavailable), and a
> cold window that gets hot traffic again **pre-warms** back to NVMe. Inspect what's parked via
> `GET /v1/cold` or the console **Storage tiers** panel.

## The one rule: tiering needs immutability

Cold tiering works by **sealing an old time-window shard and shipping its byte-identical segments to
object storage**, where they are served **read-through** (the local Tantivy bulk is evicted; a small
locator + hot cache stay on NVMe). That only holds if the old data **never changes**. So:

| Your table | Index config | Why |
|---|---|---|
| **Append-mostly / time-series / events / logs** | **Time-window + tier** | Old windows are immutable → safe to seal, replicate, park, revive. ~90% of queries hit recent windows. |
| **Mutating / CDC entity table** | **Hash-shard, keep hot** | An update/delete to old data would force a parked shard to revive → mutate → re-park. Churn defeats the savings; keep it on NVMe. |

If a table is *partly* both (mostly-append with occasional late corrections), window by **ingest
time** (not event time) so corrections land in the current window and old windows stay immutable —
see [event-time vs ingest-time](#late-arriving-data-event-vs-ingest-time).

## How tiering works (the model)

1. **Window** the index by an ingest-time field into contiguous shards (e.g. daily).
2. **Hot window** (recent) stays on NVMe. Each node **automatically parks** its own windows once they
   age past the `hot_windows` policy (a background timer): the window's Tantivy **bulk** is shipped to
   object storage and evicted from local disk, while a small **locator** (`aux.redb` + `location.arr`)
   and a hot cache stay on NVMe. Parking is **in-place and non-interrupting** — the window keeps
   answering queries across the hot→cold swap. It's opt-in per deployment and node-local (the parked
   data lives on the node's own object-storage prefix).
3. A query **prunes** to the windows its time filter touches. If it touches a cold window, that window
   is served **read-through from object storage** — byte-range reads through a cache-bounded object
   directory, so the **query always completes** (cold is just slower). A cold window that starts
   getting hot traffic again **pre-warms** back to NVMe automatically. (The `growlerdb` CLI's `park` /
   `revive` also let you back a window up and fully restore it to NVMe on demand.)

A 30-day-hot window over a 180-day corpus keeps ~⅙ of the index's bulk on NVMe — modeled at roughly
**70–85% lower steady-state NVMe spend** for time-series workloads where ~90% of queries hit recent
data.

## Limitations (be honest before you design around it)

- **Cold reads are slower, not unavailable.** A cold window is queried **in place** (read-through from
  object storage), so the query always completes — but a cold hit pays object-store GET/egress and
  range-read latency vs. an NVMe hit, warming into the cache as it goes. Window pruning means most
  queries never touch cold at all; the cold-cache hit-rate is exposed as an SLI.
- **Whole-shard granularity.** You park a *whole cold window-shard*, not rows within a shard — which
  is why time-windowing is the prerequisite (cold data must be isolated into its own shards). A
  single shard mixing hot + cold data can't be partially tiered.
- **Cold-read economics.** Reaching back into cold data costs object-store GET/egress and slower
  range reads. Tiering pays off only when cold data is **genuinely rarely** queried; a workload that
  frequently reads old data will thrash the hot⇄cold pre-warm cycle and lose the savings.
- **Mutating data can't be tiered** (the rule above) — it stays hot.
- **Late backfills erode pruning.** A large late backfill widens a window's event-time zone-map, so
  event-time queries prune it less and scan it more (never *wrong*, just slower). Route known bulk
  backfills to a separate index.

## Late-arriving data (event vs ingest time)

Events carry two timestamps — **event time** (when it happened) and **ingest time** (when it landed
in Iceberg) — and the skew can be hours to days. **Window by ingest time** so late events always
land in the *current* window (old windows stay immutable → parkable). To keep **event-time queries
fast**, each window records an **event-time min/max zone-map** and the event field is a `fast`
column: a query prunes windows whose event-time range can't overlap its filter, and scans the rest
fast. The cost: a wide backfill broadens a window's event-time range and reduces pruning for that
window (it gets scanned, never mis-answered).

## Decision checklist

- [ ] Is old data **immutable** once written? → tiering is viable.
- [ ] Is it **time-partitionable** (a reliable ingest-time field)? → required for windowing.
- [ ] Are old windows **rarely queried**? → tiering pays off.
- [ ] Do you accept **slower (read-through) latency** on the occasional deep query into cold data? → required.
- [ ] Mutating / CDC entity table, or hot across the whole range? → **keep it hot, hash-shard**.
