---
title: Storage & tiering
layout: default
nav_order: 8
---

# Storage tiering: hot vs cold (and when to use which)
{: .no_toc }

1. TOC
{:toc}

---

GrowlerDB keeps indexes on **local NVMe** for low-latency search, and can **tier** older,
immutable data to **object storage** to cut steady-state cost. Tiering is powerful for the right
workload and counterproductive for the wrong one — this page is the decision guide.

> Status: the **windowing** that tiering builds on is in progress; **parking/revive**
> and the object-storage-*served* backend are on the roadmap. The guidance below is
> the design contract so you can model deployments against it.

## The one rule: tiering needs immutability

Cold tiering works by **sealing an old time-window shard, shipping byte-identical segments, and
evicting it to object storage** — revived on demand. That only holds if the old data **never
changes**. So:

| Your table | Index config | Why |
|---|---|---|
| **Append-mostly / time-series / events / logs** | **Time-window + tier** | Old windows are immutable → safe to seal, replicate, park, revive. ~90% of queries hit recent windows. |
| **Mutating / CDC entity table** | **Hash-shard, keep hot** | An update/delete to old data would force a parked shard to revive → mutate → re-park. Churn defeats the savings; keep it on NVMe. |

If a table is *partly* both (mostly-append with occasional late corrections), window by **ingest
time** (not event time) so corrections land in the current window and old windows stay immutable —
see [event-time vs ingest-time](#late-arriving-data-event-vs-ingest-time).

## How tiering works (the model)

1. **Window** the index by an ingest-time field into contiguous shards (e.g. daily).
2. **Hot window** (recent) stays on NVMe; **cold windows** (aged past a policy) are **parked**:
   backed up to object storage and evicted from local disk, built on the
   [backup/restore](install#run-modes) machinery.
3. A query **prunes** to the windows its time filter touches; if it touches a parked window, that
   window is **revived** (restored to NVMe) before serving.

A 30-day-hot window over a 180-day corpus keeps ~⅙ of the index on NVMe — modeled at roughly
**70–85% lower steady-state NVMe spend** for time-series workloads where ~90% of queries hit recent
data.

## Limitations (be honest before you design around it)

- **No direct query-from-object-storage (today).** "Cold" means *parked + revive-before-serve*, not
  queried in place. A query hitting a parked window pays a **cold-start** (download + open: seconds
  to minutes by size) and needs free NVMe to revive into. The object-storage-served backend (D3)
  that would query cold data in place with caching is **deferred**.
- **Whole-shard granularity.** You park a *whole cold window-shard*, not rows within a shard — which
  is why time-windowing is the prerequisite (cold data must be isolated into its own shards). A
  single shard mixing hot + cold data can't be partially tiered.
- **Revive economics.** Reaching back into cold data costs object-store GET/egress + the revive
  time. Tiering pays off only when cold data is **genuinely rarely** queried; a workload that
  frequently reads old data will thrash (revive → query → re-park).
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
- [ ] Do you accept a **cold-start** on the occasional deep query? → required (until D3).
- [ ] Mutating / CDC entity table, or hot across the whole range? → **keep it hot, hash-shard**.
