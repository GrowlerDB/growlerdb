---
type: Concept
title: GrowlerDB — Overview
description: What GrowlerDB is — a fast, derived, open-source retrieval engine (full-text, vector & hybrid) over your data, keyed back to an authoritative source (Apache Iceberg today).
tags: [overview, thesis, product]
timestamp: 2026-07-04T14:22:00
---

# GrowlerDB — Overview

GrowlerDB is an **open-source retrieval engine — full-text, vector, and hybrid search over your
data**. It keeps your source (Apache Iceberg today) as the system of record and maintains a fast,
**derived, rebuildable** index of it, fed by streaming ingestion — then serves search that returns
**primary keys**, which resolve straight back to the authoritative rows.

## The problem

Operational and analytical data increasingly lives in **Apache Iceberg** — the lakehouse system of
record. But Iceberg is built for *scans* (columnar files on object storage, partition pruning, big
sequential reads); it is excellent for analytics and poor at what search needs — **fast full-text
lookup and low-latency point retrieval**. The usual answer is to ETL a second copy of the data into a
separate search store, which brings a second source of truth that drifts, a hand-built sync pipeline
to babysit, governance reinvented, and a key/identity mismatch between the index and the table.

## The thesis

> Keep your source as the system of record. Build a fast, derived, open-source retrieval index
> (full-text, vector & hybrid) *of* the data, fed by streaming ingestion, and serve search that
> returns primary keys — which resolve straight back to the authoritative rows.

Three commitments make it work:

1. **The index is derived and rebuildable, never authoritative.** Iceberg owns the data; GrowlerDB
   owns a secondary index that can be dropped and rebuilt from Iceberg at any time. There is one
   source of truth, and the index is explicitly subordinate to it.
2. **The index lives in a purpose-built, *local*, open-source store — not in Iceberg.** It is
   [Tantivy](/system/storage/index-store.md) segments on local NVMe, durably backed up to object
   storage. Local-first means search-engine latency, not object-storage-scan latency.
3. **Primary keys are the bridge.** Documents are indexed under their Iceberg
   [composite key](/system/storage/data-model.md); search returns keys (with scores and optional
   cached display fields); the full authoritative record is a fast
   [point lookup](/product/functional/hydration.md) against Iceberg.

## What it buys you

| Value | How |
|---|---|
| One source of truth | Your source owns the data; the index is a derived view |
| Always in sync | Continuous streaming ingestion from your source, with checkpoints (Apache Iceberg today; Delta, CDC & Kafka on the roadmap) |
| Fast search | Local Tantivy index on NVMe; milliseconds, not object-storage round trips |
| Governed retrieval | Search returns keys; the authoritative, access-controlled row is fetched from Iceberg |
| Rebuildable & cheap to operate | Lose the index? Replay from Iceberg. Durability tier on object storage |
| Open end to end | Open search engine + open index store + open clients |

## What GrowlerDB is *not*

- **A retrieval engine, not the app layer above it.** No detection/alerting/percolator (that is the
  app layer above GrowlerDB); not an analytics/OLAP engine; not a datastore.
- **Not a system of record.** The index holds only what it needs to search and to point back to
  Iceberg; Iceberg holds the truth.
- **Not "search inside Iceberg files."** GrowlerDB maintains a dedicated index store next to Iceberg
  rather than trying to make Iceberg itself fast at point lookups.
- **Not a from-scratch scoring engine.** It stands on Tantivy.
- **Not a closed appliance.** Open end to end.

## Navigating this knowledge base

- **[Product](/product/index.md)** — what users can do: interfaces, actors, use cases, functional &
  non-functional capabilities.
- **[System](/system/index.md)** — how it is implemented: architecture, runtime, storage, deployment,
  and decisions.
- **[Quality](/quality/index.md)** — how quality is maintained: tests, security, reliability, release
  readiness, and how issues are handled.
- **[Workflow](/workflow.md)** — how we work: contribution, the gate, and the rule that **every PR
  updates this OKF**. Also the OKF authoring conventions.
- **[Glossary](/glossary.md)** — GrowlerDB terminology.
