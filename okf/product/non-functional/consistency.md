---
type: Requirement
title: Consistency
description: Eventually consistent with Iceberg, bounded by ingestion lag.
tags: [nfr, consistency]
timestamp: 2026-07-04T14:22:00
---

# Consistency

The index is **eventually consistent** with Iceberg, bounded by ingestion
[lag](/product/non-functional/throughput.md). **Read-your-writes is not guaranteed** until a document
is ingested and committed (sub-second would need the deferred hot tier).

Within that bound, results are correct: a true cross-shard match `total`, an honest `partial` flag
when a shard is down, and [exactly-once](/product/functional/ingestion/checkpoints-exactly-once.md)
ingestion (no loss, no duplicates).

**Status.** The consistency model is a design property, not a tuning target.
