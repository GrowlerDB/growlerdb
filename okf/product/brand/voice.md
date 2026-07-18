---
type: Concept
title: Brand voice & terminology
description: GrowlerDB's verbal identity — the tagline, tone, and the canonical product vocabulary all copy must use.
tags: [brand, voice, copy, terminology]
timestamp: 2026-07-18T00:00:00
---

# Brand voice & terminology

The verbal side of [Brand v1.0](/product/brand/index.md). Applies to all copy — website, console,
docs, README, release notes.

## Tone

**Tagline: "Search your lake. Keep one truth."** Plain, confident, **falsifiable** claims; no
superlatives; **no iceberg puns in product UI** (the *growler* name story is for the name only). Sentence
case, never all-caps headlines.

## Canonical terminology

The [glossary](/glossary.md) holds the definitions; this table fixes the **word to use** (and the ones
to avoid) so the surfaces read as one product:

| Term | Use for | Not |
|---|---|---|
| **coordinates** | what a search hit returns (the composite Iceberg key) | doc ID, pointer, reference, "document keys" |
| **hydrate / hydration** | resolving coordinates to the authoritative row (`keys:get`) | fetch, lookup, resolve |
| **derived index** | the index GrowlerDB maintains — secondary, rebuildable | copy, replica, cache |
| **system of record** | Apache Iceberg, always | backend, upstream database |
| **connector / gateway / console** | the Spark job / the public API / the bundled web UI | ingester, proxy, dashboard |
| **growler** | the name story only (a small calved berg) | naming nodes/shards; mascot-speak |

Notable copy corrections from the pre-GA surfaces: **"document keys" → "coordinates"** everywhere; the
website hero tagline is **"Full-text search over Apache Iceberg."** Maturity wording follows the current
[release-readiness](/quality/release-readiness.md) state (Beta / pre-1.0) — see the caveat in
[D40](/system/decisions/d40-brand-system.md).
