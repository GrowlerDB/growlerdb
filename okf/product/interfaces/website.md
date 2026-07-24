---
type: Interface
title: Website (growlerdb.com)
description: The public documentation site and project landing page.
tags: [interface, website, docs]
resource: https://growlerdb.com
timestamp: 2026-07-04T14:22:00
---

# Website (growlerdb.com)

The public **documentation site**, built from `docs/` (a just-the-docs Jekyll site) and served via
GitHub Pages at **docs.growlerdb.com**. The bare apex **growlerdb.com** is a separate one-page landing
(`www/index.html`, hosted on a GCP VM behind Apache + Let's Encrypt) that links into the docs and repo.

## Contents

Getting started, install (Compose + Helm), configuration, the
[REST & gRPC reference](/docs/rest-api.md) (incl. the aggregations/facets surface), the
[query language](/docs/query-language.md), [comparison & positioning](/docs/comparison.md),
[performance (directional)](/docs/performance.md), storage tiering, and GA criteria/readiness.

Both the docs site and the apex landing page follow [Brand v1.0](/product/brand/index.md) — see
[brand surfaces](/product/brand/surfaces.md) for the homepage design and the docs theme.

## Discoverability (SEO)

Both hosts are built to be crawled and indexed. The apex `index.html` carries the on-page signals
inline — `<title>`, meta `description`, `canonical`, a `robots` directive
(`index, follow, max-image-preview:large`), Open Graph + Twitter card tags (1200×630 social image),
and **schema.org JSON-LD** (`Organization` + `WebSite` + a free `SoftwareApplication`) — and ships a
`robots.txt` + `sitemap.xml` (`www/`). The docs site emits per-page SEO/JSON-LD via `jekyll-seo-tag`
and a full `sitemap.xml` via `jekyll-sitemap`, with a `robots.txt` advertising it. Each host has its
**own** sitemap (apex is one page; docs is generated).

Getting indexed is a one-time, account-based submission per property (anonymous sitemap pings were
retired by Google and Bing in 2023): verify `growlerdb.com` and `docs.growlerdb.com` in Google Search
Console and Bing Webmaster Tools and submit each sitemap. The runbook + verification commands live in
[`www/README.md`](https://github.com/GrowlerDB/growlerdb/blob/main/www/README.md).

## Notes

Publishing the site (Pages) and pointing the domain is part of going public — see the GA-release work.
Deep design/system knowledge lives here in the OKF; the website is the user-facing docs surface.

The **getting-started** quickstart is validated end-to-end on a fresh box (Ubuntu 24.04 + macOS):
the core walkthrough (§1–§8) needs only **Docker + Compose v2 + `just` + `jq`** (dual-OS prereqs), on a
**host or VM — not a container** (Docker bind mounts don't resolve there), with **~4 GB RAM**. The
streaming demo (§9) additionally needs **`mise`** on the host to build the Spark connector jar (JDK 21
+ Maven). `just stack` builds the
shared `growlerdb-local:dev` image **once** before starting the control-plane/node/gateway, avoiding
a same-tag parallel-build race on Docker's containerd store; `setup-polaris.sh` parses the Polaris
token with `sed` (no python3 dependency). CI's `e2e` runs the engine in-process, so it doesn't
exercise the `--profile stack` build path — this quickstart is what covers it.

The quickstart also seeds a second, richer demo index — **`catalog`** (10 rows, one field of every
type) — served alongside `docs` and routed through the single `--all-indexes` gateway
([Compose](/system/deployment/compose.md)). Its **query playground** section walks every Lucene/KQL
operator (term, phrase, keyword, set, numeric/float/date range, CIDR, wildcard, prefix, fuzzy, boost,
bool, `NOT`, match-all, regex) with the exact rows each returns. Because two indexes are served with no
default, every REST search / `keys:get` names its `index`, and the console selector switches between them.

An optional **`trino` profile** (`just trino`) runs Trino over the *same* Polaris/MinIO Iceberg
catalog the seed writes, so users can `SELECT` (and `INSERT`) the source rows GrowlerDB indexes and
compare source vs. index — the "Iceberg is the system of record" story made tangible. Validated on a
fresh VM (Trino reads and writes `iceberg.growlerdb.docs`); the config disables Polaris credential
vending (`vended-credentials-enabled=false`) since MinIO can't vend STS.
