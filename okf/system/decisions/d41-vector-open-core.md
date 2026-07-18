---
type: Decision
title: 'D41. Vector / RAG is open-core: capability open, scale + governance paid'
description: The vector/hybrid retrieval capability ships in the AGPL core, not behind the paywall; monetization is the existing scale cap plus AI governance and managed operations around it.
tags: [decision, adr]
timestamp: 2026-07-18T00:00:00
---

# D41. Vector / RAG is open-core: capability open, scale + governance paid

**Decision.** The **vector / hybrid retrieval capability is open** — it ships in the AGPL-3.0 core, not
behind the Enterprise paywall. Vector fields, local embeddings, the ANN index, RRF fusion, filtered
KNN, and the reranker seam ([D19](/system/decisions/d19-ann-library.md) /
[D20](/system/decisions/d20-embedding-model.md) / [D21](/system/decisions/d21-reranker.md)) are part of
the free engine. Monetization is the **scale + governance + managed layer around** it, not the feature
itself.

**Why the capability stays open.**

- **Adoption is the OSS engine's job**, and vector/hybrid is the single hottest reason to try a search
  engine right now. Gating it kills the biggest top-of-funnel driver.
- **It is table-stakes.** Elasticsearch, OpenSearch, pgvector, Qdrant, Weaviate, and LanceDB all ship
  vector free/open. An OSS search engine *without* free vector reads as crippled, not premium.
- **Nothing proprietary is being protected** — Tantivy KNN/HNSW, local BGE embeddings, RRF, and the
  reranker seam are all open tech; a paywall here guards nothing.
- **AGPL already monetizes it.** A competitor cannot offer GrowlerDB-vector as a managed SaaS without
  AGPL compliance or a [commercial license](/system/decisions/d36-license-agplv3.md), and the node-cap
  entitlement already gates large fleets.

This is consistent with the project's own line — [D38](/system/decisions/d38-scale-limit-entitlement.md):
*"scale is the gate, not code"* — and with how automatic cold-tiering
([D39](/system/decisions/d39-automatic-cold-tiering.md)) shipped open rather than paywalled.

**What is Enterprise** (out-of-tree via the [extension seams](/system/decisions/d37-extension-seams.md),
consistent with the existing SSO/SAML · audit · advanced-HA · managed-multi-tenancy add-on line):

- **Scale** — the existing node-cap ([D38](/system/decisions/d38-scale-limit-entitlement.md)) applies to
  vector too; large vector fleets need a license. This is the primary monetization and it is already in
  place — vectors just ride it.
- **AI governance** — audit logging of RAG / agent retrieval (who / which agent retrieved what),
  DLP / policy on outbound provider calls (what text leaves for external embedding), advanced tenant
  controls. Natural extensions of the existing audit + managed-multi-tenancy add-ons.
- **Enterprise agent access** — SSO / SCIM + audit for the MCP retrieval server. The *basic* MCP server
  stays open (it is the agent-adoption driver); only the enterprise identity/audit around it is gated.
- **Managed / accelerated infra** — hosted or GPU embedding, advanced / proprietary rerankers, a hosted
  RAG service.

**Status.** Accepted.
