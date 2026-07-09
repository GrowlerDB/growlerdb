# Security Policy

## Reporting a vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately via [GitHub Security Advisories](https://github.com/GrowlerDB/growlerdb/security/advisories/new)
(Security → Report a vulnerability). Include affected version/commit, a description, and ideally a
reproduction. We aim to acknowledge within **3 business days** and to provide a remediation plan or
fix timeline within **10 business days**. Coordinated disclosure is appreciated — we'll agree on a
public-disclosure date with you.

## Supported versions

While GrowlerDB is pre-1.0, security fixes land on `main` and the latest minor release line. Once
1.0 ships, the two most recent minor lines receive security fixes.

## Security model (summary)

A summary of GrowlerDB's security design:

- **AuthN at the Gateway.** OIDC/JWT (validated against the issuer's JWKS), API keys, and mTLS
  between services. The Gateway is the trust boundary: it **drops any caller-asserted
  `x-growlerdb-principal`/`-tenant`/`-roles` headers and replaces them with the verified claim**
  before routing, so shards only ever see a vouched-for identity.
- **AuthZ.** Control-plane RBAC at the Engine (role → operation scopes). **Data-plane authorization
  is delegated to the lakehouse**: `_source` is hydrated from Iceberg via PK lookup, governed by
  the catalog (Polaris) — the lake governs the authoritative rows.
- **Tenant isolation.** When an index sets `tenant_field`, every read has a mandatory, non-scoring
  `tenant_field = <verified claim>` filter ANDed in; no query structure (`OR`, nested bool) can
  widen past it, and a missing claim is denied. Verified end-to-end in
  `crates/growlerdb-engine/tests/tenant_isolation.rs`.
- **Encryption.** In transit via TLS/mTLS; at rest via encrypted volumes + bucket encryption
  (deployment-provided).

## Supply chain

- Dependencies are gated in CI by [`cargo-deny`](deny.toml) (license allow-list + advisory + bans
  + source checks).
- Release artifacts are built in CI, accompanied by an **SBOM** and **cosign signatures** (see
  [RELEASING.md](RELEASING.md)); verify signatures before deploying.

## Hardening checklist for operators

- Enable AuthN (`gateway --oidc-issuer …` or API keys) — the Gateway is **open** without it.
- Use mTLS between Gateway↔node↔control-plane on untrusted networks.
- Set `tenant_field` on multi-tenant indexes; confirm every client token carries the tenant claim.
- Restrict the control-plane and node ports to the cluster network; expose only the Gateway.
- Run images as the non-root `growlerdb` user (the chart's default) with read-only root FS where
  possible.
