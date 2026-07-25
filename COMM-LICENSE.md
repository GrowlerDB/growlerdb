# GrowlerDB Commercial License

GrowlerDB's core is open source under the **[GNU AGPL-3.0](LICENSE)**. For most users — including
internal/self-hosted production use — the AGPL is all you need, at no cost.

A **commercial license** is available from **GrowlerDB LLC** for cases the AGPL doesn't fit. This page
explains what it covers and when you need it; it is a summary, not the contract — the signed agreement
governs.

## When you need a commercial license

You need one if any of the following apply:

1. **Embedding / OEM.** You want to include GrowlerDB in a **proprietary or closed-source product** you
   distribute or offer as a service, without the AGPL's copyleft obligation to release your own source.
   A commercial license grants an **exception to AGPL section 13** for your product.
2. **AGPL non-compliance.** Your organization's policy prohibits AGPL, or you cannot meet the AGPL's
   requirement to offer corresponding source to users who interact with the software over a network.
3. **Enterprise capabilities.** You want to run GrowlerDB's **commercial add-ons** (advanced HA —
   zero-downtime windowed/multi-shard replica sets, cross-region DR; enterprise identity —
   SSO/SAML/SCIM; audit logging; managed multi-tenancy), or to operate **beyond the open-source
   scale limits** (node count / index size / data volume), which require a commercial license.

If none of these apply — you're using the open-source core and complying with the AGPL — you do **not**
need a commercial license.

## What it grants (summary)

- A non-exclusive, non-transferable right to use GrowlerDB (and, where purchased, the commercial add-ons)
  under terms compatible with **closed-source** distribution or SaaS — i.e. **without** the AGPL's
  source-disclosure and network-copyleft obligations for your product.
- The scope (deployments, seats, sites, term, support level) is set in your agreement.
- Trademarks are **not** granted here — see [`TRADEMARK.md`](TRADEMARK.md).

## What it does not change

- The open-source core stays **AGPL-3.0** for everyone. A commercial license is an **additional** grant
  to you; it does not make the core proprietary or remove anyone else's rights.
- Contributions remain governed by the [CLA](CLA.md); it is the CLA's sublicensing grant that lets
  GrowlerDB LLC offer this commercial option.

## Getting a license

Commercial and OEM licensing, and pricing, are handled by **GrowlerDB LLC**. Email
**[support@growlerdb.com](mailto:support@growlerdb.com)** (or see the channels in
[`SUPPORT.md`](SUPPORT.md)). Tell us your use case (embedding, SaaS, enterprise features, scale) and
we'll scope the right agreement.

## Issuing a scale-limit license (internal runbook)

The scale-limit entitlement ([D38](okf/system/decisions/d38-scale-limit-entitlement.md)) is an
Ed25519-signed token, minted **offline** by GrowlerDB LLC. The signing **private key is held privately
and never committed**; only the matching public key is embedded in the binary
(`crates/growlerdb-engine/src/license.rs`, `LICENSE_PUBLIC_KEY_PEM`).

```sh
# 1. Generate the signing keypair ONCE, offline. Keep the private key in a secret manager.
openssl genpkey -algorithm ed25519 -out license_ed25519.pem
openssl pkey -in license_ed25519.pem -pubout -out license_ed25519.pub.pem

# 2. Install the PUBLIC key: paste license_ed25519.pub.pem into LICENSE_PUBLIC_KEY_PEM (replacing the
#    placeholder) and ship a build. (This is the one-time step that activates licensing.)

# 3. Mint a token (e.g. 64 nodes, no expiry) — prints the JWT to stdout:
cargo run -p growlerdb-engine --example mint_license -- license_ed25519.pem "GrowlerDB scale" 64

# 4. Deploy it as GROWLERDB_LICENSE on the control plane: set credentials.license in the Helm values
#    (or `--set-string credentials.license=$GROWLERDB_LICENSE`). scale-up.sh forwards $GROWLERDB_LICENSE.
```

An invalid or absent token falls back to the free tier with a warning (never a startup failure).
