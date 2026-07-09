# Security

* [Tenant-isolation testing](/quality/security/tenant-isolation.md) - A standing end-to-end test that forged headers and query-widening cannot cross tenants, and that an unauthenticated request is rejected before it reaches a shard.
* [Supply-chain gates](/quality/security/supply-chain.md) - cargo-deny gates licenses, advisories, and bans in CI; releases produce an SBOM and cosign signatures; dependency and secret scanning run on the repository.
* [Security review & disclosure](/quality/security/review-and-disclosure.md) - A threat model in SECURITY.md, an independent security review before going public, and a coordinated-disclosure policy with a security contact.
