# GrowlerDB Governance

GrowlerDB is an open-source project (AGPL-3.0) **stewarded by GrowlerDB LLC**. This document describes
how the project is run today. It is intentionally lightweight and will evolve as the community grows.

## Stewardship

GrowlerDB LLC maintains the project, holds the **GrowlerDB** trademark (see [`TRADEMARK.md`](TRADEMARK.md)),
and offers commercial/OEM licensing and support. The open-source core stays **AGPL-3.0**. A move to an
independent foundation is deferred, not ruled out (see ADR **D27**).

## Roles

- **Maintainers** review and merge changes, set direction, and cut releases. Today this is the founding
  maintainer; additional maintainers are added by invitation as sustained, high-quality contribution
  earns trust.
- **Contributors** are anyone who opens issues or pull requests. Contributions are welcome under the
  [`CLA`](CLA.md) and the [Code of Conduct](CODE_OF_CONDUCT.md).

## How decisions are made

- **Everyday changes** go through pull requests; a maintainer reviews and merges.
- **Significant changes** (public APIs, on-disk format, cross-component contracts, notable new
  behaviour) are discussed in an issue/RFC first, and the outcome is recorded as an **ADR** under
  [`okf/system/decisions/`](okf/system/decisions/). The [OKF](okf/) is the living source of truth.
- Disagreements are resolved by discussion; the maintainers make the final call.

## Releases & security

Versioning and the release process are in [`RELEASING.md`](RELEASING.md); security reporting is in
[`SECURITY.md`](SECURITY.md); how to get help is in [`SUPPORT.md`](SUPPORT.md).

## Commercial offering

Enterprise capabilities and support are provided by GrowlerDB LLC and are separate from this
open-source project; they never remove capability from the AGPL core (see ADR **D37**).
