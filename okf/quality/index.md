# Quality

How GrowlerDB maintains quality and handles issues — process and methods, not just description.

* [Overview](/quality/overview.md) - the correctness guarantees and the methods that uphold them
* [Tests](/quality/tests/) - unit, integration, e2e, ui, connector, chaos, performance
* [Scalability & benchmarking](/quality/scalability.md) - how scale/perf targets are measured and regression-gated
* [Scale test plan (Hetzner)](/quality/scale-test-plan.md) - the repeatable scale run: what/how long is tested, the Hetzner cluster, run-duration cost, and the IaC
* [Reliability & resilience](/quality/reliability.md) - self-healing and recovery validated by fault injection
* [Security](/quality/security/) - tenant-isolation testing, supply-chain gates, review & disclosure
* [CI & gates](/quality/ci-and-gates.md) - the automated gates every change passes
* [Release readiness](/quality/release-readiness.md) - GA criteria, versioning, the release process
* [How issues are handled](/quality/issues.md) - the tracker and triage conventions
* [Known limitations](/quality/known-limitations/) - durable caveats and gaps
