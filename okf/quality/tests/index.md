# Tests

* [Unit tests](/quality/tests/unit.md) - Fast in-crate Rust tests over pure logic and helpers — the bulk of coverage, run on every PR via cargo test.
* [Integration tests](/quality/tests/integration.md) - Cross-crate and stack-gated tests against real dependencies; stack-dependent paths are marked ignored and factored so pure logic stays testable without a live stack.
* [End-to-end tests](/quality/tests/e2e.md) - The walking-skeleton (index to search to hydrate) against the real Compose stack on every PR; the full suite runs nightly.
* [Console tests](/quality/tests/ui.md) - svelte-check type-checking, vitest unit tests, and a mocked-API Playwright suite over the console flows.
* [Connector tests](/quality/tests/connector.md) - JVM tests for the Spark and Trino connectors, including changelog read and the Trino execution path verified offline against a loopback gRPC server; live engine round-trips are stack-gated.
* [Chaos & resilience tests](/quality/tests/chaos-resilience.md) - Fault-injection drills that assert self-healing and recovery — see reliability.
* [Performance benchmarks](/quality/tests/performance-bench.md) - The benchmark harness measuring latency, throughput, and top-K document retrieval — see scalability.
