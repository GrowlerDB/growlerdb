# System

How GrowlerDB is implemented — the "how" behind the [product](/product/index.md) "what."

# Overview

* [Architecture](/system/architecture.md) - components, data flow, the JVM/Rust boundary
* [Repository layout](/system/git-repo.md) - the Cargo workspace and subprojects
* [Build & release](/system/build.md) - toolchain, CI workflows, the gate, the release pipeline

# Runtime & internals

* [Runtime](/system/runtime/) - the running components and their dependencies
* [Storage](/system/storage/) - the index store, data model, cold bundles, backup format, catalog metadata
* [Distribution](/system/distribution.md) - sharding, bucket routing, scatter-gather, elasticity
* [Query execution](/system/query-execution.md) - planning, pruning, PIT, cursors, cost guards
* [Observability](/system/observability.md) - OpenTelemetry instrumentation and SLIs

# Deployment & decisions

* [Deployment](/system/deployment/) - Compose, Helm/k8s, single-binary, sharded HA, IaC
* [Decisions](/system/decisions/) - architecture decision records (ADRs)
