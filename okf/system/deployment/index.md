# Deployment

How GrowlerDB is packaged and deployed, and the topologies.

* [Docker Compose](/system/deployment/compose.md) - the single-host stack for dev and CI
* [Helm / Kubernetes](/system/deployment/helm-k8s.md) - the production path (sharded chart + in-cluster deps)
* [Single-binary (embedded)](/system/deployment/single-binary.md) - one binary serving a single index
* [Sharded HA topology](/system/deployment/sharded-ha.md) - the distributed topology and availability posture
* [Infrastructure as Code](/system/deployment/iac.md) - repeatable cloud/scale provisioning
