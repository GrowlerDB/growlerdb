---
type: Concept
title: Infrastructure as Code
description: Repeatable provisioning for the cloud/scale clusters.
tags: [deployment, iac, terraform]
timestamp: 2026-07-04T14:22:00
---

# Infrastructure as Code

Provisioning the cloud/scale clusters (a Hetzner k3s cluster for the scale test) as **repeatable
Infrastructure-as-Code** — Terraform + the hcloud provider, in **`deploy/iac/`**: a private-network,
firewalled k3s cluster (one server + `node_count-1` agents, bootstrapped by cloud-init), parameterized
by node count/type, shard count, and dataset, with `apply`/`destroy`.

## Notes

The GrowlerDB workload is deployed onto the provisioned cluster via
[Helm](/system/deployment/helm-k8s.md) and driven by the scale-test harness (`bench/scale/`); the run
is specified by the [scale test plan](/quality/scale-test-plan.md). Secrets (the cloud token) and
state stay out of the repo. Parameterized `apply`/`destroy` make scale/regression runs repeatable with
clean, cost-guarded teardown.
