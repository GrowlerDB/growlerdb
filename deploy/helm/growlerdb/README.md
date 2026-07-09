# GrowlerDB Helm chart

Deploys the GrowlerDB **sharded-cluster topology** ([Design 14](../../../../wiki/14-deployment-ops.md))
on Kubernetes:

| Component | Primitive | Role |
|---|---|---|
| **control-plane** | StatefulSet (+PVC) | the cluster index **registry** (gRPC) |
| **node** | StatefulSet on local PVs | stateful **searcher/index**; builds + serves the index, self-registers |
| **gateway** | Deployment + Service/Ingress | public **Engine API** (query routing) + the **console UI** |

Plus a ConfigMap (catalog/object-store config), a Secret (credentials), PodDisruptionBudgets,
pod anti-affinity, an optional HPA, and a `helm test`.

## Prerequisites

1. **The GrowlerDB image in a registry your cluster can pull.** There's no public image — build
   and push it:
   ```sh
   docker build -t <your-registry>/growlerdb:<tag> -f deploy/Dockerfile .
   docker push <your-registry>/growlerdb:<tag>
   ```
2. **An Iceberg REST catalog (Polaris) + S3-compatible object store reachable from the cluster**,
   with the source table already present (the local-dev defaults point at `polaris:8181` /
   `minio:9000` — override `iceberg.*` and `credentials.*` for your lakehouse). For an end-to-end
   local lakehouse, see the [Compose stack](../../compose/README.md).

## Install

```sh
helm install gdb deploy/helm/growlerdb \
  --namespace growlerdb --create-namespace \
  --set image.repository=<your-registry>/growlerdb \
  --set image.tag=<tag> \
  --set iceberg.catalogUri=https://catalog.example/api/catalog \
  --set iceberg.s3Endpoint=https://s3.example \
  --set index.name=docs --set index.sourceTable=growlerdb.docs \
  --set credentials.catalogCredential='id:secret' \
  --set credentials.s3AccessKey=AKIA... --set credentials.s3SecretKey=...
```

Prefer `--set existingSecret=<name>` (with keys `catalogCredential`, `s3AccessKey`, `s3SecretKey`)
over inline credentials for anything real. Then:

```sh
helm test gdb -n growlerdb        # probes the gateway /readyz + runs a query
kubectl -n growlerdb port-forward svc/gdb-growlerdb-gateway 8080:8080
# open http://localhost:8080  → the console; REST API at /v1/...
```

## Key values

| Key | Default | Notes |
|---|---|---|
| `image.repository` / `image.tag` | `growlerdb` / appVersion | **set these** to your pushed image |
| `index.name` / `index.sourceTable` | `docs` / `growlerdb.docs` | the index built + served |
| `index.shards` | `1` | shard ordinals — the node StatefulSet runs **one pod per shard** |
| `gateway.reloadSecs` | `15` | control-plane topology poll/hot-reload interval (0 = fixed) |
| `node.persistence.storageClass` | `""` (default) | set to your **local/NVMe** class ([index store](../../../../wiki/04-index-store.md)) |
| `node.persistence.size` | `20Gi` | per-shard index PV |
| `controlPlane.persistence.size` | `1Gi` | registry PV |
| `gateway.ingress.enabled` | `false` | expose the console/API via Ingress |
| `gateway.autoscaling.enabled` | `false` | HPA on CPU |
| `gateway.prometheusUrl` | `""` | when set, proxies `/v1/stats/*` for the UI SLI panels |
| `gateway.auth.oidc.issuer` / `.audience` | `""` | enable OIDC/JWT auth + RBAC (**set for any Ingress-exposed deploy**) |
| `gateway.opensearch` | `false` | expose the OpenSearch-compatible `_search` adapter |
| `observability.otlpEndpoint` | `""` | OTLP collector base (e.g. `http://lgtm:4318`) → export traces |
| `metrics.serviceMonitor.enabled` | `false` | create a Prometheus-Operator ServiceMonitor (needs the CRD) |
| `credentials.existingSecret` | `""` | reference your own Secret instead of inlining creds |

### Observability & auth

Every component serves `/metrics` + `/healthz` + `/readyz` on its metrics port (the probes use them).
To **scrape** with kube-prometheus-stack, set `metrics.serviceMonitor.enabled=true` (and
`metrics.serviceMonitor.labels` to match the operator's `serviceMonitorSelector`). To **export traces**,
point `observability.otlpEndpoint` at an OTLP/HTTP receiver. To **authenticate**, set
`gateway.auth.oidc.issuer`/`.audience` — without it the gateway is **open** (fine on a private LAN,
not on a public Ingress).

See [`values.yaml`](values.yaml) for the full surface (resources, PDBs, affinity, probes,
security context).

## Sharding & resilience

- **`index.shards` shards, one primary pod each** (pod-K serves ordinal K on its own PV). The
  gateway reads the per-ordinal primaries + bucket map from the **live control-plane** over gRPC
  (`--control-plane --index`) and **hot-reloads** on change (reshard cutover / a primary moving).
- **StatefulSets** give each shard a stable identity + local PV; readiness gates a shard until its
  partition is built/restored (the probe `failureThreshold` covers a cold first build).
- **PodDisruptionBudgets** (when `shards`/`gateway.replicas` > 1) + **pod anti-affinity** spread
  shards across hosts, so a host loss takes down at most the shards on it — the gateway returns
  **honest partial results** for the missing shard(s) and the StatefulSet self-heals from the PV.
  *(For shard-at-a-time node drains, prefer a `maxUnavailable: 1` PDB — tune per cluster.)*
- No GrowlerDB state is irreplaceable — a lost shard's partition is rebuilt from Iceberg on replacement.

## Scope / not yet

This chart deploys what GrowlerDB ships as binaries today: **control-plane + node + gateway**, in a
**multi-shard** topology (one primary pod per shard). Not yet modelled (Design 14 / future work):

- **Zero-downtime per-shard replicas** — `serve --replica` segment-shipping is single-shard only, and
  a Service-fronted replica set would diverge under streaming writes; so a shard is briefly degraded
  (partial results) during pod loss/restart rather than transparently failed over.
- **Ingest-connector Deployments** — the streaming changelog reader is an external Spark job, not
  the `growlerdb` binary.
- **Compactor Deployment/CronJob** — compaction is currently in-process on the node.
