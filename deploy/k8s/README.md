# Deploying GrowlerDB to Kubernetes (multi-shard)

A runbook for a **multi-shard, multi-node** GrowlerDB cluster: the
[Helm chart](../helm/growlerdb) deploys control-plane + node (one pod per shard) + gateway, and
[`deps/`](deps) deploys the dependencies it needs (MinIO, Postgres, Apache Polaris) — a faithful port
of the [Compose stack](../compose). Two worked configs ship as
[`values-microk8s.yaml`](../helm/growlerdb/values-microk8s.yaml) (resilience / chaos cluster) and
[`values-hetzner.yaml`](../helm/growlerdb/values-hetzner.yaml) (cloud scale).

> The dependency manifests are dev-grade (single replicas, demo credentials) — for the lab/test
> clusters, not production. Polaris-on-K8s is version-sensitive; validate on first apply (below).

## Scale test — one command

For the Hetzner scale test the whole sequence below is automated by
[`scale-up.sh`](scale-up.sh) against [`values-scale.yaml`](../helm/growlerdb/values-scale.yaml) (the
in-cluster variant of values-hetzner: static-Prometheus scrape, no ingress/OIDC, `_search` adapter on,
deps credentials wired). It enforces the ordering that bites when done by hand — **deps → generator
creates the table → chart builds the shards → connector (`--nodes` sized to the shard count) →
observability → verify**:

```sh
export KUBECONFIG=deploy/iac/kubeconfig.yaml            # from `terraform -chdir=deploy/iac output`
GHCR_PAT=ghp_xxx GH_USER=<you> deploy/k8s/scale-up.sh   # PAT needs read:packages
```

The manual steps 1–6 below document each piece (and remain the path for microk8s / a custom source
table). Everything after covers those pieces in detail.

## 0. Prerequisites

- `kubectl` + `helm` pointed at the cluster.
- **microk8s:** `microk8s enable dns hostpath-storage`.
- A registry **all nodes can pull from**. We use a **private GHCR repo** (`ghcr.io/growlerdb/growlerdb`)
  for both clusters — the microk8s built-in registry's `localhost:32000` only resolves on the node
  hosting it, so it doesn't work across 3 nodes.

## 1. Build & push the image (private GHCR)

```sh
echo $CR_PAT | docker login ghcr.io -u <github-username> --password-stdin   # PAT with write:packages
docker build -t ghcr.io/growlerdb/growlerdb:dev -f deploy/Dockerfile .
docker push ghcr.io/growlerdb/growlerdb:dev
```

The package is private by default. (Hetzner: tag `:latest` or your release tag the same way.)

## 2. Namespace + pull secret + dependencies

```sh
kubectl create namespace growlerdb
# Read-only token (read:packages) so nodes can pull the private image:
kubectl -n growlerdb create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io --docker-username=<github-username> --docker-password=$CR_PAT_READONLY
kubectl -n growlerdb apply -k deploy/k8s/deps
# Watch them come up (Polaris bootstraps its realm via an init container, then serves):
kubectl -n growlerdb rollout status deploy/minio deploy/polaris
kubectl -n growlerdb wait --for=condition=complete job/polaris-catalog-setup --timeout=300s
```

Service names (`minio`, `polaris`, `polaris-db`) match the chart's `iceberg.*` defaults, so no extra
wiring is needed when deps + chart share the namespace.

## 3. Seed the source table

GrowlerDB indexes an **existing** Iceberg table. For the demo `growlerdb.docs`, build + push the seed
image ([`deploy/compose/seed`](../compose/seed), pyiceberg) and run it as a one-shot:

```sh
docker build -t <registry>/growlerdb-seed:dev deploy/compose/seed && docker push <registry>/growlerdb-seed:dev
kubectl -n growlerdb run seed --rm -i --restart=Never --image=<registry>/growlerdb-seed:dev \
  --env POLARIS_URI=http://polaris:8181/api/catalog \
  --env POLARIS_CATALOG=growlerdb --env POLARIS_CREDENTIAL=root:s3cr3t \
  --env AWS_ENDPOINT_URL_S3=http://minio:9000 \
  --env AWS_ACCESS_KEY_ID=minioadmin --env AWS_SECRET_ACCESS_KEY=minioadmin
```

For the **scale** cluster, seed a large table instead (the bench generator
[`gen_telemetry.py`](../../bench)) and point `index.sourceTable` at it. For a real lakehouse, skip
the deps/seed and point `iceberg.*` + `index.sourceTable` at your catalog/table.

## 4. Install the chart

```sh
# microk8s (resilience): 3 shards across the 3 nodes
# Polaris >= 1.5 enforces auth on the catalog API - the chart's empty credential defaults 401;
# pass the demo creds (or point credentials.existingSecret at your own):
helm install gdb deploy/helm/growlerdb -n growlerdb -f deploy/helm/growlerdb/values-microk8s.yaml \
  --set credentials.catalogCredential=root:s3cr3t \
  --set credentials.s3AccessKey=minioadmin --set credentials.s3SecretKey=minioadmin

# Hetzner (scale): 6 shards, HPA, OIDC, Ingress — edit the CHANGEME values first
helm install gdb deploy/helm/growlerdb -n growlerdb -f deploy/helm/growlerdb/values-hetzner.yaml
```

Each `node` pod builds its shard from the source on first boot, registers with the control-plane, and
the gateway (`--control-plane --index`) fronts the live shard map and hot-reloads on change.

## 5. Verify

```sh
kubectl -n growlerdb rollout status statefulset/gdb-growlerdb-node
kubectl -n growlerdb rollout status deployment/gdb-growlerdb-gateway
helm test gdb -n growlerdb
kubectl -n growlerdb port-forward svc/gdb-growlerdb-gateway 8080:8080   # → http://localhost:8080 (console + /v1)
```

## 6. Observability — light up the console (SLI panels + health pill)

The console's Observability panels and header Health pill read metrics through the gateway's
`/v1/stats` proxy. Two things must be wired:

**a) Point the gateway at your metrics backend** — set `gateway.prometheusUrl` to a Prometheus-style
query API (the values files do this; the reference deployment uses Grafana Mimir):

```yaml
gateway:
  prometheusUrl: "http://<your-metrics-backend>/prometheus"   # or http://<prometheus>:9090
```

Without it, `/v1/stats/*` falls through to the SPA and the page errors with
`SyntaxError: Unexpected token '<'`.

**b) Scrape GrowlerDB's `/metrics` into that backend** — every component serves Prometheus metrics on
its metrics port (control-plane `:9101`, node `:9102`, gateway `:9103`). With a **Prometheus-Operator**
present, set `metrics.serviceMonitor.enabled=true`. Otherwise add a scrape job to your agent. Example
for **Grafana Agent** (static mode) — append to its `metrics.configs`, remote-writing to your store:

```yaml
- name: growlerdb
  remote_write:
    - url: http://<your-metrics-backend>/api/v1/push
      tls_config: { insecure_skip_verify: true }
  scrape_configs:
    - job_name: growlerdb            # control-plane + gateway (single services)
      metrics_path: /metrics
      static_configs:
        - targets:
            - gdb-growlerdb-controlplane.growlerdb.svc.cluster.local:9101
            - gdb-growlerdb-gateway.growlerdb.svc.cluster.local:9103
          labels: { namespace: growlerdb }
    - job_name: growlerdb-node       # node shards (headless svc → all pod IPs)
      metrics_path: /metrics
      dns_sd_configs:
        - names: [gdb-growlerdb-node-headless.growlerdb.svc.cluster.local]
          type: A
          port: 9102
      relabel_configs:
        - { target_label: namespace, replacement: growlerdb }
```

Then restart the agent. The `namespace: growlerdb` label matters: the console health pill scopes to
GrowlerDB's own targets by namespace, so a **shared** Prometheus/Mimir scraping other apps
doesn't drag the pill to "Down". (`up{namespace="growlerdb"}` should then return your components.)

> The console UI bundle is baked into the image. After changing UI behaviour (or with a mutable `:dev`
> tag) set `image.pullPolicy: Always`, redeploy, and **hard-refresh** the browser to drop the cached SPA.

## 7. Cluster-specific

- **microk8s — resilience drills:** with `index.shards: 3` spread across the 3 nodes,
  exercise self-heal and honest degradation:
  - `kubectl -n growlerdb delete pod gdb-growlerdb-node-1` → that shard returns partial results, then
    the StatefulSet recovers it from its PV; the gateway hot-reloads as it re-registers.
  - Drain a node (`kubectl drain <node>`) → the PDB keeps ≥2 shards serving; search stays up (partial).
  - Kill Polaris (`kubectl -n growlerdb delete pod -l app=polaris`) → search continues, hydration/
    ingestion pause and resume when it returns (the persistent Postgres metastore means no catalog loss).
- **Hetzner — scale/load:** drive concurrent search load (the bench harness), watch the
  gateway HPA scale and the per-shard latency in Grafana (ServiceMonitor → Prometheus). Cold-tier +
  hydration throughput per the bench plan.

## Known limitations

- **One primary pod per shard** — a shard is briefly degraded (partial results) during pod loss/restart;
  zero-downtime per-shard *replicas* are future work (segment-shipping is single-shard; a Service-fronted
  replica set diverges under streaming writes). HA today = shards spread + PDBs + fast PV self-heal.
- The streaming **connector** is an external Spark job, not part of the chart.
