# Scale-test IaC (Hetzner k3s)

Repeatable provisioning for the GrowlerDB scale test (task-159) — Terraform + the `hcloud` provider.
Spec: [`okf/quality/scale-test-plan.md`](../../okf/quality/scale-test-plan.md). Budget-pinned to
**≤ $200/run** (default 4× `ccx33`, local NVMe); `destroy` returns the account to baseline.

## What it provisions

A private-network k3s cluster behind a firewall: **1 k3s server + `node_count-1` agents**
(cloud-init installs k3s; agents join over the private network). Everything the run scales on —
`node_count`, `server_type`, `shard_count`, `dataset` — is a variable, so a run is resized or
reproduced by editing `terraform.tfvars`.

## Use

```sh
export HCLOUD_TOKEN=...                     # never committed
cp terraform.tfvars.example terraform.tfvars   # set ssh_public_key + admin_ssh_cidr (restrict it!)

terraform init
terraform apply

# Pull the kubeconfig (see the kubeconfig_hint output), then deploy + run:
helm install growlerdb ../helm/growlerdb -f ../helm/growlerdb/values-hetzner.yaml \
  --set index.shards=$(terraform output -raw run_parameters | ...)   # shard_count
python ../../bench/scale/harness.py run "$(terraform output ... dataset)"

terraform destroy                            # cost-guarded teardown; record the run cost
```

## Notes

- **Secrets/state stay out of git** (`.gitignore`): `*.tfstate` (holds the k3s token), real
  `terraform.tfvars`, `.terraform/`, and the pulled `kubeconfig.yaml`.
- **Budget:** the default 4-node `ccx33` cluster is sized for the $200/run cap. A larger, dedicated-
  NVMe (multi-TB) run is a separate, higher-budget effort.
- **`shard_count` / `dataset`** don't shape the infra directly — they parameterize the Helm deploy and
  the [benchmark harness](../../bench/scale/README.md) and are surfaced as the `run_parameters` output
  so the whole run is reproducible from one `tfvars`.
- Hetzner cloud prices change periodically; reconfirm rates before a run.

## Hetzner gotchas (learned the hard way, now handled in cloud-init)

- **Dedicated-vCPU (`ccx`) quota** on a fresh account can be as low as **8 cores (1 node)**. If the
  planned `ccx33` is rejected with "dedicated core limit exceeded", request a limit increase in the
  Hetzner console, or use a shared-vCPU type (e.g. `cpx42`, 8 vCPU / 16 GB / 320 GB NVMe) as an
  interim — shared vCPU means noisier perf numbers. Check availability per location:
  `curl -H "Authorization: Bearer $HCLOUD_TOKEN" https://api.hetzner.cloud/v1/server_types`.
- **Private NIC timing:** the private network attaches shortly after boot; cloud-init **actively
  brings `enp7s0` up + requests DHCP with retries** (bounded 300s deadline, then loud failure) before
  installing k3s (task-221). This self-heals the boot-timing straggler that previously came up with the
  NIC DOWN and hung cloud-init forever — no manual `ip link set enp7s0 up; dhcpcd -4 enp7s0` rescue
  should be needed. If a node still won't join, check its `cloud-init` log for the FATAL NIC message.
- **Node IP + flannel:** agents join with `--node-ip <private>` and **`--flannel-iface enp7s0`** so
  flannel's VXLAN MTU is derived from the private NIC (1450→1400). Without this, flannel sizes MTU
  from the public `eth0` (1500) and cross-node pod/DNS traffic silently drops.
- **API cert:** the server adds its public IP as a `--tls-san` so the pulled kubeconfig works over
  the internet.
