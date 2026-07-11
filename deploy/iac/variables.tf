# Scale-test cluster inputs. Everything the run scales on is a variable here, so a run is
# reproduced or resized by changing tfvars — see okf/quality/scale-test-plan.md.

variable "hcloud_token" {
  description = "Hetzner Cloud API token. Prefer the HCLOUD_TOKEN env var; keep it out of git."
  type        = string
  sensitive   = true
  default     = null
}

variable "name" {
  description = "Cluster name prefix for all resources."
  type        = string
  default     = "growlerdb-scale"
}

variable "location" {
  description = "Hetzner location (EU: nbg1/fsn1/hel1; US: ash/hil). Egress is ~free within region."
  type        = string
  default     = "nbg1"
}

variable "node_count" {
  description = "Total nodes (1 k3s server + node_count-1 agents). Budget-pinned default: 4 (see the $200/run cap)."
  type        = number
  default     = 4
}

variable "server_type" {
  description = "Hetzner server type. Default ccx33 = dedicated vCPU + local NVMe, the pinned budget config."
  type        = string
  default     = "ccx33"
}

# The following are not consumed by the infra directly — they parameterize the *run* (the Helm deploy
# and the benchmark harness) from one place, and are surfaced as outputs so the whole run is
# reproducible from this tfvars.

variable "shard_count" {
  description = "GrowlerDB index shard count for the run (consumed by the Helm deploy: values-hetzner)."
  type        = number
  default     = 6
}

variable "dataset" {
  description = "Benchmark workload for the run (consumed by the harness). Default http_logs; drop-in: wikipedia, msmarco, synthetic."
  type        = string
  default     = "http_logs"
}

variable "ssh_public_key" {
  description = "SSH public key (contents) to upload + authorize on every node. Leave empty and set existing_ssh_key_name to reuse a key already in the project."
  type        = string
  default     = ""
}

variable "existing_ssh_key_name" {
  description = "Name of an SSH key ALREADY in the Hetzner project to use instead of uploading ssh_public_key. Set this when your public key is already uploaded — otherwise `terraform apply` fails with 'SSH key not unique' (409). Empty = upload ssh_public_key."
  type        = string
  default     = ""
}

variable "admin_ssh_cidr" {
  description = "CIDR allowed to SSH in. DEFAULT IS OPEN — set to your admin IP/32 for a real run."
  type        = string
  default     = "0.0.0.0/0"
}

variable "k3s_channel" {
  description = "k3s install channel (stable/latest or a pinned version)."
  type        = string
  default     = "stable"
}
