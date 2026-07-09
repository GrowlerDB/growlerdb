# Provisions a Hetzner k3s cluster for the scale test: one k3s server + (node_count-1) agents on a
# private network, behind a firewall, with local-NVMe nodes. Parameterized + destroy-able (task-159).
# k3s is bootstrapped via cloud-init; the workload is then deployed with the Helm chart and driven by
# the benchmark harness (bench/scale/). See okf/quality/scale-test-plan.md.

locals {
  server_private_ip = "10.0.0.10"
  # Use an existing project key by name, else the one we upload. Servers reference this id.
  ssh_key_id = var.existing_ssh_key_name != "" ? data.hcloud_ssh_key.existing[0].id : hcloud_ssh_key.admin[0].id
}

# Shared join token for the k3s agents (generated, never committed; lives only in state).
resource "random_password" "k3s_token" {
  length  = 48
  special = false
}

# SSH key: reuse an existing project key by name (set existing_ssh_key_name — avoids Hetzner's "SSH
# key not unique" 409 when your public key is already uploaded), OR upload ssh_public_key. Exactly one
# is active; servers use local.ssh_key_id.
data "hcloud_ssh_key" "existing" {
  count = var.existing_ssh_key_name != "" ? 1 : 0
  name  = var.existing_ssh_key_name
}

resource "hcloud_ssh_key" "admin" {
  count      = var.existing_ssh_key_name == "" ? 1 : 0
  name       = "${var.name}-admin"
  public_key = var.ssh_public_key
}

resource "hcloud_network" "net" {
  name     = var.name
  ip_range = "10.0.0.0/16"
}

resource "hcloud_network_subnet" "subnet" {
  network_id   = hcloud_network.net.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = "10.0.0.0/24"
}

resource "hcloud_firewall" "fw" {
  name = var.name

  rule {
    description = "SSH (restrict admin_ssh_cidr for a real run)"
    direction   = "in"
    protocol    = "tcp"
    port        = "22"
    source_ips  = [var.admin_ssh_cidr]
  }

  rule {
    description = "k3s API for kubectl/helm from admin"
    direction   = "in"
    protocol    = "tcp"
    port        = "6443"
    source_ips  = [var.admin_ssh_cidr]
  }

  rule {
    description = "ICMP"
    direction   = "in"
    protocol    = "icmp"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }
}

# The k3s server (control plane). Fixed private IP so agents have a stable join address.
resource "hcloud_server" "server" {
  name         = "${var.name}-server"
  server_type  = var.server_type
  image        = "ubuntu-24.04"
  location     = var.location
  ssh_keys     = [local.ssh_key_id]
  firewall_ids = [hcloud_firewall.fw.id]
  labels = {
    role    = "k3s-server"
    cluster = var.name
    dataset = var.dataset
    shards  = tostring(var.shard_count)
  }

  user_data = templatefile("${path.module}/cloud-init/server.yaml.tftpl", {
    k3s_channel = var.k3s_channel
    k3s_token   = random_password.k3s_token.result
    private_ip  = local.server_private_ip
  })

  network {
    network_id = hcloud_network.net.id
    ip         = local.server_private_ip
  }

  depends_on = [hcloud_network_subnet.subnet]
}

# k3s agents (searcher/index nodes). node_count includes the server, so agents = node_count - 1.
resource "hcloud_server" "agent" {
  count        = var.node_count - 1
  name         = "${var.name}-agent-${count.index}"
  server_type  = var.server_type
  image        = "ubuntu-24.04"
  location     = var.location
  ssh_keys     = [local.ssh_key_id]
  firewall_ids = [hcloud_firewall.fw.id]
  labels = {
    role    = "k3s-agent"
    cluster = var.name
  }

  user_data = templatefile("${path.module}/cloud-init/agent.yaml.tftpl", {
    k3s_channel = var.k3s_channel
    k3s_token   = random_password.k3s_token.result
    server_url  = "https://${local.server_private_ip}:6443"
  })

  network {
    network_id = hcloud_network.net.id
  }

  depends_on = [hcloud_server.server]
}
