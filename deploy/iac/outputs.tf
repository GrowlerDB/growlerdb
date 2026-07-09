output "server_ip" {
  description = "Public IP of the k3s server. Fetch the kubeconfig from here."
  value       = hcloud_server.server.ipv4_address
}

output "agent_ips" {
  description = "Public IPs of the k3s agent nodes."
  value       = hcloud_server.agent[*].ipv4_address
}

output "kubeconfig_hint" {
  description = "How to pull the kubeconfig for kubectl/helm."
  value       = "ssh root@${hcloud_server.server.ipv4_address} 'cat /etc/rancher/k3s/k3s.yaml' | sed 's/127.0.0.1/${hcloud_server.server.ipv4_address}/' > kubeconfig.yaml"
}

output "run_parameters" {
  description = "The run this cluster was provisioned for (feeds the Helm deploy + harness)."
  value = {
    nodes       = var.node_count
    server_type = var.server_type
    shard_count = var.shard_count
    dataset     = var.dataset
    location    = var.location
  }
}
