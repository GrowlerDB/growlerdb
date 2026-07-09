terraform {
  required_version = ">= 1.6"
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.49"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# Token comes from the HCLOUD_TOKEN env var (preferred) or -var hcloud_token=...; never commit it.
provider "hcloud" {
  token = var.hcloud_token
}
