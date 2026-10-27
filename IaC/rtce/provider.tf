terraform {
  required_version = ">= 1.16.3"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.88.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}
