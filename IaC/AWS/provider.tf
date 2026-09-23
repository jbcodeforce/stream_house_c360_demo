terraform {
  required_version = ">= 1.16.3"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.86"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region_primary
}