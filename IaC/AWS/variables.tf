variable "aws_region_primary" {
  type        = string
  description = "AWS region for primary resources (AWS West default: us-west-2)"
  default     = "us-west-2"
}

variable "environment" {
  type        = string
  description = "Deployment environment name"
  default     = "demo"
}

variable "project_name" {
  type        = string
  description = "Project name identifier"
  default     = "c360"
}

variable "vpc_id" {
  type        = string
  description = "Existing VPC ID to deploy resources into (optional, uses vpc_name_tag filter if empty)"
  default     = null
}

variable "vpc_name_tag" {
  type        = string
  description = "Name tag filter for existing VPC if vpc_id is not provided"
  default     = null
}

variable "subnet_ids" {
  type        = list(string)
  description = "Explicit list of existing subnet IDs for the DB subnet group (optional, queries VPC subnets if empty)"
  default     = null
}

variable "db_name" {
  type        = string
  description = "Name of the default PostgreSQL database to create"
  default     = "c360db"
}

variable "db_username" {
  type        = string
  description = "Master username for PostgreSQL RDS instance"
  default     = "dbadmin"
}

variable "db_instance_class" {
  type        = string
  description = "Compute and memory capacity of the RDS instance"
  default     = "db.t4g.micro"
}

variable "db_allocated_storage" {
  type        = number
  description = "Allocated storage size in gigabytes (GB)"
  default     = 20
}

variable "db_max_allocated_storage" {
  type        = number
  description = "Upper limit in GB for storage autoscaling (0 disables autoscaling)"
  default     = 100
}

variable "publicly_accessible" {
  type        = bool
  description = "Whether the RDS instance should be publicly accessible (requires public subnets)"
  default     = true
}

variable "confluent_cloud_api_key" {
  type        = string
  description = "Confluent Cloud API Key (or set via CONFLUENT_CLOUD_API_KEY environment variable)"
  default     = null
}

variable "confluent_cloud_api_secret" {
  type        = string
  description = "Confluent Cloud API Secret (or set via CONFLUENT_CLOUD_API_SECRET environment variable)"
  sensitive   = true
  default     = null
}

variable "confluent_region" {
  type        = string
  description = "Confluent Cloud region for Connect / Kafka egress IPs (defaults to aws_region_primary)"
  default     = null
}

variable "db_allowed_cidr_blocks" {
  type        = list(string)
  description = "Additional CIDR blocks allowed to access PostgreSQL (e.g. workstation IP/32)"
  default     = []
}

variable "my_ip_addr" {
  type        = string
}