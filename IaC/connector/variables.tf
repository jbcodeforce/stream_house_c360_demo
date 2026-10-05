################################################################################
# Confluent Cloud Provider Credentials
################################################################################

variable "confluent_cloud_api_key" {
  type        = string
  description = "Confluent Cloud API Key for provider authentication (or set via CONFLUENT_CLOUD_API_KEY env var)"
  default     = null
}

variable "confluent_cloud_api_secret" {
  type        = string
  description = "Confluent Cloud API Secret for provider authentication (or set via CONFLUENT_CLOUD_API_SECRET env var)"
  sensitive   = true
  default     = null
}

################################################################################
# Cloud / region
################################################################################

variable "cloud_provider" {
  type        = string
  description = "Cloud provider of the Kafka cluster (used for the connector cloud.provider setting)"
  default     = "AWS"
}

variable "aws_region_primary" {
  type        = string
  description = "AWS region where RDS and Secrets Manager resources reside (also the connector kafka.region)"
  default     = "us-west-2"
}

################################################################################
# Core stack state
################################################################################

variable "core_state_path" {
  type        = string
  description = "Path to the IaC/ccloud Terraform state file whose outputs this stack consumes"
  default     = "../ccloud/terraform.tfstate"
}

################################################################################
# AWS Secrets Manager
################################################################################

variable "rds_secret_arn" {
  type        = string
  description = "ARN of the AWS Secrets Manager secret holding RDS credentials (maps to AWS IaC output: secrets_manager_secret_arn)"
  sensitive   = true
}

################################################################################
# Connector Configuration
################################################################################

variable "connector_name" {
  type        = string
  description = "Display name for the Confluent Cloud managed Debezium PostgreSQL Source V2 connector"
  default     = "c360-debezium-postgres-source"
}

variable "kafka_topic_prefix" {
  type        = string
  description = "Prefix for Kafka topic names produced by the connector (topics will be <prefix>.public.<table>)"
  default     = "cdc"
}
