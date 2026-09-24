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
# AWS Configuration
################################################################################

variable "aws_region_primary" {
  type        = string
  description = "AWS region where RDS and Secrets Manager resources reside"
  default     = "us-west-2"
}

################################################################################
# Confluent Cloud Resource References
################################################################################

variable "confluent_environment_id" {
  type        = string
  description = "ID of the existing Confluent Cloud environment (e.g. env-abc123)"
}

variable "confluent_kafka_cluster_id" {
  type        = string
  description = "ID of the existing Confluent Cloud Kafka cluster (e.g. lkc-abc123)"
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

variable "cdc_publication_name" {
  type        = string
  description = "PostgreSQL logical replication publication name (must already exist on the RDS instance)"
  default     = "c360_cdc_publication"
}

variable "cdc_slot_name" {
  type        = string
  description = "PostgreSQL replication slot name the connector will create and use"
  default     = "c360_debezium_slot"
}

variable "kafka_topic_prefix" {
  type        = string
  description = "Prefix for Kafka topic names produced by the connector (topics will be <prefix>.public.<table>)"
  default     = "c360"
}
