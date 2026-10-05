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
# Cloud / region (used by the Kafka cluster and Flink compute pool)
################################################################################
variable "cloud_provider" {
  type        = string
  description = "Cloud provider for the Kafka cluster and Flink compute pool"
  default     = "AWS"
}

variable "aws_region_primary" {
  type        = string
  description = "Cloud region for the Kafka cluster and Flink compute pool"
  default     = "us-west-2"
}

variable "prefix" {
  type        = string
  default     = "j9r-jtbd1"
  description = "prefix for environment"
}

variable "cc_sa_env_mgr" {
  type        = string
  description = "Existing Confluent Cloud service account ID used as environment manager"
}

################################################################################
# Flink
################################################################################

variable "flink_max_cfu" {
  type        = number
  description = "Maximum CFUs (Confluent Flink Units) for the Flink compute pool"
  default     = 5
}
