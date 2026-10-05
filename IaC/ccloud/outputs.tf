# ------------------------------------------------------
# Environment
# ------------------------------------------------------
output "environment_id" {
  value = confluent_environment.env.id
}
output "env_display_name" {
  description = "Environment display name (used as Flink catalog)"
  value       = confluent_environment.env.display_name
}

# ------------------------------------------------------
# Kafka Cluster
# ------------------------------------------------------

output "kafka_id" {
  value = confluent_kafka_cluster.kcl.id
}

output "kafka_name" {
  value = confluent_kafka_cluster.kcl.display_name
}

output "kafka_bootstrap_endpoint" {
  description = "Kafka cluster bootstrap endpoint"
  value       = confluent_kafka_cluster.kcl.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  description = "Kafka cluster REST endpoint"
  value       = confluent_kafka_cluster.kcl.rest_endpoint
}

output "kafka_api_key_id" {
  description = "Kafka API key ID (owned by env-manager)"
  value       = confluent_api_key.kcl-kafka-api-key.id
}

output "kafka_api_key_secret" {
  description = "Kafka API key secret"
  value       = confluent_api_key.kcl-kafka-api-key.secret
  sensitive   = true
}

output "sa_id" {
  value = confluent_service_account.kafka_mgr.id
}
output "sa_name" {
  value = confluent_service_account.kafka_mgr.display_name
}

# ------------------------------------------------------
# Flink
# ------------------------------------------------------
output "flink_compute_pool_id" {
  description = "ID of the Flink compute pool"
  value       = confluent_flink_compute_pool.pool.id
}


# ------------------------------------------------------
# Schema registry
# ------------------------------------------------------
output "schema_registry_id" {
  description = "Schema Registry cluster ID"
  value       = data.confluent_schema_registry_cluster.sr_essentials.id
}

output "schema_registry_rest_endpoint" {
  description = "Schema Registry REST endpoint"
  value       = data.confluent_schema_registry_cluster.sr_essentials.rest_endpoint
}

output "schema_registry_api_key_id" {
  description = "Schema Registry API key ID"
  value       = confluent_api_key.schema-registry-api-key.id
}

output "schema_registry_api_key_secret" {
  description = "Schema Registry API key secret"
  value       = confluent_api_key.schema-registry-api-key.secret
  sensitive   = true
}