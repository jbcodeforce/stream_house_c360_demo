################################################################################
# Outputs
################################################################################
output "environment_id" {
  value = confluent_environment.env.id
}

output "kafka_id" {
  value = confluent_kafka_cluster.kcl.id
}

output "kafka_name" {
  value = confluent_kafka_cluster.kcl.display_name
}

output "sa_id" {
  value = confluent_service_account.kafka_mgr.id
}
output "sa_name" {
  value = confluent_service_account.kafka_mgr.display_name
}

output "api_key_name" {
  value = confluent_api_key.kcl-kafka-api-key.display_name
}

output "api_key_id" {
  value = confluent_api_key.kcl-kafka-api-key.id
}

output "api_key_secret" {
  value = confluent_api_key.kcl-kafka-api-key.secret
  sensitive = true
}


output "connector_id" {
  description = "ID of the Debezium PostgreSQL Source V2 managed connector"
  value       = confluent_connector.debezium_postgres.id
}

output "connector_status" {
  description = "Current status of the Debezium PostgreSQL Source V2 managed connector"
  value       = confluent_connector.debezium_postgres.status
}
