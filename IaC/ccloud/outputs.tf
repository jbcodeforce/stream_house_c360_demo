################################################################################
# Outputs
################################################################################

output "connector_id" {
  description = "ID of the Debezium PostgreSQL Source V2 managed connector"
  value       = confluent_connector.debezium_postgres.id
}

output "connector_status" {
  description = "Current status of the Debezium PostgreSQL Source V2 managed connector"
  value       = confluent_connector.debezium_postgres.status
}
