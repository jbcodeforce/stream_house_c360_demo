################################################################################
# Data Sources
################################################################################

# Pre-existing Confluent Cloud service account used as the owner of the
# Schema Registry API key (see schema_registry.tf).
data "confluent_service_account" "env_mgr" {
  id = var.cc_sa_env_mgr
}
