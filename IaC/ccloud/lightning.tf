# RTCE / Lightning Query API — service account + Global API key
#
# The Confluent Lightning (SQL) query endpoint used by the analytics
# "Customer lookup via RTCE" section requires a *Global* resource API key — one
# that authenticates the whole organization. A cluster-, environment-, Flink-,
# or Metrics-scoped key is rejected with 401. We create a dedicated service
# account, grant it read access to the data it queries (the Kafka cluster that
# backs fct_c360_profile and the Schema Registry subjects for Avro decoding),
# and mint a Global key owned by it. Populate CONFLUENT_GLOBAL_KEY /
# CONFLUENT_GLOBAL_SECRET (see outputs.tf) from this key.
#

resource "confluent_service_account" "rtce_lightning" {
  display_name = "${var.prefix}-rtce-lightning"
  description  = "Queries fct_c360_profile via the RTCE Lightning Query API"
}

# Read access to the Kafka cluster (the lightning table's data). CloudClusterAdmin
# mirrors the reference demo; it can be narrowed to DeveloperRead on the specific
# topics once the exact privilege the Lightning query needs is confirmed.
resource "confluent_role_binding" "rtce_lightning_cluster_admin" {
  principal   = "User:${confluent_service_account.rtce_lightning.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.kcl.rbac_crn
}

# Read access to the Schema Registry subjects, so Avro-encoded rows decode.
resource "confluent_role_binding" "rtce_lightning_sr_owner" {
  principal   = "User:${confluent_service_account.rtce_lightning.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr_essentials.resource_name}/subject=*"
}

# The Global API key — kind "Global" authenticates the whole organization, which
# is what the Lightning Query API requires.
resource "confluent_api_key" "rtce_lightning" {
  display_name = "${var.prefix}-rtce-lightning-key"
  description  = "Global API key for the RTCE Lightning Query API (CONFLUENT_GLOBAL_KEY)"

  owner {
    id          = confluent_service_account.rtce_lightning.id
    api_version = confluent_service_account.rtce_lightning.api_version
    kind        = confluent_service_account.rtce_lightning.kind
  }

  managed_resource {
    id          = "global"
    api_version = "global/v1"
    kind        = "Global"
  }

  depends_on = [
    confluent_role_binding.rtce_lightning_cluster_admin,
    confluent_role_binding.rtce_lightning_sr_owner,
  ]
}
