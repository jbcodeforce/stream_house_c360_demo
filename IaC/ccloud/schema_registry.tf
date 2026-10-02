# Schema Registry for j9r-env base infrastructure
# Schema Registry is auto-provisioned with the environment when stream_governance is enabled

data "confluent_schema_registry_cluster" "sr_essentials" {
  environment {
    id = confluent_environment.env.id
  }

  depends_on = [
    confluent_kafka_cluster.kcl
  ]
}

resource "confluent_api_key" "schema-registry-api-key" {
  display_name = "env-manager-schema-registry-api-key"
  description  = "Schema Registry API Key that is owned by 'env_mgr' service account"
  owner {
    id          = data.confluent_service_account.env_mgr.id
    api_version = data.confluent_service_account.env_mgr.api_version
    kind        = data.confluent_service_account.env_mgr.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr_essentials.id
    api_version = data.confluent_schema_registry_cluster.sr_essentials.api_version
    kind        = data.confluent_schema_registry_cluster.sr_essentials.kind

    environment {
      id = confluent_environment.env.id
    }
  }

  depends_on = [
    data.confluent_service_account.env_mgr
  ]
}

