# Kafka Cluster and Service Accounts for j9r-env base infrastructure

resource "confluent_kafka_cluster" "kcl" {
  display_name = "${var.prefix}-kafka"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud_provider
  region       = var.aws_region_primary
  standard {}

  environment {
    id = confluent_environment.env.id
  }

  depends_on = [
    confluent_environment.env
  ]
}



# ------------------------------------------------------
# Service account for Kafka cluster administration
# Service accounts are top-level organization resources. They become "scoped" 
# to an environment when assigned a confluent_role_binding where crn_pattern references data.confluent_environment.target_env.resource_name.
# ------------------------------------------------------

resource "confluent_service_account" "kafka_mgr" {
  display_name = "${var.prefix}-kafka-mgr"
  description  = "Service account to manage 'kcl' Kafka cluster"
}

resource "confluent_role_binding" "kafka-mgr-kcluster-admin" {
  principal   = "User:${confluent_service_account.kafka_mgr.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.kcl.rbac_crn
}

# ------------------------------------------------------
# API Key
# ------------------------------------------------------
resource "confluent_api_key" "kcl-kafka-api-key" {
  # display_name = "${var.prefix}-kafka-api-key"
  display_name = "standard-kafka-api-key"
  description  = "Kafka API Key for 'standard' cluster"
  owner {
    id          = confluent_service_account.kafka_mgr.id
    api_version = confluent_service_account.kafka_mgr.api_version
    kind        = confluent_service_account.kafka_mgr.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.kcl.id
    api_version = confluent_kafka_cluster.kcl.api_version
    kind        = confluent_kafka_cluster.kcl.kind

    environment {
      id = confluent_environment.env.id
    }
  }

  depends_on = [
    confluent_kafka_cluster.kcl,
    confluent_service_account.kafka_mgr,
    confluent_role_binding.kafka-mgr-kcluster-admin
  ]
}
