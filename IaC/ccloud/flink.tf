# ------------------------------------------------------
# Flink Compute Pool
# Cloud/region mirror the Kafka cluster (see kafka.tf) so Flink runs
# alongside the cluster in the same environment.
# ------------------------------------------------------

resource "confluent_flink_compute_pool" "pool" {
  display_name = "${var.prefix}-flink-pool"
  cloud        = var.cloud_provider
  region       = var.aws_region_primary
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.env.id
  }

  depends_on = [
    confluent_kafka_cluster.kcl
  ]
}

# ------------------------------------------------------
# Flink API key (environment + region scoped)
#
# A Confluent Flink API key is bound to one (environment, region) pair. dbt's
# confluent adapter authenticates its Flink statements with this key, so the key
# MUST live in the same environment as confluent_environment.env — otherwise the
# Flink REST API returns 404 resource_not_found. Creating it here (rather than by
# hand) keeps it in lockstep with the environment id whenever Terraform rotates
# the environment. set_env_from_tf.sh exports it as FLINK_API_KEY/SECRET.
# ------------------------------------------------------

data "confluent_organization" "org" {}

data "confluent_flink_region" "main" {
  cloud  = var.cloud_provider
  region = var.aws_region_primary
}

# Service account that owns the Flink API key and is the principal the statements
# run as. It needs EnvironmentAdmin (to create the Kafka-backed tables dbt
# materializes) and Assigner on itself (so it may submit statements that run as
# its own principal).
resource "confluent_service_account" "flink_runner" {
  display_name = "${var.prefix}-flink-runner"
  description  = "Owns the Flink API key and runs dbt Flink statements"
}

resource "confluent_role_binding" "flink-runner-env-admin" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.env.resource_name
}

resource "confluent_role_binding" "flink-runner-assigner" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "Assigner"
  crn_pattern = "${data.confluent_organization.org.resource_name}/service-account=${confluent_service_account.flink_runner.id}"
}

resource "confluent_api_key" "flink-api-key" {
  display_name = "${var.prefix}-flink-api-key"
  description  = "Flink API Key scoped to ${var.prefix}-env / ${var.aws_region_primary}"

  owner {
    id          = confluent_service_account.flink_runner.id
    api_version = confluent_service_account.flink_runner.api_version
    kind        = confluent_service_account.flink_runner.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind

    environment {
      id = confluent_environment.env.id
    }
  }

  depends_on = [
    confluent_role_binding.flink-runner-env-admin,
    confluent_role_binding.flink-runner-assigner,
  ]
}
