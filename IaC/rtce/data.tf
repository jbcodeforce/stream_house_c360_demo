################################################################################
# Data Sources
#
# The environment and Kafka cluster are provisioned by the IaC/ccloud stack.
# Read its outputs from local state (same pattern as IaC/connector) rather than
# hand-copying ids. The cluster/SR data sources below expose rbac_crn /
# resource_name, needed for the role bindings.
################################################################################

data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = var.core_state_path
  }
}

locals {
  environment_id   = data.terraform_remote_state.core.outputs.environment_id
  kafka_cluster_id = data.terraform_remote_state.core.outputs.kafka_id
}

data "confluent_kafka_cluster" "this" {
  id = local.kafka_cluster_id
  environment {
    id = local.environment_id
  }
}

data "confluent_schema_registry_cluster" "this" {
  environment {
    id = local.environment_id
  }
}
