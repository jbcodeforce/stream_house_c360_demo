################################################################################
# Data Sources
################################################################################

# Core Confluent Cloud resources (environment, Kafka cluster, Kafka API key)
# are provisioned by the IaC/ccloud stack. Read its outputs from local state
# instead of hand-copying ids/keys.
data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = var.core_state_path
  }
}

# Read the RDS credentials JSON from AWS Secrets Manager.
# The ARN is produced by the IaC/AWS stack's output: secrets_manager_secret_arn
data "aws_secretsmanager_secret_version" "rds_creds" {
  secret_id = var.rds_secret_arn
}

################################################################################
# Locals: decode the Secrets Manager JSON into individual fields
# Secret shape (from IaC/AWS/aws.tf):
#   { engine, host, port, username, password, database }
################################################################################

locals {
  rds_creds    = jsondecode(data.aws_secretsmanager_secret_version.rds_creds.secret_string)
  rds_host     = local.rds_creds["host"]
  rds_port     = tostring(local.rds_creds["port"])
  rds_username = local.rds_creds["username"]
  rds_password = local.rds_creds["password"]
  rds_database = local.rds_creds["database"]

  # Core stack outputs
  environment_id           = data.terraform_remote_state.core.outputs.environment_id
  kafka_cluster_id         = data.terraform_remote_state.core.outputs.kafka_id
  kafka_bootstrap_endpoint = data.terraform_remote_state.core.outputs.kafka_bootstrap_endpoint
  kafka_api_key_id         = data.terraform_remote_state.core.outputs.kafka_api_key_id
  kafka_api_key_secret     = data.terraform_remote_state.core.outputs.kafka_api_key_secret
}
