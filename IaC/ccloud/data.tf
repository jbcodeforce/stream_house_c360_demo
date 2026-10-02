################################################################################
# Data Sources
################################################################################

# Read the RDS credentials JSON from AWS Secrets Manager
# The ARN is provided by the AWS IaC module's output: secrets_manager_secret_arn
data "aws_secretsmanager_secret_version" "rds_creds" {
  secret_id = var.rds_secret_arn
}

data "confluent_service_account" "env_mgr" {
   id = var.cc_sa_env_mgr
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
}
