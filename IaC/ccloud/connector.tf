################################################################################
# Debezium PostgreSQL Source V2 — Managed Connector
################################################################################

resource "confluent_connector" "debezium_postgres" {
  environment {
    id = confluent_environment.env.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.kcl.id
  }

  # Sensitive config is stored separately so Terraform keeps the password
  # out of the plan diff and marks the block as sensitive in state.
  config_sensitive = {
    "database.password" = local.rds_password
    "kafka.api.secret"  = confluent_api_key.kcl-kafka-api-key.secret
  }

  # All non-sensitive connector properties
  config_nonsensitive = {
    # ── Connector identity ──────────────────────────────────────────────────
    "connector.class" = "PostgresCdcSourceV2"
    "name"            = var.connector_name

    # ── Kafka authentication ─────────────────────────────────────────────────
    # Must be the cluster-scoped key (paired with its secret in config_sensitive),
    # not the org-level Cloud API key used for the Terraform provider.
    "kafka.auth.mode" = "KAFKA_API_KEY"
    "kafka.api.key"   = confluent_api_key.kcl-kafka-api-key.id

    # Required by the managed connector runtime to locate and authenticate
    # the Kafka cluster. endpoint comes from the cluster bootstrap_endpoint
    # (which already carries the SASL_SSL:// scheme); region and cloud
    # are derived from the variables already used for cluster provisioning.
    "kafka.endpoint"    = confluent_kafka_cluster.kcl.bootstrap_endpoint
    "kafka.region"      = var.aws_region_primary
    "cloud.environment" = "prod"
    "cloud.provider"    = lower(var.cloud_provider)

    # ── Database connection ──────────────────────────────────────────────────
    "database.hostname" = local.rds_host
    "database.port"     = local.rds_port
    "database.user"     = local.rds_username
    "database.dbname"   = local.rds_database

    # TLS: matches rds.force_ssl = 1 enforced by the custom parameter group
    "database.sslmode" = "require"

    # ── CDC source configuration ─────────────────────────────────────────────
    # Tables to capture (schema.table format)
    # Order matches the deployed connector config.
    "table.include.list" = "public.accounts,public.customers,public.transactions"

    # ── Kafka topic routing ──────────────────────────────────────────────────
    # Topics will be: <prefix>.public.accounts, <prefix>.public.customers, etc.
    # The deployed connector uses "cdc" as the prefix; that is the default.
    "topic.prefix" = var.kafka_topic_prefix

    # ── Serialization ────────────────────────────────────────────────────────
    "output.data.format" = "AVRO"
    "output.key.format"  = "AVRO"

    # ── Parallelism ──────────────────────────────────────────────────────────
    # Debezium PostgreSQL source is single-threaded by design (WAL is ordered)
    "tasks.max" = "1"
  }
}
