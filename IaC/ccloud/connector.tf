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

    # ── Database connection ──────────────────────────────────────────────────
    "database.hostname" = local.rds_host
    "database.port"     = local.rds_port
    "database.user"     = local.rds_username
    "database.dbname"   = local.rds_database

    # TLS: matches rds.force_ssl = 1 enforced by the custom parameter group
    "database.sslmode" = "require"

    # ── CDC source configuration ─────────────────────────────────────────────
    # Logical decoding plugin — pgoutput is native to PostgreSQL 10+ and
    # the only plugin supported on RDS without installing extensions.
    "plugin.name" = "pgoutput"

    # Publication created by scripts/db/create_tables.py
    "publication.name" = var.cdc_publication_name

    # Replication slot the connector will create and manage
    "slot.name" = var.cdc_slot_name

    # Tables to capture (schema.table format)
    "table.include.list" = "public.customers,public.accounts,public.transactions"

    # ── Kafka topic routing ──────────────────────────────────────────────────
    # Topics will be: c360.public.customers, c360.public.accounts, c360.public.transactions
    # topic.prefix is what actually names the output topics for V2 connectors;
    # database.server.name is the separate Debezium logical server name.
    "database.server.name" = var.kafka_topic_prefix
    "topic.prefix"          = var.kafka_topic_prefix

    # ── Serialization ────────────────────────────────────────────────────────
    "output.data.format" = "AVRO"
    "output.key.format"  = "AVRO"

    # ── Parallelism ──────────────────────────────────────────────────────────
    # Debezium PostgreSQL source is single-threaded by design (WAL is ordered)
    "tasks.max" = "1"
  }
}
