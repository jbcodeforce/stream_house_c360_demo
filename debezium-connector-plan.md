# Plan: Debezium PostgreSQL Source V2 Connector in IaC/ccloud/

## Overview

Add a Confluent Cloud managed Debezium PostgreSQL Source V2 connector to `IaC/ccloud/` that:
- Looks up an existing Confluent Cloud environment and Kafka cluster via data sources
- Reads RDS PostgreSQL connection credentials from AWS Secrets Manager (ARN passed as variable)
- Captures CDC events for the `customers`, `accounts`, and `transactions` tables using the existing `c360_cdc_publication` publication and a `c360_debezium_slot` replication slot
- Emits Avro-serialized Kafka records to topics prefixed `c360.public.<table>`
- Reuses the existing Confluent Cloud API key from the provider (no new service account)

The `IaC/ccloud/` module already has `provider.tf` with the `confluent`, `aws`, and `random` providers and references `var.confluent_cloud_api_key`, `var.confluent_cloud_api_secret`, and `var.aws_region_primary`.

---

## Sub-Tasks

---

### Sub-Task 1 — Create `IaC/ccloud/variables.tf`

**Status:** [ ] pending

**Intent:**  
Define all input variables needed by the ccloud module: Confluent Cloud IDs, AWS region, the Secrets Manager ARN for RDS credentials, and CDC-specific configuration that callers may want to override.

**Expected Outcomes:**
- `IaC/ccloud/variables.tf` exists with typed, described variables and sensible defaults
- No credentials or IDs are hardcoded anywhere

**Todo List:**
1. Declare `confluent_cloud_api_key` / `confluent_cloud_api_secret` (sensitive) — already used by the provider
2. Declare `confluent_environment_id` (string, required) — ID of the existing Confluent Cloud environment
3. Declare `confluent_kafka_cluster_id` (string, required) — ID of the existing Kafka cluster
4. Declare `aws_region_primary` (string, default `"us-west-2"`) — used by the AWS provider
5. Declare `rds_secret_arn` (string, required, sensitive) — ARN of the Secrets Manager secret from AWS Terraform output `secrets_manager_secret_arn`
6. Declare `connector_name` (string, default `"c360-debezium-postgres-source"`) — Confluent connector display name
7. Declare `cdc_publication_name` (string, default `"c360_cdc_publication"`)
8. Declare `cdc_slot_name` (string, default `"c360_debezium_slot"`)
9. Declare `kafka_topic_prefix` (string, default `"c360"`)

**Relevant Context:**
- `IaC/ccloud/provider.tf` — already references `var.confluent_cloud_api_key`, `var.confluent_cloud_api_secret`, `var.aws_region_primary`
- `IaC/AWS/variables.tf` — follow the same variable style (type, description, default pattern)
- `IaC/AWS/outputs.tf` — `secrets_manager_secret_arn` is the output that callers will pass as `rds_secret_arn`

---

### Sub-Task 2 — Create `IaC/ccloud/data.tf`

**Status:** [ ] pending

**Intent:**  
Look up the existing Confluent Cloud environment and Kafka cluster by ID, and read the RDS connection details from AWS Secrets Manager so the connector configuration can reference them without hardcoding.

**Expected Outcomes:**
- `IaC/ccloud/data.tf` exists with three data sources
- RDS hostname, port, username, password, and database name are all available as local values derived from the Secrets Manager JSON

**Todo List:**
1. Add `data "confluent_environment" "main"` using `var.confluent_environment_id`
2. Add `data "confluent_kafka_cluster" "main"` using `var.confluent_kafka_cluster_id` and `environment { id = data.confluent_environment.main.id }`
3. Add `data "aws_secretsmanager_secret_version" "rds_creds"` using `secret_id = var.rds_secret_arn`
4. Add a `locals` block that JSON-decodes the secret string into individual fields: `rds_host`, `rds_port`, `rds_username`, `rds_password`, `rds_database`

**Relevant Context:**
- `IaC/AWS/aws.tf` — the secret JSON contains keys `host`, `port`, `username`, `password`, `database`
- Confluent provider `~> 2.86` supports `confluent_environment` and `confluent_kafka_cluster` data sources

---

### Sub-Task 3 — Create `IaC/ccloud/connector.tf`

**Status:** [ ] pending

**Intent:**  
Define the `confluent_connector` resource for the Debezium PostgreSQL Source V2 managed connector, wiring in the data source outputs for cluster, environment, and RDS credentials.

**Expected Outcomes:**
- `IaC/ccloud/connector.tf` exists with a single `confluent_connector` resource
- The connector is configured to capture `c360.public.customers`, `c360.public.accounts`, and `c360.public.transactions`
- Avro serialization is set for both keys and values
- The connector reuses the Confluent Cloud API key from the provider

**Todo List:**
1. Define `resource "confluent_connector" "debezium_postgres"` with:
   - `display_name = var.connector_name`
   - `environment { id = data.confluent_environment.main.id }`
   - `kafka_cluster { id = data.confluent_kafka_cluster.main.id }`
2. Set the `config_sensitive` block with `database.password = local.rds_password`
3. Set the `config_nonsensitive` block with all required Debezium PostgreSQL Source V2 settings:
   - `connector.class = "PostgresDebeziumSourceV2"` (Confluent managed plugin name)
   - `name = var.connector_name`
   - `kafka.auth.mode = "KAFKA_API_KEY"`
   - `kafka.api.key` and `kafka.api.secret` from `var.confluent_cloud_api_key` / `var.confluent_cloud_api_secret`
   - `database.hostname = local.rds_host`
   - `database.port = local.rds_port`
   - `database.user = local.rds_username`
   - `database.dbname = local.rds_database`
   - `database.server.name = var.kafka_topic_prefix`
   - `table.include.list = "public.customers,public.accounts,public.transactions"`
   - `publication.name = var.cdc_publication_name`
   - `slot.name = var.cdc_slot_name`
   - `output.data.format = "AVRO"`
   - `output.key.format = "AVRO"`
   - `tasks.max = "1"`
   - `plugin.name = "pgoutput"` (required for RDS logical replication)
   - `database.sslmode = "require"` (TLS enforced on RDS)

**Relevant Context:**
- `IaC/AWS/aws.tf` — `rds.force_ssl = 1` mandates SSL; `database.sslmode = "require"` matches
- `IaC/AWS/aws.tf` — `rds.logical_replication = 1` is already enabled; `plugin.name = "pgoutput"` is the RDS-compatible logical decoding plugin
- `scripts/db/create_tables.py` — confirms publication name `c360_cdc_publication` and tables `customers`, `accounts`, `transactions` in schema `public`
- Confluent provider `~> 2.86` — `confluent_connector` resource with `config_sensitive` / `config_nonsensitive` split

---

### Sub-Task 4 — Create `IaC/ccloud/outputs.tf`

**Status:** [ ] pending

**Intent:**  
Export key connector attributes so callers can reference the connector ID and status for observability or downstream Terraform modules.

**Expected Outcomes:**
- `IaC/ccloud/outputs.tf` exists with two outputs

**Todo List:**
1. Output `connector_id` — `confluent_connector.debezium_postgres.id`
2. Output `connector_status` — `confluent_connector.debezium_postgres.status`

**Relevant Context:**
- Follow naming and tagging style from `IaC/AWS/outputs.tf`

---

## Implementation Notes

- The `IaC/ccloud/` module is **not** a child module — it is a standalone root module with its own provider configuration. Callers set variables via a `terraform.tfvars` file or `-var` flags.
- The AWS provider in `IaC/ccloud/provider.tf` is already declared; only add an `aws` data source for Secrets Manager — no new provider block needed.
- `terraform.tfvars.example` should be created alongside the variables file to document required inputs without exposing real values.
