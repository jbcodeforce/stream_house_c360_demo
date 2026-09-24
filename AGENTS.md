# Agent Guidelines and Architecture Decisions

This document outlines the architecture, decisions, security practices, and infrastructure standards applied across this repository.

---

## 1. Project Context

**Repository**: `stream_house_c360_demo`  
**Purpose**: Customer 360 streaming data pipeline integrating operational databases (PostgreSQL, DB2) with event streaming, Change Data Capture (CDC), and modern lakehouse analytical sinks.

---

## 2. Infrastructure as Code (IaC) Layout

Terraform files are organized under the [`IaC/`](IaC/) directory with clear separation of concerns:

- [`IaC/provider.tf`](IaC/provider.tf): Required Terraform versions, provider definitions (AWS, Confluent, Random), and provider configurations.
- [`IaC/variables.tf`](IaC/variables.tf): All input variables, type declarations, descriptions, and sensible defaults (targeting AWS West / `us-west-2`).
- [`IaC/AWS/provider.tf`](IaC/AWS/provider.tf): Required Terraform versions, provider definitions (AWS, Confluent, Random), and provider configurations.
- [`IaC/AWS/variables.tf`](IaC/AWS/variables.tf): All input variables, type declarations, descriptions, and sensible defaults (targeting AWS West / `us-west-2`).
- [`IaC/AWS/data.tf`](IaC/AWS/data.tf): Data sources (Availability Zones, existing VPC, subnets, Confluent Cloud egress IPs).
- [`IaC/AWS/aws.tf`](IaC/AWS/aws.tf): AWS resource declarations (DB Subnet Group, Security Groups, KMS, Secrets Manager, RDS).
- [`IaC/AWS/outputs.tf`](IaC/AWS/outputs.tf): Exported attributes (VPC IDs, RDS endpoints, Security Group IDs, Secrets Manager ARN, Confluent egress IPs).

---

## 3. Completed Infrastructure Components

### PostgreSQL RDS Instance (AWS West)

1. **Existing Network Integration**:
   - Reuses an existing VPC discovered dynamically via `data.aws_vpc.existing` (configurable by `vpc_id` or `vpc_name_tag`, or defaulting to the default VPC).
   - Reuses existing subnets via `data.aws_subnets.existing` or explicit `subnet_ids` list.
   - `aws_db_subnet_group` links the discovered subnets for RDS placement.

2. **Security & Cryptography**:
   - **KMS Encryption**: Dedicated Customer-Managed Key (`aws_kms_key`) with annual key rotation enabled for storage at rest and performance insights.
   - **Secrets Management**: Credentials automatically generated via `random_password` (24 chars) and written directly to AWS Secrets Manager (`aws_secretsmanager_secret`), eliminating plain-text secrets in git.
   - **Network Security**: Security Group restricting ingress on port 5432 strictly to specified CIDRs (Confluent Cloud NAT Egress IPs, workstation IP/32, and VPC CIDR). In-transit encryption (`rds.force_ssl = 1`) is mandatory.

3. **Database & CDC Readiness**:
   - Engine: PostgreSQL 17 (parameter group family `postgres17`).
   - Custom parameter group enforces TLS (`rds.force_ssl = 1`) and enables `rds.logical_replication = 1` to support real-time Change Data Capture (Debezium / Confluent Kafka Connect / Flink).
   - Automated backups (7-day retention) and CloudWatch log exports (`postgresql`, `upgrade`).

### Customer 360 Schema Bootstrap (scripts/db/)

Python scripts to create and seed the PostgreSQL schema live under [`scripts/db/`](scripts/db/).
They are managed as a **uv project** (`pyproject.toml` + `uv.lock`).

- [`scripts/db/create_tables.py`](scripts/db/create_tables.py): Creates `customers`, `accounts`, `transactions` tables, indexes, `updated_at` triggers, and the `c360_cdc_publication` logical replication publication.
- [`scripts/db/seed_data.py`](scripts/db/seed_data.py): Inserts realistic synthetic records (Faker-generated) — configurable volume via `--customers N`.

Run order:
```bash
cd scripts/db
uv sync
uv run python create_tables.py --secret-arn <arn>
uv run python seed_data.py     --secret-arn <arn> --customers 100
```

### Local Testing (scripts/local-tests/)

Scripts for running services locally during development live under [`scripts/local-tests/`](scripts/local-tests/).

- [`scripts/local-tests/start_pg.sh`](scripts/local-tests/start_pg.sh): Starts a PostgreSQL 17 container via the **Apple Container CLI** (`container`) and optionally runs the schema + seed scripts against it.

```bash
# Start container only
./scripts/local-tests/start_pg.sh

# Start + create tables + seed 100 customers in one step
./scripts/local-tests/start_pg.sh --seed --customers 100

# Tear down when done
./scripts/local-tests/start_pg.sh --stop

# Wipe and restart fresh (full reset)
./scripts/local-tests/start_pg.sh --reset --seed
```

Environment overrides: `PG_PASSWORD` (default: `localdevonly`), `PG_PORT` (default: `5432`).

---

## 4. Best Practices for Future Agents & Contributors

When extending this repository, adhere to the following principles:

### A. Security & Compliance
- **No Hardcoded Credentials**: Never commit passwords, tokens, or API keys. Always use AWS Secrets Manager, HashiCorp Vault, or environment variables.
- **Principle of Least Privilege**: Ingress security group rules must target exact subnet CIDRs or security group references. Never bind databases or internal services to `0.0.0.0/0`.
- **Encryption by Default**: All persistent data stores (RDS, EBS, S3) must use KMS customer-managed keys (CMK) with `enable_key_rotation = true`.
- **Enforce TLS**: In-transit encryption (TLS 1.2+) is mandatory for all network communication and database clients.

### B. Terraform Standards
- **File Structure**:
  - `variables.tf`: Inputs and default configurations.
  - `data.tf`: External lookups and discovery (`aws_vpc`, `aws_subnets`, `aws_availability_zones`).
  - `outputs.tf`: Values intended for downstream consumers.
  - `aws.tf` / resource files: Pure resource definitions without inlining variables or outputs.
- **Resource Naming & Tagging**: Always tag resources with `Name`, `Environment`, and `Project` matching the standard `${var.project_name}-${var.environment}-*` convention.
- **Explicit Dependencies**: Use direct attribute references (e.g. `data.aws_vpc.existing.id`, `aws_kms_key.rds_key.arn`) to let Terraform build a deterministic DAG (Directed Acyclic Graph).

### C. CDC & Streaming Integration
- Any operational database added (PostgreSQL, DB2, MySQL) must be configured with CDC prerequisites:
  - Logical replication / WAL retention enabled.
  - User permissions granted for replication slots and publication creation.

### D. Python Tooling
- **All Python scripts in this repository use [uv](https://docs.astral.sh/uv/) as the package manager and runner.** Do not use `pip install`, `pipenv`, or `poetry`.
- Every Python script directory must contain a `pyproject.toml` (dependencies) and a committed `uv.lock` (pinned versions).
- Run scripts with `uv run python <script>.py` — uv handles the virtualenv automatically.
- Add new packages with `uv add <package>`; never edit `pyproject.toml` dependencies manually.
- Python version minimum: **3.11**. Always declare `requires-python = ">=3.11"` in `pyproject.toml`.
