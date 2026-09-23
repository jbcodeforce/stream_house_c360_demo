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
- [`IaC/data.tf`](IaC/data.tf): Data sources (Availability Zones, existing VPC discovery by ID/tag/default, existing subnets lookup).
- [`IaC/aws.tf`](IaC/aws.tf): AWS resource declarations (DB Subnet Group, Security Groups, KMS, Secrets Manager, RDS).
- [`IaC/outputs.tf`](IaC/outputs.tf): Exported attributes (VPC IDs, RDS endpoints, Security Group IDs, Secrets Manager ARN).

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
   - **Network Security**: Security Group restricting ingress on port 5432 strictly to the existing VPC CIDR (and any optional CIDRs provided in `db_allowed_cidr_blocks`). Public accessibility is disabled (`publicly_accessible = false`).

3. **Database & CDC Readiness**:
   - Engine: PostgreSQL 16 (`16.4`).
   - Custom parameter group enforces TLS (`rds.force_ssl = 1`) and enables `rds.logical_replication = 1` to support real-time Change Data Capture (Debezium / Confluent Kafka Connect / Flink).
   - Automated backups (7-day retention) and CloudWatch log exports (`postgresql`, `upgrade`).

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
