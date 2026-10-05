#!/usr/bin/env bash
#
# Run Terraform in IaC/ccloud — the core Confluent Cloud stack.
#
# Usage:
#   scripts/tf.sh plan
#   scripts/tf.sh apply
#
# This core stack provisions ONLY Confluent Cloud resources (environment, Kafka
# cluster, Flink compute pool, Schema Registry, service account + API keys) and
# needs NO AWS credentials. The managed Kafka connector and its AWS RDS
# dependency live in IaC/connector/ — use scripts/tf_connector.sh for that.
#
# Why this wrapper exists:
#   The Confluent provider here needs ONLY cloud_api_key / cloud_api_secret.
#   Sourcing the full ~/.confluent/.env also exports the provider's *native*
#   env vars for Schema Registry and Flink, but as INCOMPLETE sets
#   (SCHEMA_REGISTRY_ENDPOINT instead of SCHEMA_REGISTRY_REST_ENDPOINT;
#   PRINCIPAL_ID instead of FLINK_PRINCIPAL_ID). The provider then fails its
#   "all-or-none" validation. Those creds also point at a different,
#   already-deployed environment, so they must not reach this provider at
#   all. We strip them below.
set -euo pipefail

CONFLUENT_ENV_FILE="${CONFLUENT_ENV_FILE:-$HOME/.confluent/.env}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CCLOUD_DIR="$(cd "${SCRIPT_DIR}/../IaC/ccloud" && pwd)"

if [[ ! -f "${CONFLUENT_ENV_FILE}" ]]; then
  echo "ERROR: Confluent env file not found at ${CONFLUENT_ENV_FILE}" >&2
  exit 1
fi

# ── Confluent: load cloud API credentials from the reference env file ─────────
set -a
# shellcheck disable=SC1090
source "${CONFLUENT_ENV_FILE}"
set +a

# Strip the Confluent-provider native env vars that would partially (and
# incorrectly) configure the provider for Schema Registry / Flink.
unset SCHEMA_REGISTRY_API_KEY SCHEMA_REGISTRY_API_SECRET SCHEMA_REGISTRY_ID \
      SCHEMA_REGISTRY_ENDPOINT SCHEMA_REGISTRY_REST_ENDPOINT \
      FLINK_API_KEY FLINK_API_SECRET FLINK_REST_ENDPOINT FLINK_REST_END_POINT \
      FLINK_COMPUTE_POOL_ID FLINK_PRINCIPAL_ID PRINCIPAL_ID COMPUTE_POOL_ID \
      ORGANIZATION_ID ORG_ID ENVIRONMENT_ID ENV_ID

cd "${CCLOUD_DIR}"
exec terraform "$@"
