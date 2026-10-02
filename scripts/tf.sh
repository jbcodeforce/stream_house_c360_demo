#!/usr/bin/env bash
#
# Run Terraform in IaC/ccloud with the right credentials loaded.
#
# Usage:
#   AWS_PROFILE=nonprod-administrator-829250931565 scripts/tf.sh plan
#   AWS_PROFILE=nonprod-administrator-829250931565 scripts/tf.sh apply
#
# NOTE: the AWS profile must resolve to the account that owns the RDS secret
#   (var.rds_secret_arn, account 829250931565). Beware similarly-named profiles
#   that assume a role in a DIFFERENT account.
#
# Why this wrapper exists:
#   1. The Confluent provider here needs ONLY cloud_api_key / cloud_api_secret.
#      Sourcing the full ~/.confluent/.env also exports the provider's *native*
#      env vars for Schema Registry and Flink, but as INCOMPLETE sets
#      (SCHEMA_REGISTRY_ENDPOINT instead of SCHEMA_REGISTRY_REST_ENDPOINT;
#      PRINCIPAL_ID instead of FLINK_PRINCIPAL_ID). The provider then fails its
#      "all-or-none" validation. Those creds also point at a different,
#      already-deployed environment, so they must not reach this provider at
#      all. We strip them below.
#   2. AWS: the SSO profile is not reliably resolved by the AWS provider via
#      AWS_PROFILE (the provider's Go SDK throws ExpiredToken even when the CLI
#      works), so we export temporary static creds from it instead. We also
#      refresh the SSO token automatically when it has expired.
set -euo pipefail

AWS_PROFILE_NAME="${AWS_PROFILE:-default}"
CONFLUENT_ENV_FILE="${CONFLUENT_ENV_FILE:-$HOME/.confluent/.env}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CCLOUD_DIR="$(cd "${SCRIPT_DIR}/../IaC/ccloud" && pwd)"

if [[ ! -f "${CONFLUENT_ENV_FILE}" ]]; then
  echo "ERROR: Confluent env file not found at ${CONFLUENT_ENV_FILE}" >&2
  exit 1
fi

# ── AWS: temporary static credentials from the SSO profile ───────────────────
# Refresh the SSO token first if it is missing or expired, then export temporary
# static creds (bypasses the AWS provider's unreliable SSO resolution).
if ! aws sts get-caller-identity --profile "${AWS_PROFILE_NAME}" >/dev/null 2>&1; then
  echo "AWS SSO token for profile '${AWS_PROFILE_NAME}' is missing or expired; logging in..." >&2
  aws sso login --profile "${AWS_PROFILE_NAME}"
fi
eval "$(aws configure export-credentials --profile "${AWS_PROFILE_NAME}" --format env)"

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
