#!/usr/bin/env bash
#
# Run Terraform in IaC/AWS — the AWS stack (PostgreSQL RDS 17, VPC/subnet
# lookups, security group, KMS, Secrets Manager secret).
#
# Usage:
#   scripts/tf_aws.sh plan
#   scripts/tf_aws.sh apply
#   AWS_PROFILE=nonprod-administrator-829250931565 scripts/tf_aws.sh plan
#
# Prerequisites:
#   - An AWS profile that resolves to the account that will own the RDS instance
#     (defaults to the `default` profile; override with AWS_PROFILE).
#   - CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET exported in your
#     shell: the security group ingress is built from the Confluent Cloud
#     Connect/Kafka egress IPs, so the Confluent provider needs cloud_api_key /
#     cloud_api_secret even though this stack is mostly AWS.
#
# Why this wrapper exists:
#   1. AWS: the AWS provider's Go SDK does NOT reliably use an SSO profile via
#      AWS_PROFILE. Shared-profile files written by SSO tooling often contain
#      temporary (ASIA...) access keys WITHOUT aws_session_token; the CLI still
#      works from its SSO cache, but Terraform rejects the incomplete set and
#      falls through to "No valid credential sources found". We refresh the SSO
#      token if needed and export the COMPLETE temporary credential triple
#      (key + secret + session token) into the environment instead.
#   2. Confluent: if the shell also has the provider's native Schema Registry /
#      Flink env vars set as INCOMPLETE sets, they trip the provider's
#      "all-or-none" validation. We strip them for the Terraform subprocess
#      below (see tf.sh) as a defensive measure.
set -euo pipefail

AWS_PROFILE_NAME="${AWS_PROFILE:-default}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_DIR="$(cd "${SCRIPT_DIR}/../IaC/AWS" && pwd)"

# ── Confluent: cloud API credentials must be present in the environment ───────
if [[ -z "${CONFLUENT_CLOUD_API_KEY:-}" || -z "${CONFLUENT_CLOUD_API_SECRET:-}" ]]; then
  echo "ERROR: CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET must be exported" >&2
  echo "       (the security group ingress reads Confluent Cloud egress IPs)." >&2
  exit 1
fi

# ── AWS: complete temporary credentials from the (SSO) profile ───────────────
# Refresh the SSO token first if it is missing or expired, then export the full
# key + secret + session-token triple (bypasses the AWS provider's unreliable
# SSO resolution and the missing-session-token problem).
if ! aws sts get-caller-identity --profile "${AWS_PROFILE_NAME}" >/dev/null 2>&1; then
  echo "AWS SSO token for profile '${AWS_PROFILE_NAME}' is missing or expired; logging in..." >&2
  aws sso login --profile "${AWS_PROFILE_NAME}"
fi
eval "$(aws configure export-credentials --profile "${AWS_PROFILE_NAME}" --format env)"

# Guard against a profile that resolves to the WRONG AWS account. The RDS / VPC /
# Secrets Manager resources live in one specific account, but a similarly-named
# SSO profile can silently assume a role in a different one (observed:
# 829250931565_nonprod-administrator -> 898188061957). Checks the exported creds
# (what Terraform will actually use). Set EXPECTED_AWS_ACCOUNT_ID= (empty) to skip.
EXPECTED_AWS_ACCOUNT_ID="${EXPECTED_AWS_ACCOUNT_ID-829250931565}"
if [[ -n "${EXPECTED_AWS_ACCOUNT_ID}" ]]; then
  ACTUAL_AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  if [[ "${ACTUAL_AWS_ACCOUNT_ID}" != "${EXPECTED_AWS_ACCOUNT_ID}" ]]; then
    echo "ERROR: AWS credentials resolve to account '${ACTUAL_AWS_ACCOUNT_ID:-<unknown>}', expected '${EXPECTED_AWS_ACCOUNT_ID}'." >&2
    echo "       Profile '${AWS_PROFILE_NAME}' points at the wrong account." >&2
    echo "       Use a profile in ${EXPECTED_AWS_ACCOUNT_ID}, e.g.: AWS_PROFILE=default scripts/tf_aws.sh $*" >&2
    exit 1
  fi
fi

# Defensively strip any native Confluent-provider env vars that would partially
# (and incorrectly) configure the provider for Schema Registry / Flink. Only
# affects the Terraform subprocess, not your shell.
unset SCHEMA_REGISTRY_API_KEY SCHEMA_REGISTRY_API_SECRET SCHEMA_REGISTRY_ID \
      SCHEMA_REGISTRY_ENDPOINT SCHEMA_REGISTRY_REST_ENDPOINT \
      FLINK_API_KEY FLINK_API_SECRET FLINK_REST_ENDPOINT FLINK_REST_END_POINT \
      FLINK_COMPUTE_POOL_ID FLINK_PRINCIPAL_ID PRINCIPAL_ID COMPUTE_POOL_ID \
      ORGANIZATION_ID ORG_ID ENVIRONMENT_ID ENV_ID

cd "${AWS_DIR}"
exec terraform "$@"
