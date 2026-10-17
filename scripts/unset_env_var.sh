#!/usr/bin/env bash
# Usage: source scripts/unset_env_var.sh
# Must be sourced (not executed) to affect the current shell's environment.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "ERROR: This script must be sourced, not executed." >&2
  echo "  Run: source ${0}" >&2
  exit 1
fi

unset SCHEMA_REGISTRY_ID SCHEMA_REGISTRY_ENDPOINT SCHEMA_REGISTRY_API_KEY SCHEMA_REGISTRY_API_SECRET
unset ENVIRONMENT_ID ORGANIZATION_ID
unset FLINK_API_KEY FLINK_API_SECRET FLINK_COMPUTE_POOL_ID FLINK_REST_ENDPOINT
unset TF_VAR_schema_registry_id TF_VAR_schema_registry_rest_endpoint TF_VAR_schema_registry_api_key TF_VAR_schema_registry_api_secret 