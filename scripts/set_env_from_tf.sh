# ─────────────────────────────────────────────────────────────────────────────
# set_env_from_tf.sh
# Export Confluent Cloud + AWS RDS settings (incl. DATABASE_URL) from Terraform
# outputs into the current shell, so run_dev.sh and friends pick them up.
#
# MUST be sourced (exports must survive into your shell):
#   source scripts/set_env_from_tf.sh
# ─────────────────────────────────────────────────────────────────────────────

# Guard: refuse to run as an executed subshell, where exports would be lost.
(return 0 2>/dev/null) || {
  echo "ERROR: source this script so the exports reach your shell:" >&2
  echo "       source scripts/set_env_from_tf.sh" >&2
  exit 1
}

# Resolve this script's own path. zsh arrays are 1-indexed so ${BASH_SOURCE[0]}
# is empty there — use zsh's %x prompt escape instead; bash uses BASH_SOURCE.
if [ -n "${ZSH_VERSION:-}" ]; then
  SCRIPT_SRC="${(%):-%x}"
else
  SCRIPT_SRC="${BASH_SOURCE[0]}"
fi

# Resolve the IaC directories regardless of where the script is sourced from.
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_SRC}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CCLOUD_DIR="${REPO_ROOT}/IaC/ccloud"
AWS_DIR="${REPO_ROOT}/IaC/AWS"

if [[ ! -d "${CCLOUD_DIR}" ]]; then
  echo "ERROR: cannot find IaC/ccloud at ${CCLOUD_DIR}" >&2
  return 1
fi
if [[ ! -d "${AWS_DIR}" ]]; then
  echo "ERROR: cannot find IaC/AWS at ${AWS_DIR}" >&2
  return 1
fi

# Use 'terraform -chdir' instead of cd so sourcing does not move the caller's shell.
export ENVIRONMENT_ID=$(terraform -chdir="${CCLOUD_DIR}" output -raw environment_id)
export CC_REGION=$(terraform -chdir="${CCLOUD_DIR}" output -raw cc_region)
export CC_PROVIDER=$(terraform -chdir="${CCLOUD_DIR}" output -raw cloud_provider)
export KAFKA_CLUSTER_NAME=$(terraform -chdir="${CCLOUD_DIR}" output -raw kafka_name)
export KAFKA_API_KEY=$(terraform -chdir="${CCLOUD_DIR}" output -raw kafka_api_key_id)
export KAFKA_API_SECRET=$(terraform -chdir="${CCLOUD_DIR}" output -raw kafka_api_key_secret)
export KAFKA_BOOTSTRAP_SERVERS=$(terraform -chdir="${CCLOUD_DIR}" output -raw kafka_bootstrap_endpoint)
export FLINK_COMPUTE_POOL_ID=$(terraform -chdir="${CCLOUD_DIR}" output -raw flink_compute_pool_id)
export SCHEMA_REGISTRY_KEY=$(terraform -chdir="${CCLOUD_DIR}" output -raw schema_registry_api_key_id)
export SCHEMA_REGISTRY_SECRET=$(terraform -chdir="${CCLOUD_DIR}" output -raw schema_registry_api_key_secret)
echo "----------------"
export RDSHOST=$(terraform -chdir="${AWS_DIR}" output -raw rds_address)
export SECRET_ARN=$(terraform -chdir="${AWS_DIR}" output -raw secrets_manager_secret_arn)
export RDSPORT=$(terraform -chdir="${AWS_DIR}" output -raw rds_port)
export AWS_REGION=$(echo "$SECRET_ARN" | cut -d: -f4)
export DB_PASSWORD=$(aws secretsmanager get-secret-value --secret-id ${SECRET_ARN} --query SecretString --region ${AWS_REGION} --output text | jq -r .password)
export DBNAME=$(terraform -chdir="${AWS_DIR}" output -raw rds_database_name)
export DATABASE_URL="postgresql://dbadmin:${DB_PASSWORD}@${RDSHOST}:${RDSPORT}/${DBNAME}?sslmode=require"

# This path targets RDS, where the managed Debezium CDC connector publishes to
# the cdc.public.* topics. The backend must NOT also dual-write Kafka events
# (that would double-publish), so hard-disable app emission regardless of the
# runtime kafka_produce_enabled toggle.
export CDC_CONNECTOR_ENABLED=true

# Kept for debugging — note this prints DB_PASSWORD and DATABASE_URL in clear.
printenv
