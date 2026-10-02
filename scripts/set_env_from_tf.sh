

#!/usr/bin/env bash
set -euo pipefail

# Resolve the IaC/ccloud directory regardless of whether the script is run
# from the repo root or from the scripts/ subdirectory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CCLOUD_DIR="${REPO_ROOT}/IaC/ccloud"

if [[ ! -d "${CCLOUD_DIR}" ]]; then
  echo "ERROR: cannot find IaC/ccloud at ${CCLOUD_DIR}" >&2
  exit 1
fi

cd "${CCLOUD_DIR}"

export ENVIRONMENT_ID=$(terraform output -raw environment_id)
export KAFKA_API_KEY=$(terraform output -raw kafka_api_key_id)
export KAFKA_API_SECRET=$(terraform output -raw kafka_api_key_secret)
export KAFKA_BOOTSTRAP_SERVERS=$(terraform output -raw kafka_bootstrap_endpoint)
echo "----------------"
printenv
