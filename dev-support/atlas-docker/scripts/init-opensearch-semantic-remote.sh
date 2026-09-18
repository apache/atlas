#!/usr/bin/env bash
# Provision a remote OpenSearch embedding model for Atlas semantic search (dev).
# Ingest pipeline is created/updated by Atlas on startup (ensureIngestPipeline).
#
# Use one of these paths:
#
# 1) Model already deployed in OpenSearch:
#    MODEL_ID=xxx EMBEDDING_DIMENSION=1536 ./scripts/init-opensearch-semantic-remote.sh
#
# 2) Connector already exists:
#    CONNECTOR_ID=xxx REMOTE_MODEL_NAME="my-embedding" EMBEDDING_DIMENSION=1536 \
#      ./scripts/init-opensearch-semantic-remote.sh
#
# 3) Create connector from OpenSearch blueprint JSON (any provider — OpenAI, Bedrock, Cohere, etc.):
#    CONNECTOR_JSON=config/connectors/openai-embedding.example.json \
#    REMOTE_MODEL_NAME="OpenAI text-embedding-3-small" EMBEDDING_DIMENSION=1536 \
#      ./scripts/init-opensearch-semantic-remote.sh
#
# CONNECTOR_JSON must be the request body for POST /_plugins/_ml/connectors/_create
# (see OpenSearch connector blueprints). Credentials belong in that JSON or env-expanded before run.
#
# Optional: OPENSEARCH_URL, PIPELINE_NAME (default atlas-semantic-ingest)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=init-opensearch-semantic-common.sh
source "${SCRIPT_DIR}/init-opensearch-semantic-common.sh"

DOCKER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
PIPELINE_NAME="${PIPELINE_NAME:-atlas-semantic-ingest}"
EMBEDDING_DIMENSION="${EMBEDDING_DIMENSION:?Set EMBEDDING_DIMENSION to match your model output}"
REMOTE_MODEL_NAME="${REMOTE_MODEL_NAME:-Atlas remote embedding model}"
BOOTSTRAP_ARTIFACT="${BOOTSTRAP_ARTIFACT:-${DOCKER_DIR}/config/.semantic-bootstrap-last.env}"
MODEL_ID="${MODEL_ID:-}"
CONNECTOR_ID="${CONNECTOR_ID:-}"
CONNECTOR_JSON="${CONNECTOR_JSON:-}"

wait_for_opensearch "${OPENSEARCH_URL}"

if [ -z "${MODEL_ID}" ]; then
  if [ -n "${CONNECTOR_JSON}" ]; then
    if [ ! -f "${CONNECTOR_JSON}" ]; then
      echo "CONNECTOR_JSON file not found: ${CONNECTOR_JSON}"
      exit 1
    fi

    echo "==> Creating ML connector from ${CONNECTOR_JSON} ..."
    CONNECTOR_RESPONSE=$(curl -sf -X POST "${OPENSEARCH_URL}/_plugins/_ml/connectors/_create" \
      -H "Content-Type: application/json" \
      -d @"${CONNECTOR_JSON}")

    CONNECTOR_ID=$(echo "${CONNECTOR_RESPONSE}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('connector_id',''))")
    if [ -z "${CONNECTOR_ID}" ]; then
      echo "Failed to create connector: ${CONNECTOR_RESPONSE}"
      exit 1
    fi
    echo "==> Connector created: ${CONNECTOR_ID}"
  fi

  if [ -z "${CONNECTOR_ID}" ]; then
    cat <<EOF
Provide one of:
  MODEL_ID=<deployed OpenSearch ML model id>
  CONNECTOR_ID=<existing connector id>
  CONNECTOR_JSON=<path to connector create JSON>

Also set EMBEDDING_DIMENSION to the model output size.
EOF
    exit 1
  fi

  REGISTER_RESPONSE=$(register_remote_model "${OPENSEARCH_URL}" "${CONNECTOR_ID}" "${REMOTE_MODEL_NAME}")
  TASK_ID=$(echo "${REGISTER_RESPONSE}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task_id',''))")
  if [ -z "${TASK_ID}" ]; then
    echo "Failed to register remote model: ${REGISTER_RESPONSE}"
    exit 1
  fi

  wait_for_ml_task "${OPENSEARCH_URL}" "${TASK_ID}" MODEL_ID
  echo "==> Remote model deployed: ${MODEL_ID}"
else
  echo "==> Using existing model id: ${MODEL_ID}"
fi

write_bootstrap_artifact "${BOOTSTRAP_ARTIFACT}" "${MODEL_ID}" "${PIPELINE_NAME}" "${EMBEDDING_DIMENSION}"
print_atlas_config_guide "${MODEL_ID}" "${PIPELINE_NAME}" "${EMBEDDING_DIMENSION}" "${BOOTSTRAP_ARTIFACT}"
