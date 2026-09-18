#!/usr/bin/env bash
# Provision a local OpenSearch embedding model for Atlas semantic search (dev).
# Ingest pipeline is created/updated by Atlas on startup (ensureIngestPipeline).
#
# Usage:
#   ./scripts/init-opensearch-semantic-local.sh
#   OPENSEARCH_URL=http://localhost:9200 PIPELINE_NAME=atlas-semantic-ingest EMBEDDING_DIMENSION=384 \
#     ./scripts/init-opensearch-semantic-local.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=init-opensearch-semantic-common.sh
source "${SCRIPT_DIR}/init-opensearch-semantic-common.sh"

DOCKER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
PIPELINE_NAME="${PIPELINE_NAME:-atlas-semantic-ingest}"
MODEL_NAME="${MODEL_NAME:-huggingface/sentence-transformers/paraphrase-MiniLM-L3-v2}"
MODEL_VERSION="${MODEL_VERSION:-1.0.1}"
EMBEDDING_DIMENSION="${EMBEDDING_DIMENSION:-384}"
BOOTSTRAP_ARTIFACT="${BOOTSTRAP_ARTIFACT:-${DOCKER_DIR}/config/.semantic-bootstrap-last.env}"

wait_for_opensearch "${OPENSEARCH_URL}"

echo "==> Registering and deploying local embedding model (${MODEL_NAME}) ..."
REGISTER_RESPONSE=$(curl -sf -X POST "${OPENSEARCH_URL}/_plugins/_ml/models/_register?deploy=true" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"${MODEL_NAME}\",\"version\":\"${MODEL_VERSION}\",\"model_format\":\"TORCH_SCRIPT\"}")

TASK_ID=$(echo "${REGISTER_RESPONSE}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task_id',''))")
if [ -z "${TASK_ID}" ]; then
  echo "Failed to register model: ${REGISTER_RESPONSE}"
  exit 1
fi

wait_for_ml_task "${OPENSEARCH_URL}" "${TASK_ID}" MODEL_ID

echo "==> Model deployed: ${MODEL_ID}"

write_bootstrap_artifact "${BOOTSTRAP_ARTIFACT}" "${MODEL_ID}" "${PIPELINE_NAME}" "${EMBEDDING_DIMENSION}"
print_atlas_config_guide "${MODEL_ID}" "${PIPELINE_NAME}" "${EMBEDDING_DIMENSION}" "${BOOTSTRAP_ARTIFACT}"
