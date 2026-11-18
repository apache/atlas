#!/usr/bin/env bash
# Provision a local OpenSearch embedding model for Atlas semantic search (dev).
# The ingest pipeline and the embedding mapping are created by the Semantic Indexer when it starts.
#
# Reuses a DEPLOYED model in OpenSearch with the same MODEL_NAME (search + GET verify).
# Otherwise registers a new model. Pass --force to always register a new model.
# Atlas finds the model by name (atlas.semantic.model.name), so nothing has to be copied to its configuration.
#
# Usage:
#   ./scripts/init-opensearch-semantic-local.sh
#   ./scripts/init-opensearch-semantic-local.sh --force
#   OPENSEARCH_URL=http://localhost:9200 EMBEDDING_DIMENSION=384 ./scripts/init-opensearch-semantic-local.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=init-opensearch-semantic-common.sh
source "${SCRIPT_DIR}/init-opensearch-semantic-common.sh"

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
MODEL_NAME="${MODEL_NAME:-huggingface/sentence-transformers/paraphrase-MiniLM-L3-v2}"
MODEL_VERSION="${MODEL_VERSION:-1.0.1}"
EMBEDDING_DIMENSION="${EMBEDDING_DIMENSION:-384}"
FORCE_REGISTER="${FORCE_REGISTER:-false}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--force]

Ensures a local OpenSearch ML embedding model named ${MODEL_NAME} is deployed.

Options:
  -f, --force   Always register and deploy a new model. Atlas keeps using the oldest deployed model
                with the name until that one is undeployed.

Default (no --force):
  - Search OpenSearch for a DEPLOYED model named ${MODEL_NAME}, verify with
    GET /_plugins/_ml/models/{id}, and reuse it when found.
  - Otherwise register and deploy a new model.

Environment:
  FORCE_REGISTER=true              Same as --force
  ATLAS_SEMANTIC_FORCE_INIT=true   Used by run-atlas-semantic.sh to pass --force
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -f|--force)
      FORCE_REGISTER=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

wait_for_opensearch "${OPENSEARCH_URL}"

# the docker cluster is a single data node: without this, OpenSearch refuses to deploy the model
curl -sf -X PUT "${OPENSEARCH_URL}/_cluster/settings" -H "Content-Type: application/json" \
  -d '{"persistent":{"plugins.ml_commons.only_run_on_ml_node":false}}' >/dev/null

resolve_local_embedding_model_id \
  "${OPENSEARCH_URL}" \
  "${MODEL_NAME}" \
  "${MODEL_VERSION}" \
  "${FORCE_REGISTER}" \
  MODEL_ID

print_atlas_config_guide "${MODEL_NAME}" "${EMBEDDING_DIMENSION}"
