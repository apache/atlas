#!/usr/bin/env bash
# Start Atlas with Postgres + OpenSearch and the standalone Semantic Indexer.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${DOCKER_DIR}"

export ATLAS_BACKEND=postgres
export ATLAS_INDEX_BACKEND=opensearch
export COMPOSE_PROFILES=opensearch-index
export BUILD_HOST_SRC="${BUILD_HOST_SRC:-true}"
export COMPOSE_PULL_POLICY="${COMPOSE_PULL_POLICY:-never}"

COMPOSE_FILES=(
  -f docker-compose.atlas.yml
  -f docker-compose.atlas-postgres.yml
  -f docker-compose.atlas-semantic.yml
)

echo "==> Building Atlas + slim Semantic Indexer images ..."
echo "    Requires dist/apache-atlas-\${ATLAS_VERSION}-server.tar.gz"
echo "    and dist/apache-atlas-\${ATLAS_VERSION}-semantic-indexer.tar.gz"
docker compose "${COMPOSE_FILES[@]}" build atlas atlas-semantic-indexer

if [ "${ATLAS_SEMANTIC_SKIP_INIT:-false}" != "true" ]; then
  echo "==> Starting OpenSearch ..."
  docker compose "${COMPOSE_FILES[@]}" up -d --wait atlas-index

  echo "==> Bootstrapping OpenSearch ML model (local MiniLM) ..."
  INIT_ARGS=()
  if [ "${ATLAS_SEMANTIC_FORCE_INIT:-false}" = "true" ]; then
    INIT_ARGS+=(--force)
  fi
  OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}" \
    "${SCRIPT_DIR}/init-opensearch-semantic-local.sh" ${INIT_ARGS[@]+"${INIT_ARGS[@]}"}
fi

echo "==> Starting Postgres + OpenSearch + Atlas + Semantic Indexer ..."
docker compose "${COMPOSE_FILES[@]}" up -d --wait

echo ""
echo "Atlas UI:     http://localhost:21000/n3/index.html"
echo "Credentials:  admin / atlasR0cks!"
echo ""
echo "Indexer health:              curl -s http://localhost:8089/health | jq"
echo "Semantic Indexer INFO logs:  docker logs -f atlas-semantic-indexer 2>&1 | grep --line-buffered 'org.apache.atlas'"
echo "Semantic REST DEBUG logs:    docker logs -f atlas 2>&1 | grep --line-buffered 'org.apache.atlas.semantic'"
echo "Backfill (optional): docker exec atlas-semantic-indexer /opt/atlas-semantic-indexer/bin/atlas_semantic_repair.sh"
