#!/usr/bin/env bash
# Smoke-test semantic search against UDF retail seed data (PDF use cases).
set -euo pipefail

ATLAS_URL="${ATLAS_URL:-http://localhost:21000}"
ATLAS_USER="${ATLAS_USER:-admin}"
ATLAS_PASS="${ATLAS_PASS:-atlasR0cks!}"
AUTH=(-u "${ATLAS_USER}:${ATLAS_PASS}")
HDR=(-H "Accept: application/json" -H "Content-Type: application/json")

semantic_search() {
  local query="$1"
  echo ""
  echo "--- Query: ${query}"
  curl -sS "${AUTH[@]}" "${HDR[@]}" -X POST "${ATLAS_URL}/api/atlas/v2/search/semantic" \
    -d "$(jq -n --arg q "${query}" '{query: $q, limit: 5, excludeDeletedEntities: true}')" \
    | jq '{query: .queryText, count: (.entities | length), top: [.entities[:3][] | {name: .displayText, guid: .guid, score: .score}]}'
}

echo "==> UDF retail semantic search smoke tests"

semantic_search "customer lifetime value"
semantic_search "why customers stop buying"
semantic_search "trusted revenue reporting gold certified"
semantic_search "personal customer email data"
semantic_search "approved for Q3 finance deck"
semantic_search "source of truth orders owner"
semantic_search "campaign returns marketing ROI"

echo ""
echo "==> Repair log tail (if container running):"
docker logs atlas-semantic-indexer 2>&1 | tail -5 || true
