#!/usr/bin/env bash
# Smoke-test semantic search against UDF retail seed data (PDF use cases Q1–Q7).
set -euo pipefail

ATLAS_URL="${ATLAS_URL:-http://localhost:21000}"
ATLAS_USER="${ATLAS_USER:-admin}"
ATLAS_PASS="${ATLAS_PASS:-atlasR0cks!}"
AUTH=(-u "${ATLAS_USER}:${ATLAS_PASS}")
HDR=(-H "Accept: application/json" -H "Content-Type: application/json")

semantic_search() {
  local label="$1"
  local query="$2"
  echo ""
  echo "--- ${label}: ${query}"
  curl -sS "${AUTH[@]}" "${HDR[@]}" -X POST "${ATLAS_URL}/api/atlas/v2/search/semantic" \
    -d "$(jq -n --arg q "${query}" '{query: $q, limit: 5, excludeDeletedEntities: true}')" \
    | jq '(.similarityScores // {}) as $scores | {query: .queryText, count: (.entities | length), top: [.entities[:3][] | {name: .displayText, guid: .guid, score: $scores[.guid]}]}'
}

echo "==> UDF retail semantic search smoke tests (PDF Q1–Q7)"

semantic_search "Q1 CLV / glossary" "customer lifetime value"
semantic_search "Q2 customer worth / v_band column" "customer worth value band"
semantic_search "Q3 certified revenue / classification" "trusted revenue reporting gold certified"
semantic_search "Q4 PII / email lineage source" "personal customer email data"
semantic_search "Q5 finance deck labels" "approved for Q3 finance deck"
semantic_search "Q6 source of truth / business metadata" "source of truth orders owner"
semantic_search "Q7 campaign ROI / glossary" "campaign returns marketing ROI"

echo ""
echo "==> Repair log tail (if container running):"
docker logs atlas-semantic-indexer 2>&1 | tail -5 || true
