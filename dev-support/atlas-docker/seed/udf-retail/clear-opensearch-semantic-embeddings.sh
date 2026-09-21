#!/usr/bin/env bash
# Remove atlas_semantic_embedding from all JanusGraph vertex index documents in OpenSearch.
set -euo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
INDEX="${OPENSEARCH_INDEX:-janusgraph_vertex_index}"

echo "==> Clearing atlas_semantic_embedding in ${INDEX} at ${OPENSEARCH_URL} ..."

curl -sS -X POST "${OPENSEARCH_URL}/${INDEX}/_update_by_query?conflicts=proceed&refresh=true" \
  -H "Content-Type: application/json" \
  -d '{
    "script": {
      "source": "ctx._source.remove(\"atlas_semantic_embedding\")",
      "lang": "painless"
    },
    "query": { "exists": { "field": "atlas_semantic_embedding" } }
  }' | jq '{updated: .updated, total: .total, failures: (.failures | length)}'

echo "==> Done. Re-run atlas_semantic_repair.sh to backfill embeddings."
