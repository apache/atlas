#!/usr/bin/env bash
# CDPD-40161: verify approximateCountExact on live Atlas basic search API.
set -euo pipefail

ATLAS_URL="${ATLAS_URL:-http://localhost:21000}"
USER="${ATLAS_USER:-admin}"
PASS="${ATLAS_PASS:-atlasR0cks!}"

auth_curl() {
  curl -sS -u "${USER}:${PASS}" -H "Content-Type: application/json" "$@"
}

echo "==> Atlas version"
auth_curl "${ATLAS_URL}/api/atlas/admin/version" | head -c 200
echo

echo "==> Index-only search (expect approximateCountExact=true)"
INDEX_ONLY=$(auth_curl -X POST "${ATLAS_URL}/api/atlas/v2/search/basic" -d '{
  "typeName": "hive_db",
  "limit": 20,
  "offset": 0,
  "excludeDeletedEntities": true
}')
echo "$INDEX_ONLY" | python3 -c "
import json,sys
d=json.load(sys.stdin)
exact=d.get('approximateCountExact')
count=d.get('approximateCount')
print('approximateCount=', count, 'approximateCountExact=', exact)
assert exact is True, 'expected approximateCountExact true for type-only search'
"

echo "==> In-memory filter search (expect approximateCountExact not true)"
FILTERED=$(auth_curl -X POST "${ATLAS_URL}/api/atlas/v2/search/basic" -d '{
  "typeName": "hive_column",
  "limit": 20,
  "offset": 0,
  "excludeDeletedEntities": true,
  "entityFilters": {
    "condition": "AND",
    "criterion": [
      {"attributeName": "qualifiedName", "operator": "startsWith", "attributeValue": "default."},
      {"attributeName": "qualifiedName", "operator": "endsWith", "attributeValue": "@cm"}
    ]
  }
}')
echo "$FILTERED" | python3 -c "
import json,sys
d=json.load(sys.stdin)
exact=d.get('approximateCountExact')
count=d.get('approximateCount')
print('approximateCount=', count, 'approximateCountExact=', exact)
assert exact is not True, 'expected approximate count (no exact flag) for in-memory filters'
"

echo "==> All live API checks passed"
