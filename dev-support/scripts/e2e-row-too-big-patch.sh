#!/usr/bin/env bash
# CDPD-63922 / ATLAS-5426: validate paginated edge patch + vertex edge scan changes locally.
#
# Unit/integration (always):
#   - EdgePatchScannerTest, RelationshipTypeNamePatchTest, EdgePatchProcessorTest
#   - AtlasJanusGraphTest#getAllEdgesVertices* (batched neighbor scan)
#
# Live Atlas (optional): set ATLAS_URL (default http://localhost:21000) and ensure server is up.
# Verifies admin API and that RelationshipTypeNamePatch is not failing startup (grep application log if ATLAS_LOG set).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 1.8 2>/dev/null || true)}"

echo "==> Installing modules (skip tests)..."
mvn -q -pl intg,graphdb/api,graphdb/janus,repository -am install \
  -DskipTests -Drat.skip=true -DskipEnunciate=true

echo "==> repository: patch scanner + processor tests..."
mvn -q -pl repository \
  -Dtest=EdgePatchScannerTest,RelationshipTypeNamePatchTest,EdgePatchProcessorTest \
  -DfailIfNoTests=false -Drat.skip=true test

echo "==> graphdb/janus: getAllEdgesVertices (small + high-degree)..."
mvn -q -pl graphdb/janus \
  -Dtest=AtlasJanusGraphTest#testGetAllEdgesVertices,AtlasJanusGraphTest#testGetAllEdgesVerticesHighDegree \
  -DfailIfNoTests=false -Drat.skip=true test

ATLAS_URL="${ATLAS_URL:-http://localhost:21000}"
ATLAS_USER="${ATLAS_USER:-admin}"
ATLAS_PASS="${ATLAS_PASS:-atlasR0cks!}"

echo "==> Live Atlas check (${ATLAS_URL})..."
if curl -sf -u "${ATLAS_USER}:${ATLAS_PASS}" -H "Accept: application/json" \
  "${ATLAS_URL}/api/atlas/admin/version" >/tmp/atlas-e2e-version.json 2>/dev/null; then
  python3 -m json.tool /tmp/atlas-e2e-version.json | head -8
  echo "Live Atlas responded — server is up."

  if [[ -n "${ATLAS_LOG:-}" && -f "${ATLAS_LOG}" ]]; then
    if grep -q "RelationshipTypeNamePatch" "${ATLAS_LOG}" && \
       grep -q "Error applying patches" "${ATLAS_LOG}"; then
      echo "ERROR: ATLAS_LOG shows patch application failure — check RelationshipTypeNamePatch / RowTooBig."
      exit 1
    fi
    if grep -q "RelationshipTypeNamePatch: Starting" "${ATLAS_LOG}"; then
      echo "ATLAS_LOG: RelationshipTypeNamePatch ran during startup (no global patch failure logged)."
    fi
  else
    echo "Tip: set ATLAS_LOG to application log path to assert patch startup on a live cluster."
  fi
else
  echo "No live Atlas at ${ATLAS_URL} — skipped (unit/integration tests above are the local e2e gate)."
  echo "To run live check: start Atlas (e.g. dev-support/atlas-docker) and re-run this script."
fi

echo "==> All targeted checks passed."
