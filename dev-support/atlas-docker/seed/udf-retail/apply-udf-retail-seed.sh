#!/usr/bin/env bash
# Apply UDF retail seed data: each step POSTs/PUTs a JSON file to the Atlas v2 API.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ATLAS_URL="${ATLAS_URL:-http://localhost:21000}"
ATLAS_USER="${ATLAS_USER:-admin}"
ATLAS_PASS="${ATLAS_PASS:-atlasR0cks!}"
AUTH=(-u "${ATLAS_USER}:${ATLAS_PASS}")
HDR=(-H "Accept: application/json" -H "Content-Type: application/json")

log() { echo "==> $*" >&2; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

require_cmd curl
require_cmd jq

post_file() {
  local url="$1"
  local file="$2"
  log "POST ${file}"
  curl -sS "${AUTH[@]}" "${HDR[@]}" -X POST "${url}" -d @"${file}"
}

post_json() {
  local url="$1"
  local json="$2"
  curl -sS "${AUTH[@]}" "${HDR[@]}" -X POST "${url}" -d "${json}"
}

put_file() {
  local url="$1"
  local file="$2"
  log "PUT ${file}"
  curl -sS "${AUTH[@]}" "${HDR[@]}" -X PUT "${url}" -d @"${file}"
}

post_relationships_file() {
  local file="$1"
  local count
  count="$(jq 'length' "${file}")"
  log "POST relationships from ${file} (${count})"
  local i=0
  while [ "${i}" -lt "${count}" ]; do
    local body
    body="$(jq -c ".[${i}]" "${file}")"
    curl -sS "${AUTH[@]}" "${HDR[@]}" -X POST "${ATLAS_URL}/api/atlas/v2/relationship" \
      -d "${body}" >/dev/null \
      || echo "    (relationship ${i} may already exist — continuing)"
    i=$((i + 1))
  done
}

unique_attr_url() {
  local entity_type="$1"
  local qualified_name="$2"
  local suffix="$3"
  printf '%s/api/atlas/v2/entity/uniqueAttribute/type/%s/%s?attr:qualifiedName=%s' \
    "${ATLAS_URL}" "${entity_type}" "${suffix}" "${qualified_name}"
}

log "Creating typedefs from typedefs.json ..."
if ! post_file "${ATLAS_URL}/api/atlas/v2/types/typedefs" "${SCRIPT_DIR}/typedefs.json" \
  | jq -e '.guidAssignments // .mutatedEntities // .' >/dev/null 2>&1; then
  echo "    (typedef create may fail if types already exist — continuing)"
fi

log "Creating entities from entities-bulk.json ..."
post_file "${ATLAS_URL}/api/atlas/v2/entity/bulk" "${SCRIPT_DIR}/entities-bulk.json" \
  | jq -e '.guidAssignments // .mutatedEntities // .' >/dev/null

log "Creating glossary from glossary-create.json ..."
GLOSSARY_RESP="$(post_file "${ATLAS_URL}/api/atlas/v2/glossary" "${SCRIPT_DIR}/glossary-create.json")"
GLOSSARY_GUID="$(echo "${GLOSSARY_RESP}" | jq -r '.guid // .mutatedEntities.entityCreate[0].guid // empty')"
if [ -z "${GLOSSARY_GUID}" ]; then
  GLOSSARY_GUID="$(curl -s "${AUTH[@]}" "${HDR[@]}" \
    "${ATLAS_URL}/api/atlas/v2/search/basic?typeName=AtlasGlossary&query=UDF%20Retail" \
    | jq -r '.entities[0].guid // empty')"
fi
log "    glossary guid=${GLOSSARY_GUID}"

log "Creating glossary terms from glossary-terms.json ..."
while IFS= read -r term_json; do
  term_name="$(echo "${term_json}" | jq -r '.name')"
  log "    term: ${term_name}"
  post_json "${ATLAS_URL}/api/atlas/v2/glossary/term" \
    "$(echo "${term_json}" | jq -c --arg g "${GLOSSARY_GUID}" '. + {anchor: {glossaryGuid: $g}}')" \
    >/dev/null
done < <(jq -c '.terms[]' "${SCRIPT_DIR}/glossary-terms.json")

post_relationships_file "${SCRIPT_DIR}/term-relationships.json"
post_relationships_file "${SCRIPT_DIR}/term-assignments.json"

log "Applying classifications from entity-classifications.json ..."
while IFS= read -r row; do
  entity_type="$(echo "${row}" | jq -r '.entityType')"
  qn="$(echo "${row}" | jq -r '.qualifiedName')"
  body="$(echo "${row}" | jq -c '.classifications')"
  log "    classifications on ${qn}"
  curl -sS "${AUTH[@]}" "${HDR[@]}" -X POST \
    "$(unique_attr_url "${entity_type}" "${qn}" "classifications")" \
    -d "${body}" >/dev/null \
    || curl -sS "${AUTH[@]}" "${HDR[@]}" -X PUT \
      "$(unique_attr_url "${entity_type}" "${qn}" "classifications")" \
      -d "${body}" >/dev/null
done < <(jq -c '.[]' "${SCRIPT_DIR}/entity-classifications.json")

log "Applying labels from entity-labels.json ..."
while IFS= read -r row; do
  entity_type="$(echo "${row}" | jq -r '.entityType')"
  qn="$(echo "${row}" | jq -r '.qualifiedName')"
  body="$(echo "${row}" | jq -c '.labels')"
  log "    labels on ${qn}"
  curl -sS "${AUTH[@]}" "${HDR[@]}" -X POST \
    "$(unique_attr_url "${entity_type}" "${qn}" "labels")" \
    -d "${body}" >/dev/null
done < <(jq -c '.[]' "${SCRIPT_DIR}/entity-labels.json")

log "Applying business metadata from entity-business-metadata.json ..."
while IFS= read -r row; do
  entity_type="$(echo "${row}" | jq -r '.entityType')"
  qn="$(echo "${row}" | jq -r '.qualifiedName')"
  body="$(echo "${row}" | jq -c '.businessMetadata')"
  guid="$(curl -s "${AUTH[@]}" "${HDR[@]}" \
    "${ATLAS_URL}/api/atlas/v2/entity/uniqueAttribute/type/${entity_type}?attr:qualifiedName=${qn}" \
    | jq -r '.entity.guid // empty')"
  if [ -z "${guid}" ]; then
    echo "    skip business metadata on ${qn} (entity not found)" >&2
    continue
  fi
  log "    business metadata on ${qn}"
  curl -sS "${AUTH[@]}" "${HDR[@]}" -X POST \
    "${ATLAS_URL}/api/atlas/v2/entity/guid/${guid}/businessmetadata" \
    -d "${body}" >/dev/null
done < <(jq -c '.[]' "${SCRIPT_DIR}/entity-business-metadata.json")

log "Seed apply complete."
log "Run: ${SCRIPT_DIR}/test-udf-retail-semantic.sh"
