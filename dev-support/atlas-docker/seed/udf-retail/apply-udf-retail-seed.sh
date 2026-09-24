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

typedef_exists() {
  local type_name="$1"
  local http_code
  http_code="$(curl -s -o /dev/null -w '%{http_code}' "${AUTH[@]}" "${HDR[@]}" \
    "${ATLAS_URL}/api/atlas/v2/types/typedef/name/${type_name}")"
  [ "${http_code}" = "200" ]
}

# Register typedef section(s) from typedefs.json, skipping names that already exist.
# Full POST of typedefs.json fails atomically when e.g. PII already exists, leaving
# Sensitive/Certified/Deprecated unregistered and classification apply broken.
ensure_typedef_section() {
  local section="$1"
  local defs missing name i count
  defs="$(jq -c ".[\"${section}\"] // []" "${SCRIPT_DIR}/typedefs.json")"
  count="$(echo "${defs}" | jq 'length')"
  missing='[]'
  i=0
  while [ "${i}" -lt "${count}" ]; do
    name="$(echo "${defs}" | jq -r ".[${i}].name")"
    if typedef_exists "${name}"; then
      log "    typedef ${name} already exists — skip"
    else
      missing="$(echo "${missing}" | jq -c --argjson def "$(echo "${defs}" | jq -c ".[${i}]")" '. + [$def]')"
      log "    typedef ${name} missing — will create"
    fi
    i=$((i + 1))
  done
  if [ "$(echo "${missing}" | jq 'length')" -eq 0 ]; then
    return 0
  fi
  local payload
  payload="$(jq -nc --arg section "${section}" --argjson defs "${missing}" '{($section): $defs}')"
  local resp http_code
  resp="$(curl -sS -w '\n%{http_code}' "${AUTH[@]}" "${HDR[@]}" -X POST \
    "${ATLAS_URL}/api/atlas/v2/types/typedefs" -d "${payload}")"
  http_code="$(echo "${resp}" | tail -1)"
  if [ "${http_code}" -ge 400 ]; then
    echo "    FAILED to create ${section}: $(echo "${resp}" | sed '$d' | jq -c . 2>/dev/null || echo "${resp}")" >&2
    exit 1
  fi
}

log "Ensuring typedefs from typedefs.json (idempotent) ..."
ensure_typedef_section "entityDefs"
ensure_typedef_section "classificationDefs"
ensure_typedef_section "businessMetadataDefs"

log "Creating entities from entities-bulk.json ..."
post_file "${ATLAS_URL}/api/atlas/v2/entity/bulk" "${SCRIPT_DIR}/entities-bulk.json" \
  | jq -e '.guidAssignments // .mutatedEntities // .' >/dev/null

log "Creating lineage process from entity-lineage-process.json ..."
post_file "${ATLAS_URL}/api/atlas/v2/entity" "${SCRIPT_DIR}/entity-lineage-process.json" \
  | jq -e '.guidAssignments // .mutatedEntities // .entity // .' >/dev/null \
  || echo "    (lineage process may already exist — continuing)"

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

entity_has_classification() {
  local entity_type="$1"
  local qn="$2"
  local class_name="$3"
  curl -s "${AUTH[@]}" "${HDR[@]}" \
    "${ATLAS_URL}/api/atlas/v2/entity/uniqueAttribute/type/${entity_type}?attr:qualifiedName=${qn}" \
    | jq -e --arg c "${class_name}" '.entity.classifications[]? | select(.typeName == $c)' >/dev/null 2>&1
}

apply_classification() {
  local entity_type="$1"
  local qn="$2"
  local class_json="$3"
  local class_name
  class_name="$(echo "${class_json}" | jq -r '.typeName')"
  if entity_has_classification "${entity_type}" "${qn}" "${class_name}"; then
    log "    ${class_name} on ${qn} already applied — skip"
    return 0
  fi
  local resp http_code err_code
  resp="$(curl -sS -w '\n%{http_code}' "${AUTH[@]}" "${HDR[@]}" -X POST \
    "$(unique_attr_url "${entity_type}" "${qn}" "classifications")" \
    -d "[${class_json}]")"
  http_code="$(echo "${resp}" | tail -1)"
  if [ "${http_code}" -lt 400 ]; then
    return 0
  fi
  err_code="$(echo "${resp}" | sed '$d' | jq -r '.errorCode // empty' 2>/dev/null)"
  if [ "${err_code}" = "ATLAS-400-00-01A" ]; then
    log "    ${class_name} on ${qn} already applied — skip"
    return 0
  fi
  echo "    FAILED ${class_name} on ${qn}: $(echo "${resp}" | sed '$d' | jq -c . 2>/dev/null)" >&2
  exit 1
}

log "Applying classifications from entity-classifications.json ..."
while IFS= read -r row; do
  entity_type="$(echo "${row}" | jq -r '.entityType')"
  qn="$(echo "${row}" | jq -r '.qualifiedName')"
  log "    classifications on ${qn}"
  count="$(echo "${row}" | jq '.classifications | length')"
  i=0
  while [ "${i}" -lt "${count}" ]; do
    class_json="$(echo "${row}" | jq -c ".classifications[${i}]")"
    apply_classification "${entity_type}" "${qn}" "${class_json}"
    i=$((i + 1))
  done
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
  resp="$(curl -sS -w '\n%{http_code}' "${AUTH[@]}" "${HDR[@]}" -X POST \
    "${ATLAS_URL}/api/atlas/v2/entity/guid/${guid}/businessmetadata" \
    -d "${body}")"
  http_code="$(echo "${resp}" | tail -1)"
  if [ "${http_code}" -ge 400 ]; then
    echo "    FAILED business metadata on ${qn}: $(echo "${resp}" | sed '$d' | jq -c . 2>/dev/null)" >&2
    exit 1
  fi
done < <(jq -c '.[]' "${SCRIPT_DIR}/entity-business-metadata.json")

log "Seed apply complete."
log "Run: ${SCRIPT_DIR}/test-udf-retail-semantic.sh"
