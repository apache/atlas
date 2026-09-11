#!/usr/bin/env bash

set -euo pipefail

NODE1_CONTAINER="${NODE1_CONTAINER:-atlas-docker-atlas-metadata-server-1}"
NODE2_CONTAINER="${NODE2_CONTAINER:-atlas-docker-atlas-metadata-server-2}"
LB_URL="${LB_URL:-http://localhost:21000}"
TYPE_NAME="${TYPE_NAME:-test_atlaspolicyallowcreatetype_ck_1}"
REQUEST_ID_PREFIX="${REQUEST_ID_PREFIX:-typedef-sync-demo}"
SYNC_WAIT_SECONDS="${SYNC_WAIT_SECONDS:-60}"
HTTP_TIMEOUT_SECONDS="${HTTP_TIMEOUT_SECONDS:-20}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[ERROR] Required command not found: $1" >&2
    exit 1
  fi
}

require_cmd docker
require_cmd curl

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [optional-payload-json-path]" >&2
  exit 1
fi

payload_file=""
cleanup_payload_file=true

if [[ $# -eq 1 ]]; then
  payload_file="$1"
  cleanup_payload_file=false
  if [[ ! -f "$payload_file" ]]; then
    echo "[ERROR] Payload file not found: $payload_file" >&2
    exit 1
  fi
else
  payload_file="$(mktemp "/tmp/atlas-typedef-sync-payload.XXXXXX.json")"
  cat > "$payload_file" <<'EOF'
{
  "enumDefs": [],
  "structDefs": [],
  "classificationDefs": [],
  "entityDefs": [
    {
      "attributeDefs": [
        { "name": "CKP_NAME_P", "typeName": "string", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_str", "typeName": "string", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_bool_true", "typeName": "boolean", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_bool_false", "typeName": "boolean", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_byte_min", "typeName": "byte", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_byte_rand", "typeName": "byte", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_byte_max", "typeName": "byte", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_short_min", "typeName": "short", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_short_rand", "typeName": "short", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_short_max", "typeName": "short", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_float_min", "typeName": "float", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_float_rand", "typeName": "float", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_float_max", "typeName": "float", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_double_min", "typeName": "double", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_double_rand", "typeName": "double", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_double_max", "typeName": "double", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_date", "typeName": "date", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_int_min", "typeName": "int", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_int_rand", "typeName": "int", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_int_max", "typeName": "int", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_bigint_min", "typeName": "biginteger", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_bigint_rand", "typeName": "biginteger", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_bigint_max", "typeName": "biginteger", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_bigdecimal_min", "typeName": "bigdecimal", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_bigdecimal_rand", "typeName": "bigdecimal", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_bigdecimal_max", "typeName": "bigdecimal", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_long_max", "typeName": "long", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_long_rand", "typeName": "long", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_long_min", "typeName": "long", "isOptional": true, "cardinality": "SINGLE", "valuesMinCount": 0, "valuesMaxCount": 1, "isUnique": false, "isIndexable": false },
        { "name": "type_arr_list", "typeName": "array<string>", "isOptional": false, "cardinality": "LIST", "valuesMinCount": 1, "valuesMaxCount": 2147483647, "isUnique": false, "isIndexable": false },
        { "name": "type_set", "typeName": "array<string>", "isOptional": false, "cardinality": "SET", "valuesMinCount": 1, "valuesMaxCount": 2147483647, "isUnique": false, "isIndexable": false }
      ],
      "description": "description",
      "name": "test_atlaspolicyallowcreatetype_ck_1",
      "guid": "-910550886037",
      "category": "ENTITY",
      "superTypes": []
    }
  ],
  "relationshipDefs": [],
  "businessMetadataDefs": []
}
EOF
fi

trap 'if [[ "$cleanup_payload_file" == "true" && -f "$payload_file" ]]; then rm -f "$payload_file"; fi' EXIT

echo "[INFO] Using payload file: $payload_file"
echo "[INFO] Posting typedef to node-1 container: $NODE1_CONTAINER"

docker cp "$payload_file" "${NODE1_CONTAINER}:/tmp/typedef-sync-demo.json"

echo "[INFO] Sending create request (timeout=${HTTP_TIMEOUT_SECONDS}s)..."
set +e
post_body="$(docker exec "$NODE1_CONTAINER" sh -lc "wget -qO- \
  -T ${HTTP_TIMEOUT_SECONDS} --tries=1 \
  --header='Content-Type: application/json' \
  --header='x-awc-username: admin' \
  --header='x-awc-roles: ADMIN' \
  --header='x-awc-requestid: ${REQUEST_ID_PREFIX}-create' \
  --post-file=/tmp/typedef-sync-demo.json \
  'http://localhost:21000/api/atlas/v2/types/typedefs' 2>&1")"
post_rc=$?
set -e
echo "[INFO] Create request completed on ${NODE1_CONTAINER}"
echo "$post_body"
if [[ $post_rc -ne 0 ]]; then
  if [[ "$post_body" == *"already exists"* || "$post_body" == *"ATLAS-409"* ]]; then
    echo "[WARN] TypeDef appears to already exist; continuing with sync validation."
  else
    echo "[ERROR] Create request failed (rc=${post_rc})." >&2
    exit 1
  fi
fi

validate_on_container() {
  local container="$1"
  local request_id="$2"
  local out code body

  set +e
  out="$(docker exec "$container" sh -lc "wget -qO- \
    -T ${HTTP_TIMEOUT_SECONDS} --tries=1 \
    'http://localhost:21000/api/atlas/v2/types/entitydef/name/${TYPE_NAME}' \
    --header='x-awc-username: admin' \
    --header='x-awc-roles: ADMIN' \
    --header='x-awc-requestid: ${request_id}' 2>&1")"
  local rc=$?
  set -e
  body="$out"

  if [[ $rc -ne 0 ]]; then
    echo "[ERROR] ${container} request failed (rc=${rc})" >&2
    echo "$body" >&2
    return 1
  fi

  echo "[INFO] ${container} request completed"
  if [[ "$body" != *"\"name\":\"${TYPE_NAME}\""* ]]; then
    echo "[ERROR] ${container} response does not include expected typedef name ${TYPE_NAME}" >&2
    echo "$body" >&2
    return 1
  fi
  return 0
}

wait_for_sync_on_container() {
  local container="$1"
  local request_id_prefix="$2"
  local max_wait="$3"
  local elapsed=0
  local interval=3

  while (( elapsed <= max_wait )); do
    if validate_on_container "$container" "${request_id_prefix}-${elapsed}" >/dev/null 2>&1; then
      echo "[INFO] TypeDef '${TYPE_NAME}' is visible on ${container} after ${elapsed}s"
      return 0
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  echo "[ERROR] TypeDef '${TYPE_NAME}' not visible on ${container} after ${max_wait}s" >&2
  validate_on_container "$container" "${request_id_prefix}-final" || true
  return 1
}

echo "[INFO] Validating typedef on metadata nodes..."
validate_on_container "$NODE1_CONTAINER" "${REQUEST_ID_PREFIX}-get-node1"
echo "[INFO] Waiting for typedef sync on node-2 (timeout=${SYNC_WAIT_SECONDS}s)..."
wait_for_sync_on_container "$NODE2_CONTAINER" "${REQUEST_ID_PREFIX}-wait-node2" "$SYNC_WAIT_SECONDS"

echo "[INFO] Validating typedef via load balancer URL: ${LB_URL}"
lb_out="$(curl -sS -w ' HTTP_STATUS=%{http_code}' \
  "${LB_URL}/api/atlas/v2/types/entitydef/name/${TYPE_NAME}" \
  -H "x-awc-username: admin" \
  -H "x-awc-roles: ADMIN" \
  -H "x-awc-requestid: ${REQUEST_ID_PREFIX}-get-lb")"
lb_code="${lb_out##*HTTP_STATUS=}"
lb_body="${lb_out% HTTP_STATUS=*}"
echo "[INFO] LB HTTP=${lb_code}"
if [[ "$lb_code" != "200" || "$lb_body" != *"\"name\":\"${TYPE_NAME}\""* ]]; then
  echo "[ERROR] LB validation failed for typedef ${TYPE_NAME}" >&2
  echo "$lb_body" >&2
  exit 1
fi

echo "[SUCCESS] TypeDef '${TYPE_NAME}' created on node-1 and visible on node-1, node-2, and LB."
