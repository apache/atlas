#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Enables index.knn on an existing JanusGraph vertex index, without a reindex.
#
# index.knn is a final setting: it can't be changed on an existing index, only set when an index is created.
# _clone creates a new index from the same segment files (hard links: seconds, almost no extra disk) and
# accepts new settings, so the index is cloned to ${INDEX}_knn with index.knn=true, then cloned back to its
# original name (OpenSearch has no rename). ${INDEX}_knn is kept as a backup.
#
# Atlas and the Semantic Indexer must be stopped. Without --confirm, only the checks run.
#
# Environment:
#   OPENSEARCH_URL       default http://localhost:9200
#   OPENSEARCH_USER      optional basic auth user (OPENSEARCH_PASSWORD for its password)
#   CURL_OPTS            extra curl options, e.g. "--cacert /path/ca.pem"
#   VERTEX_INDEX         default janusgraph_vertex_index
#   ATLAS_URL            default http://localhost:21000; the script refuses to run while it answers
#   WAIT_TIMEOUT         default 10m, max wait for each clone to become green

set -Eeuo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
VERTEX_INDEX="${VERTEX_INDEX:-janusgraph_vertex_index}"
ATLAS_URL="${ATLAS_URL:-http://localhost:21000}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-10m}"
KNN_INDEX="${VERTEX_INDEX}_knn"
CONFIRM=false

for arg in "$@"; do
  case "${arg}" in
    --confirm) CONFIRM=true ;;
    *) echo "Usage: $0 [--confirm]" >&2; exit 2 ;;
  esac
done

AUTH=()
if [ -n "${OPENSEARCH_USER:-}" ]; then
  AUTH=(-u "${OPENSEARCH_USER}:${OPENSEARCH_PASSWORD:-}")
fi

# prints the response body; fails (body on stderr) on HTTP errors
os() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -w '\n%{http_code}' -X "${method}" "${AUTH[@]+"${AUTH[@]}"}" ${CURL_OPTS:-})
  if [ -n "${body}" ]; then
    args+=(-H 'Content-Type: application/json' -d "${body}")
  fi

  local response code
  response=$(curl "${args[@]}" "${OPENSEARCH_URL}${path}")
  code="${response##*$'\n'}"
  response="${response%$'\n'*}"
  if [ "${code}" -ge 300 ]; then
    echo "ERROR: ${method} ${path} returned HTTP ${code}: ${response}" >&2
    return 1
  fi
  echo "${response}"
}

index_exists() {
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -I "${AUTH[@]+"${AUTH[@]}"}" ${CURL_OPTS:-} "${OPENSEARCH_URL}/$1")
  [ "${code}" = "200" ]
}

json() {
  python3 -c "import sys,json; d=json.load(sys.stdin); print($1)"
}

knn_enabled() {
  os GET "/$1/_settings/index.knn?include_defaults=true" \
    | json "any(str(v.get(s,{}).get('index',{}).get('knn','')).lower()=='true' for v in d.values() for s in ('settings','defaults'))"
}

doc_count() {
  os POST "/$1/_refresh" >/dev/null
  os GET "/$1/_count" | json "d['count']"
}

wait_green() {
  echo "==> Waiting for $1 to be green (timeout ${WAIT_TIMEOUT}) ..."
  local status
  status=$(os GET "/_cluster/health/$1?wait_for_status=green&timeout=${WAIT_TIMEOUT}" | json "d['status']")
  if [ "${status}" != "green" ]; then
    echo "ERROR: $1 is ${status} after ${WAIT_TIMEOUT}" >&2
    return 1
  fi
}

set_write_block() {
  os PUT "/$1/_settings" "{\"index.blocks.write\":$2}" >/dev/null
}

# --- checks (no changes) ---

if curl -s -o /dev/null --max-time 5 "${ATLAS_URL}"; then
  echo "ERROR: Atlas answers at ${ATLAS_URL}. Stop Atlas and the Semantic Indexer first" \
       "(or set ATLAS_URL if this check targets the wrong host)." >&2
  exit 1
fi

if ! index_exists "${VERTEX_INDEX}"; then
  echo "ERROR: index ${VERTEX_INDEX} does not exist" >&2
  exit 1
fi

if [ "$(knn_enabled "${VERTEX_INDEX}")" = "True" ]; then
  echo "index.knn is already enabled on ${VERTEX_INDEX}: nothing to do."
  exit 0
fi

if index_exists "${KNN_INDEX}"; then
  echo "ERROR: ${KNN_INDEX} already exists (left by a previous run?). Inspect it, delete it, then rerun." >&2
  exit 1
fi

health=$(os GET "/_cluster/health/${VERTEX_INDEX}" | json "d['status']")
if [ "${health}" != "green" ]; then
  echo "ERROR: ${VERTEX_INDEX} is ${health}; _clone needs green. On a single node, set replicas to 0 first:" >&2
  echo "  curl -X PUT ${OPENSEARCH_URL}/${VERTEX_INDEX}/_settings -H 'Content-Type: application/json' -d '{\"index.number_of_replicas\":0}'" >&2
  exit 1
fi

source_count=$(doc_count "${VERTEX_INDEX}")
# _clone doesn't copy number_of_replicas (the target gets the cluster default), so pass the source's value
replicas=$(os GET "/${VERTEX_INDEX}/_settings/index.number_of_replicas" | json "list(d.values())[0]['settings']['index']['number_of_replicas']")
echo "==> ${VERTEX_INDEX}: green, ${source_count} documents, ${replicas} replica(s), index.knn not enabled"

if [ "${CONFIRM}" != "true" ]; then
  cat <<EOF

Checks passed. With --confirm, this script will:
  1. block writes on ${VERTEX_INDEX} and clone it to ${KNN_INDEX} with index.knn=true
  2. check ${KNN_INDEX} is green, has index.knn and ${source_count} documents
  3. delete ${VERTEX_INDEX} and clone ${KNN_INDEX} back to ${VERTEX_INDEX}
  4. check ${VERTEX_INDEX} again, and keep ${KNN_INDEX} (write-blocked) as a backup
EOF
  exit 0
fi

# --- step 1-2: knn copy; on failure the original is unblocked and untouched ---

set_write_block "${VERTEX_INDEX}" true
os POST "/${VERTEX_INDEX}/_flush" >/dev/null

restore_source() {
  echo "ERROR: clone to ${KNN_INDEX} failed; ${VERTEX_INDEX} is unchanged. Inspect or delete ${KNN_INDEX}." >&2
  set_write_block "${VERTEX_INDEX}" null || true
}
trap restore_source ERR

echo "==> Cloning ${VERTEX_INDEX} to ${KNN_INDEX} with index.knn=true ..."
os POST "/${VERTEX_INDEX}/_clone/${KNN_INDEX}" \
  "{\"settings\":{\"index.knn\":true,\"index.number_of_replicas\":${replicas},\"index.blocks.write\":null}}" >/dev/null
wait_green "${KNN_INDEX}"

knn_count=$(doc_count "${KNN_INDEX}")
if [ "$(knn_enabled "${KNN_INDEX}")" != "True" ] || [ "${knn_count}" != "${source_count}" ]; then
  echo "ERROR: ${KNN_INDEX} check failed (documents ${knn_count}/${source_count}, or index.knn missing)" >&2
  false
fi
echo "==> ${KNN_INDEX}: green, index.knn=true, ${knn_count} documents"

trap - ERR

# --- step 3-4: replace the original; from here ${KNN_INDEX} holds the only full copy ---

set_write_block "${KNN_INDEX}" true

replace_failed() {
  cat >&2 <<EOF
ERROR: replacing ${VERTEX_INDEX} failed. ${KNN_INDEX} has all ${source_count} documents with index.knn=true.
Finish by hand:
  curl -X DELETE ${OPENSEARCH_URL}/${VERTEX_INDEX}     # only if a partial ${VERTEX_INDEX} exists
  curl -X POST ${OPENSEARCH_URL}/${KNN_INDEX}/_clone/${VERTEX_INDEX} -H 'Content-Type: application/json' -d '{"settings":{"index.number_of_replicas":${replicas},"index.blocks.write":null}}'
EOF
}
trap replace_failed ERR

echo "==> Deleting ${VERTEX_INDEX} and cloning ${KNN_INDEX} back to it ..."
os DELETE "/${VERTEX_INDEX}" >/dev/null
os POST "/${KNN_INDEX}/_clone/${VERTEX_INDEX}" \
  "{\"settings\":{\"index.number_of_replicas\":${replicas},\"index.blocks.write\":null}}" >/dev/null
wait_green "${VERTEX_INDEX}"

final_count=$(doc_count "${VERTEX_INDEX}")
if [ "$(knn_enabled "${VERTEX_INDEX}")" != "True" ] || [ "${final_count}" != "${source_count}" ]; then
  echo "ERROR: ${VERTEX_INDEX} check failed (documents ${final_count}/${source_count}, or index.knn missing)" >&2
  false
fi

trap - ERR

cat <<EOF
==> Done: ${VERTEX_INDEX} has index.knn=true and ${final_count} documents.

Next:
  1. set atlas.graph.index.search.opensearch.create.ext.knn=true in atlas-application.properties
  2. start Atlas, run atlas_semantic_repair.sh, then start the Semantic Indexer
  3. once search is verified, delete the backup: curl -X DELETE ${OPENSEARCH_URL}/${KNN_INDEX}
EOF
