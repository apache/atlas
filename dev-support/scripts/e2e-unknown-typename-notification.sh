#!/usr/bin/env bash
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
# E2E (ATLAS-5423): hook notifications with unknown/invalid typeName must be dropped
# without retries, and must not block subsequent notifications.
#
# Usage: ATLAS_HOME=<server dir> PYTHON=<python with kafka-python> ./dev-support/scripts/e2e-unknown-typename-notification.sh
#
set -euo pipefail

ATLAS_URL="${ATLAS_URL:-http://localhost:21000}"
ATLAS_USER="${ATLAS_USER:-admin}"
ATLAS_PASS="${ATLAS_PASS:-admin}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9027}"
PYTHON="${PYTHON:-python3}"
: "${ATLAS_HOME:?set ATLAS_HOME to the Atlas server directory}"

APP_LOG="${ATLAS_HOME}/logs/application.log"
FAILED_LOG="${ATLAS_HOME}/logs/failed.log"
RUN_ID="e2e5423_$(date +%s)"
VALID_DB_QN="${RUN_ID}_valid_db@cl1"
# not a registered type (unlike trino_table, which ships in models/6000-Trino)
UNKNOWN_TYPE="${RUN_ID}_trino_table"

APP_LOG_START=$(wc -l < "${APP_LOG}")

publish() {
  "${PYTHON}" - "${KAFKA_BOOTSTRAP}" "$1" <<'EOF'
import json, socket, sys, time
from kafka import KafkaProducer

# embedded Kafka listens on IPv4 only, while "localhost" may resolve to ::1 first
_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda host, port, family=0, *args, **kwargs: _getaddrinfo(host, port, socket.AF_INET, *args, **kwargs)

bootstrap, payload = sys.argv[1], json.loads(sys.argv[2])
envelope = {
    "version": {"version": "1.0.0", "versionParts": [1]},
    "msgCompressionKind": "NONE",
    "msgSplitIdx": 1,
    "msgSplitCount": 1,
    "msgCreatedBy": "e2e",
    "msgCreationTime": int(time.time() * 1000),
    "message": payload,
}
producer = KafkaProducer(bootstrap_servers=bootstrap)
producer.send("ATLAS_HOOK", json.dumps(envelope).encode("utf-8")).get(timeout=30)
producer.flush()
EOF
}

echo "== version =="
curl -sf -u "${ATLAS_USER}:${ATLAS_PASS}" "${ATLAS_URL}/api/atlas/admin/version" || { echo "FAIL: cannot reach ${ATLAS_URL} as ${ATLAS_USER}"; exit 1; }
echo

echo "== 1. ENTITY_PARTIAL_UPDATE_V2 with unknown typeName (scenario from the JIRA, trino model not deployed) =="
publish "$(cat <<EOF
{"type":"ENTITY_PARTIAL_UPDATE_V2","user":"admin",
 "entityId":{"typeName":"${UNKNOWN_TYPE}","uniqueAttributes":{"qualifiedName":"${RUN_ID}_v2partial@cl1"}},
 "entity":{"entity":{"typeName":"${UNKNOWN_TYPE}","attributes":{"qualifiedName":"${RUN_ID}_v2partial@cl1","name":"t"}}}}
EOF
)"

echo "== 2. ENTITY_CREATE_V2 with a valid entity and an unknown referred-entity typeName =="
publish "$(cat <<EOF
{"type":"ENTITY_CREATE_V2","user":"admin",
 "entities":{"entities":[{"typeName":"hive_db","guid":"-1","attributes":{"qualifiedName":"${RUN_ID}_mixed_db@cl1","name":"${RUN_ID}_mixed_db","clusterName":"cl1"}}],
             "referredEntities":{"-2":{"typeName":"${RUN_ID}_unknown_type","guid":"-2","attributes":{"qualifiedName":"${RUN_ID}_ref@cl1"}}}}}
EOF
)"

echo "== 3. V1 ENTITY_PARTIAL_UPDATE with unknown typeName =="
publish "$(cat <<EOF
{"type":"ENTITY_PARTIAL_UPDATE","user":"admin","typeName":"${UNKNOWN_TYPE}","attribute":"qualifiedName","attributeValue":"${RUN_ID}_v1partial@cl1",
 "entity":{"jsonClass":"org.apache.atlas.typesystem.json.InstanceSerialization\$_Reference","typeName":"${UNKNOWN_TYPE}",
           "id":{"jsonClass":"org.apache.atlas.typesystem.json.InstanceSerialization\$_Id","id":"-1","version":0,"typeName":"${UNKNOWN_TYPE}","state":"ACTIVE"},
           "values":{"qualifiedName":"${RUN_ID}_v1partial@cl1","name":"t"},"traitNames":[],"traits":{}}}
EOF
)"

echo "== 4. ENTITY_DELETE_V2 with unknown typeName =="
publish "$(cat <<EOF
{"type":"ENTITY_DELETE_V2","user":"admin","entities":[{"typeName":"${UNKNOWN_TYPE}","uniqueAttributes":{"qualifiedName":"${RUN_ID}_delete@cl1"}}]}
EOF
)"

echo "== 5. valid ENTITY_CREATE_V2 published after the invalid ones =="
publish "$(cat <<EOF
{"type":"ENTITY_CREATE_V2","user":"admin",
 "entities":{"entities":[{"typeName":"hive_db","guid":"-1","attributes":{"qualifiedName":"${VALID_DB_QN}","name":"${RUN_ID}_valid_db","clusterName":"cl1"}}]}}
EOF
)"

echo "== waiting for the valid entity to be created =="
FOUND=""
for _ in $(seq 1 60); do
  if curl -sf -u "${ATLAS_USER}:${ATLAS_PASS}" "${ATLAS_URL}/api/atlas/v2/entity/uniqueAttribute/type/hive_db?attr:qualifiedName=${VALID_DB_QN}" >/dev/null; then
    FOUND=yes
    break
  fi
  sleep 1
done
test -n "${FOUND}" || { echo "FAIL: valid notification after invalid ones was not processed"; exit 1; }
echo "OK: valid notification processed"

sleep 2
NEW_LOG=$(tail -n +"$((APP_LOG_START + 1))" "${APP_LOG}")

fail() { echo "FAIL: $*"; exit 1; }

UNRECOVERABLE=$(echo "${NEW_LOG}" | grep -c "Unrecoverable failure, skipping retries" || true)
echo "unrecoverable-failure log lines: ${UNRECOVERABLE}"
[ "${UNRECOVERABLE}" -eq 4 ] || fail "expected 4 dropped notifications, found ${UNRECOVERABLE}"

echo "${NEW_LOG}" | grep -q "skipping retries: ${UNKNOWN_TYPE}: Unknown/invalid typename. type=ENTITY_PARTIAL_UPDATE_V2" || fail "V2 partial update not rejected upfront"
echo "${NEW_LOG}" | grep -q "skipping retries: ${RUN_ID}_unknown_type: Unknown/invalid typename. type=ENTITY_CREATE_V2" || fail "create with unknown referred type not rejected upfront"
echo "${NEW_LOG}" | grep -q "skipping retries: ${UNKNOWN_TYPE}: Unknown/invalid typename. type=ENTITY_PARTIAL_UPDATE," || fail "V1 partial update not rejected upfront"
echo "${NEW_LOG}" | grep -q "skipping retries: ${UNKNOWN_TYPE}: Unknown/invalid typename. type=ENTITY_DELETE_V2" || fail "V2 delete not rejected upfront"
echo "OK: all 4 invalid notifications rejected before processing"

RETRIES=$(echo "${NEW_LOG}" | grep -c -E "Pausing & retry|Sleeping for [0-9]+ ms before retry|Max retries exceeded" || true)
[ "${RETRIES}" -eq 0 ] || fail "found ${RETRIES} retry log lines"
echo "OK: no retries"

echo "${NEW_LOG}" | grep "NotificationHookConsumer thread" | grep -q "graph rollback due to exception" && fail "invalid notification reached the entity store (graph rollback logged)"
echo "OK: no graph transaction started for invalid notifications"

DROPPED=$(grep -c "DROPPED_NOTIFICATION.*${RUN_ID}" "${FAILED_LOG}" || true)
[ "${DROPPED}" -eq 4 ] || fail "expected 4 entries in failed.log, found ${DROPPED}"
echo "OK: 4 dropped notifications recorded in failed.log"

if curl -sf -u "${ATLAS_USER}:${ATLAS_PASS}" "${ATLAS_URL}/api/atlas/v2/entity/uniqueAttribute/type/hive_db?attr:qualifiedName=${RUN_ID}_mixed_db@cl1" >/dev/null; then
  fail "entity from the rejected mixed notification was created"
fi
echo "OK: no partial writes from the rejected mixed notification"

echo "== all ATLAS-5423 e2e checks passed =="
