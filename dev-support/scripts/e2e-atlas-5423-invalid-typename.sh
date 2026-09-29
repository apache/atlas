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
# ATLAS-5423 local validation:
#  1) Validator + SerialEntityProcessor unit checks
#  2) Embedded Kafka → HookConsumer → SerialEntityProcessor E2E
#  3) Docker Atlas (Java 17) + live ATLAS_HOOK publish + log verification
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

WEBAPP_JAR="${REPO_ROOT}/webapp/target/atlas-webapp-3.0.0-SNAPSHOT/WEB-INF/lib/atlas-webapp-3.0.0-SNAPSHOT.jar"
SERVER_COMMON_JAR="${REPO_ROOT}/server-common/target/atlas-server-common-3.0.0-SNAPSHOT.jar"
ATLAS_SH="${REPO_ROOT}/dev-support/atlas-docker/scripts/atlas.sh"

wait_for_atlas() {
  local i
  for i in $(seq 1 60); do
    local code
    code=$(curl -s -u admin:admin -o /dev/null -w "%{http_code}" http://localhost:21000/api/atlas/admin/status 2>/dev/null || true)
    if [ "${code}" = "200" ]; then
      return 0
    fi
    sleep 5
  done
  return 1
}

deploy_patch_to_docker_atlas() {
  if ! docker ps -a --format '{{.Names}}' | grep -qx 'atlas'; then
    echo "    No docker container 'atlas' — skip live deploy"
    return 1
  fi

  if [ ! -f "${WEBAPP_JAR}" ]; then
    echo "    Building webapp (ATLAS-5423 patch)..."
    mvn -pl webapp -am package -DskipTests -q
  fi

  local atlas_home
  atlas_home=$(docker exec atlas printenv ATLAS_HOME 2>/dev/null || echo "/opt/apache-atlas-3.0.0-SNAPSHOT")
  local lib="${atlas_home}/server/webapp/atlas/WEB-INF/lib"

  echo "    Deploying patched jars to ${lib} (Java 17 via atlas.sh)..."
  docker cp "${WEBAPP_JAR}" "atlas:${lib}/atlas-webapp-3.0.0-SNAPSHOT.jar"
  if [ -f "${SERVER_COMMON_JAR}" ]; then
    docker cp "${SERVER_COMMON_JAR}" "atlas:${lib}/atlas-server-common-3.0.0-SNAPSHOT.jar"
  fi
  docker cp "${ATLAS_SH}" "atlas:/home/atlas/scripts/atlas.sh"

  export ATLAS_SERVER_JAVA_VERSION="${ATLAS_SERVER_JAVA_VERSION:-17}"
  export ATLAS_BACKEND="${ATLAS_BACKEND:-postgres}"
  docker restart atlas >/dev/null

  echo "    Waiting for Atlas admin/status HTTP 200..."
  wait_for_atlas

  docker exec -u atlas env JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-arm64}" PATH="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-arm64}/bin:$PATH" java -version 2>&1 | head -1 | tee /dev/stderr
  if ! docker exec -u atlas env JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-arm64}" java -version 2>&1 | grep -qE 'version "1[7-9]|version "[2-9][0-9]'; then
    echo "    WARN: Atlas container is not running Java 17; live log check may not reflect ATLAS-5423 patch"
    return 1
  fi

  docker exec atlas jar tf "${lib}/atlas-webapp-3.0.0-SNAPSHOT.jar" | grep -q NotificationMessageValidator
}

generate_hook_payload() {
  if [ -f /tmp/atlas5423-hook-msg.json ] && [ -s /tmp/atlas5423-hook-msg.json ]; then
    return 0
  fi
  mvn -pl intg,notification,webapp -q dependency:build-classpath -Dmdep.outputFile=/tmp/atlas5423-cp.txt
  local cp="${REPO_ROOT}/webapp/target/atlas-webapp-3.0.0-SNAPSHOT-classes.jar:${REPO_ROOT}/notification/target/classes:${REPO_ROOT}/intg/target/classes:$(cat /tmp/atlas5423-cp.txt)"
  javac -cp "${cp}" "${REPO_ROOT}/dev-support/scripts/GenInvalidHookMessage.java" -d /tmp/atlas5423-gen
  java -cp "/tmp/atlas5423-gen:${cp}" GenInvalidHookMessage 2>/dev/null > /tmp/atlas5423-hook-msg.json
}

echo "==> [1/3] Unit tests (validator + retry classification)"
mvn -pl webapp -Dtest=NotificationMessageValidatorTest test -q

echo "==> [2/3] Kafka E2E (embedded broker + hook consumer + SerialEntityProcessor)"
mvn -pl webapp -Dtest=NotificationInvalidTypenameKafkaE2ETest test -q

echo "==> [3/3] Docker Atlas live ATLAS_HOOK (optional)"
if command -v docker >/dev/null 2>&1 && deploy_patch_to_docker_atlas; then
  generate_hook_payload
  MARKER="e2e-atlas-5423-invalid-typename-$(date +%s)"
  # unique qualifiedName in payload so we can grep logs
  sed "s/e2e-atlas-5423-invalid-typename/${MARKER}/g" /tmp/atlas5423-hook-msg.json > /tmp/atlas5423-hook-msg-live.json

  docker exec -i atlas-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic ATLAS_HOOK < /tmp/atlas5423-hook-msg-live.json

  echo "    Published hook message (qualifiedName=${MARKER})"
  sleep 8

  LOG="${REPO_ROOT}/dev-support/scripts/.atlas5423-live.log"
  docker exec atlas sh -c "tail -500 ${ATLAS_HOME:-/opt/apache-atlas-3.0.0-SNAPSHOT}/logs/application.log" > "${LOG}" 2>/dev/null || true

  if grep -q "Skipping notification with invalid type name" "${LOG}"; then
    echo "    OK: Atlas log contains fail-fast skip for invalid typename"
  else
    echo "    FAIL: expected log line 'Skipping notification with invalid type name(s)' not found"
    echo "    Last NotificationHookConsumer / SerialEntityProcessor lines:"
    grep -E "NotificationHookConsumer|SerialEntityProcessor|trino_table|invalid type" "${LOG}" | tail -15 || true
    exit 1
  fi
else
  echo "    Skipped live docker step (no atlas container or deploy failed)"
fi

echo "==> OK: ATLAS-5423 local validation complete"
