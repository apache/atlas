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

set -euo pipefail

CONTAINER_NAME="${ATLAS_IT_RDBMS_CONTAINER_NAME:-atlas-it-postgres}"
IMAGE="${ATLAS_IT_RDBMS_IMAGE:-postgres:13.21}"
HOST_PORT="${ATLAS_IT_RDBMS_HOSTPORT:-15432}"
USERNAME="${ATLAS_IT_RDBMS_USERNAME:-atlas}"
PASSWORD="${ATLAS_IT_RDBMS_PASSWORD:-atlasR0cks!}"
DB_NAME="${ATLAS_IT_RDBMS_DBNAME:-atlas}"
MAX_CONNECTIONS="${ATLAS_IT_RDBMS_MAX_CONNECTIONS:-300}"

docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

docker run -d --name "${CONTAINER_NAME}" \
  -e "POSTGRES_DB=${DB_NAME}" \
  -e "POSTGRES_USER=${USERNAME}" \
  -e "POSTGRES_PASSWORD=${PASSWORD}" \
  -p "${HOST_PORT}:5432" \
  "${IMAGE}" -c "max_connections=${MAX_CONNECTIONS}" >/dev/null

for i in $(seq 1 60); do
  docker exec "${CONTAINER_NAME}" pg_isready -U "${USERNAME}" -d "${DB_NAME}" >/dev/null 2>&1 && exit 0
  sleep 1
done

echo "Postgres container did not become ready in time" >&2
docker logs "${CONTAINER_NAME}" || true
exit 1


