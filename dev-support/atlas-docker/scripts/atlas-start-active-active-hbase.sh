#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/docker-compose.atlas-active-active.yml" ]]; then
  ROOT_DIR="${SCRIPT_DIR}"
elif [[ -f "${SCRIPT_DIR}/../docker-compose.atlas-active-active.yml" ]]; then
  ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
else
  echo "[ERROR] Could not locate docker-compose.atlas-active-active.yml from ${SCRIPT_DIR}" >&2
  exit 1
fi
cd "${ROOT_DIR}"

COMPOSE_FILE="docker-compose.atlas-active-active.yml"
COMPOSE_FILE_MONOLITHIC="docker-compose.atlas-monolithic.yml"
ENV_BASE=".env"
ENV_AA=".env.active-active"
RUN_MODE="${RUN_MODE:-MODULAR}"
METADATA_REPLICAS="${METADATA_REPLICAS:-2}"
NOTIFICATION_REPLICAS="${NOTIFICATION_REPLICAS:-2}"
REPLICAS="${REPLICAS:-2}"

if [[ ! -f "${ENV_BASE}" || ! -f "${ENV_AA}" ]]; then
  echo "[ERROR] Missing ${ENV_BASE} or ${ENV_AA} in ${ROOT_DIR}" >&2
  exit 1
fi

if ! docker image inspect atlas-base:latest >/dev/null 2>&1; then
  echo "[0/7] atlas-base:latest not found. Building base image..."
  export DOCKER_BUILDKIT=1
  export COMPOSE_DOCKER_CLI_BUILD=1
  docker compose --env-file "${ENV_BASE}" -f docker-compose.atlas-base.yml build atlas-base
fi

echo "[1/7] Switching backend to HBase in ${ENV_AA}..."
if grep -q '^ATLAS_BACKEND=' "${ENV_AA}"; then
  sed -i '' 's/^ATLAS_BACKEND=.*/ATLAS_BACKEND=hbase/' "${ENV_AA}"
else
  printf "\nATLAS_BACKEND=hbase\n" >> "${ENV_AA}"
fi

echo "[2/7] Starting infrastructure..."
if [[ "${RUN_MODE}" == "MONOLITHIC" ]]; then
  docker compose --env-file "${ENV_BASE}" --env-file "${ENV_AA}" \
    -f "${COMPOSE_FILE_MONOLITHIC}" up -d \
    atlas-hadoop atlas-zk atlas-kafka atlas-solr atlas-backend atlas-db
else
  docker compose --env-file "${ENV_BASE}" --env-file "${ENV_AA}" \
    -f "${COMPOSE_FILE}" up -d \
    atlas-hadoop atlas-zk atlas-kafka atlas-solr atlas-backend atlas-db
fi

if [[ "${RUN_MODE}" == "MONOLITHIC" ]]; then
  echo "[3/7] Starting MONOLITHIC Atlas services..."
  RUN_MODE=MONOLITHIC docker compose --env-file "${ENV_BASE}" --env-file "${ENV_AA}" \
    -f "${COMPOSE_FILE_MONOLITHIC}" up -d --force-recreate \
    --no-deps \
    --scale atlas-monolithic-server="${REPLICAS}" \
    atlas-monolithic-server atlas-lb
else
  echo "[3/7] Running initializer..."
  docker compose --env-file "${ENV_BASE}" --env-file "${ENV_AA}" \
    -f "${COMPOSE_FILE}" up -d --force-recreate atlas-initializer

  echo "[4/7] Starting modular RUN_MODE services..."
  docker compose --env-file "${ENV_BASE}" --env-file "${ENV_AA}" \
    -f "${COMPOSE_FILE}" up -d --force-recreate \
    --scale atlas-metadata-server="${METADATA_REPLICAS}" --scale atlas-notification-proc="${NOTIFICATION_REPLICAS}" \
    atlas-metadata-server atlas-notification-proc atlas-lb
fi

echo "[5/7] Service status:"
if [[ "${RUN_MODE}" == "MONOLITHIC" ]]; then
  docker compose -f "${COMPOSE_FILE_MONOLITHIC}" ps
else
  docker compose -f "${COMPOSE_FILE}" ps
fi

echo "[6/7] Atlas admin status via LB:"
wget -q -S -O- http://localhost:21000/api/atlas/admin/status 2>&1 | tail -20 || true

echo
if [[ "${RUN_MODE}" == "MONOLITHIC" ]]; then
  echo "[DONE] MONOLITHIC Atlas started in HBase mode."
else
  echo "[DONE] Modular RUN_MODE Atlas started in HBase mode."
fi
