#!/usr/bin/env bash
# Copy freshly built semantic artifacts into a running Atlas container.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
CONTAINER="${1:-atlas}"
INDEXER_CONTAINER="${2:-atlas-semantic-indexer}"

REPO_JAR=$(ls -t "${SRC_ROOT}"/repository/target/atlas-repository-*.jar 2>/dev/null | grep -v tests | grep -v sources | head -1)
INDEXER_JAR=$(ls -t "${SRC_ROOT}"/services/atlas-semantic-indexer/target/atlas-semantic-indexer-*.jar 2>/dev/null | grep -v sources | head -1)
REPAIR_JAR=$(ls -t "${SRC_ROOT}"/tools/atlas-semantic-repair/target/atlas-semantic-repair-tool-*.jar 2>/dev/null | grep -v sources | head -1)

for f in "${REPO_JAR}" "${INDEXER_JAR}" "${REPAIR_JAR}"; do
  if [ ! -f "${f}" ]; then
    echo "Missing build artifact: ${f}" >&2
    echo "Run: mvn -pl services/atlas-semantic-indexer,tools/atlas-semantic-repair,repository -am package -DskipTests -DskipEnunciate=true" >&2
    exit 1
  fi
done

WEBINF_LIB="/opt/atlas/server/webapp/atlas/WEB-INF/lib"
docker exec "${CONTAINER}" mkdir -p /opt/atlas/lib /opt/atlas/bin

echo "==> Removing legacy in-process semantic indexing classes from repository jar (if present) ..."
docker cp "${REPO_JAR}" "${CONTAINER}:/tmp/atlas-repository.jar"
docker exec "${CONTAINER}" sh -c '
  set -e
  cd /tmp
  rm -rf repo-patch && mkdir repo-patch
  cd repo-patch
  jar xf /tmp/atlas-repository.jar
  rm -f org/apache/atlas/semantic/SemanticIndexing*.class 2>/dev/null || true
  jar cf /tmp/atlas-repository-patched.jar .
'

docker exec "${CONTAINER}" sh -c "rm -f ${WEBINF_LIB}/atlas-repository-*.jar"
TMP_PATCHED="$(mktemp)"
trap 'rm -f "${TMP_PATCHED}"' EXIT
docker cp "${CONTAINER}:/tmp/atlas-repository-patched.jar" "${TMP_PATCHED}"
docker cp "${TMP_PATCHED}" "${CONTAINER}:${WEBINF_LIB}/$(basename "${REPO_JAR}")"

docker cp "${INDEXER_JAR}" "${CONTAINER}:/opt/atlas/lib/"
docker cp "${REPAIR_JAR}" "${CONTAINER}:/opt/atlas/lib/"
docker cp "${SRC_ROOT}/services/atlas-semantic-indexer/scripts/atlas_semantic_indexer.sh" "${CONTAINER}:/opt/atlas/bin/"
docker cp "${SRC_ROOT}/tools/atlas-semantic-repair/scripts/atlas_semantic_repair.sh" "${CONTAINER}:/opt/atlas/bin/"
REPAIR_LOGBACK="${SCRIPT_DIR}/../config/atlas-semantic-repair-logback.xml"
if [ -f "${REPAIR_LOGBACK}" ]; then
  docker exec "${CONTAINER}" mkdir -p /opt/atlas/conf
  docker cp "${REPAIR_LOGBACK}" "${CONTAINER}:/opt/atlas/conf/atlas-semantic-repair-logback.xml"
fi
docker exec "${CONTAINER}" chmod +x /opt/atlas/bin/atlas_semantic_indexer.sh /opt/atlas/bin/atlas_semantic_repair.sh
# Skip when compose bind-mounts atlas-logback.xml (docker cp fails with "device or resource busy").
if [ -f "${SCRIPT_DIR}/../config/atlas-logback.xml" ] && ! docker inspect "${CONTAINER}" --format '{{range .Mounts}}{{if eq .Destination "/opt/atlas/conf/atlas-logback.xml"}}mounted{{end}}{{end}}' | grep -q mounted; then
  docker cp "${SCRIPT_DIR}/../config/atlas-logback.xml" "${CONTAINER}:/opt/atlas/conf/atlas-logback.xml"
fi

resolve_indexer_home() {
  local container="$1"
  local home=""

  if docker exec "${container}" test -x /opt/atlas-semantic-indexer/bin/atlas_semantic_indexer.sh 2>/dev/null; then
    home="/opt/atlas-semantic-indexer"
  elif docker exec "${container}" test -x /opt/apache-atlas-3.0.0-SNAPSHOT-semantic-indexer/bin/atlas_semantic_indexer.sh 2>/dev/null; then
    home="/opt/apache-atlas-3.0.0-SNAPSHOT-semantic-indexer"
  elif docker exec "${container}" test -x /opt/apache-atlas-3.0.0-SNAPSHOT/bin/atlas_semantic_indexer.sh 2>/dev/null; then
    home="/opt/apache-atlas-3.0.0-SNAPSHOT"
  elif docker exec "${container}" test -d /opt/atlas-semantic-indexer/lib 2>/dev/null; then
    home="/opt/atlas-semantic-indexer"
  elif docker exec "${container}" test -d /opt/apache-atlas-3.0.0-SNAPSHOT/server/webapp/atlas/WEB-INF/lib 2>/dev/null; then
    home="/opt/apache-atlas-3.0.0-SNAPSHOT"
  fi

  echo "${home}"
}

patch_indexer_container() {
  local container="$1"
  if ! docker ps -a --format '{{.Names}}' | grep -qx "${container}"; then
    echo "==> Skipping ${container} (container not found)"
    return 0
  fi
  if ! docker ps --format '{{.Names}}' | grep -qx "${container}"; then
    echo "==> Starting ${container} to apply patch ..."
    docker start "${container}" >/dev/null
    sleep 2
  fi

  local indexer_home
  indexer_home="$(resolve_indexer_home "${container}")"
  if [ -z "${indexer_home}" ]; then
    echo "==> Could not resolve semantic indexer home in ${container}" >&2
    return 1
  fi

  local legacy_lib="${indexer_home}/server/webapp/atlas/WEB-INF/lib"
  echo "==> Patching semantic indexer container ${container} (home=${indexer_home}) ..."

  if docker exec "${container}" test -d "${legacy_lib}" 2>/dev/null; then
    docker cp "${REPO_JAR}" "${container}:${legacy_lib}/$(basename "${REPO_JAR}")"
    docker cp "${INDEXER_JAR}" "${container}:${legacy_lib}/$(basename "${INDEXER_JAR}")"
  elif docker exec "${container}" test -d "${indexer_home}/lib" 2>/dev/null; then
    docker cp "${REPO_JAR}" "${container}:${indexer_home}/lib/$(basename "${REPO_JAR}")"
    docker cp "${INDEXER_JAR}" "${container}:${indexer_home}/lib/$(basename "${INDEXER_JAR}")"
  else
    echo "==> Could not find indexer lib directory in ${container}" >&2
    return 1
  fi

  docker exec "${container}" mkdir -p "${indexer_home}/bin"
  docker cp "${SRC_ROOT}/services/atlas-semantic-indexer/scripts/atlas_semantic_indexer.sh" "${container}:${indexer_home}/bin/"
  docker exec "${container}" chmod +x "${indexer_home}/bin/atlas_semantic_indexer.sh"
  INDEXER_LOGBACK="${SCRIPT_DIR}/../config/atlas-semantic-indexer-logback.xml"
  if [ -f "${INDEXER_LOGBACK}" ]; then
    docker exec "${container}" mkdir -p "${indexer_home}/conf"
    docker cp "${INDEXER_LOGBACK}" "${container}:${indexer_home}/conf/atlas-semantic-indexer-logback.xml"
  fi
  docker exec "${container}" sh -c "[ -e /opt/atlas-semantic-indexer ] || ln -sf ${indexer_home} /opt/atlas-semantic-indexer" || true
}

echo "==> Patched ${CONTAINER} with:"
echo "    ${REPO_JAR}"
echo "    ${INDEXER_JAR}"
echo "    ${REPAIR_JAR}"

patch_indexer_container "${INDEXER_CONTAINER}"
echo "Restart semantic indexer to pick up changes: docker restart ${INDEXER_CONTAINER}"
