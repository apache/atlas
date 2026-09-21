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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -n "${ATLAS_SEMANTIC_INDEXER_HOME:-}" ]; then
  SI_HOME="${ATLAS_SEMANTIC_INDEXER_HOME}"
elif [ -d "${SCRIPT_DIR}/../lib" ]; then
  SI_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"
else
  SI_HOME="${ATLAS_HOME:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
fi

ATLAS_HOME="${ATLAS_HOME:-${SI_HOME}}"
ATLAS_CONF="${ATLAS_CONF:-${ATLAS_HOME}/conf}"

WEBINF="${ATLAS_HOME}/server/webapp/atlas/WEB-INF"
# Prefer full WEB-INF/lib (Docker full-server layout). Slim lib/ is only used when it is a complete dependency set.
if [ -d "${WEBINF}/lib" ] && compgen -G "${WEBINF}/lib/*.jar" >/dev/null; then
  CLASSPATH="${ATLAS_CONF}:${ATLAS_HOME}/conf:${WEBINF}/classes:${WEBINF}/lib/*"
  if [ -d "${ATLAS_HOME}/libext" ]; then
    CLASSPATH="${CLASSPATH}:${ATLAS_HOME}/libext/*"
  fi
  if [ -d "${ATLAS_HOME}/lib" ]; then
    CLASSPATH="${CLASSPATH}:${ATLAS_HOME}/lib/*"
  fi
elif [ -d "${SI_HOME}/lib" ] && compgen -G "${SI_HOME}/lib/atlas-semantic-indexer-"*.jar >/dev/null \
     && [ "$(find "${SI_HOME}/lib" -maxdepth 1 -name '*.jar' 2>/dev/null | wc -l | tr -d ' ')" -gt 20 ]; then
  CLASSPATH="${ATLAS_CONF}:${SI_HOME}/conf:${SI_HOME}/lib/*"
elif [ -d "${SI_HOME}/lib" ]; then
  CLASSPATH="${ATLAS_CONF}:${SI_HOME}/conf:${SI_HOME}/lib/*"
else
  echo "Could not resolve semantic indexer classpath under ${SI_HOME}" >&2
  exit 1
fi

JVM_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"

LOGBACK_FILE="${ATLAS_SEMANTIC_INDEXER_LOGBACK:-${ATLAS_HOME}/conf/atlas-semantic-indexer-logback.xml}"
if [ ! -f "${LOGBACK_FILE}" ] && [ -f "${ATLAS_HOME}/../config/atlas-semantic-indexer-logback.xml" ]; then
  LOGBACK_FILE="${ATLAS_HOME}/../config/atlas-semantic-indexer-logback.xml"
fi
LOGBACK_OPT=""
if [ -f "${LOGBACK_FILE}" ]; then
  LOGBACK_OPT="-Dlogback.configurationFile=${LOGBACK_FILE}"
fi

exec java ${JVM_OPENS} ${LOGBACK_OPT} \
  -Datlas.home="${ATLAS_HOME}" \
  -Datlas.conf="${ATLAS_CONF}" \
  -cp "${CLASSPATH}" \
  org.apache.atlas.semantic.indexer.SemanticIndexer "$@"
