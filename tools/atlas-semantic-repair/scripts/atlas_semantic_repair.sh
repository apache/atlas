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

ATLAS_HOME="${ATLAS_HOME:-$(cd "$(dirname "$0")/../../.." && pwd)}"
WEBINF="${ATLAS_HOME}/server/webapp/atlas/WEB-INF"

if [ -z "${ATLAS_CONF:-}" ]; then
  if [ -f "${ATLAS_HOME}/conf/runtime/atlas-application.properties" ]; then
    ATLAS_CONF="${ATLAS_HOME}/conf/runtime"
  else
    ATLAS_CONF="${ATLAS_HOME}/conf"
  fi
fi

CLASSPATH="${ATLAS_CONF}:${WEBINF}/classes:${WEBINF}/lib/*"
if [ -d "${ATLAS_HOME}/lib" ]; then
  CLASSPATH="${CLASSPATH}:${ATLAS_HOME}/lib/*"
fi

JVM_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOGBACK_FILE="${ATLAS_SEMANTIC_REPAIR_LOGBACK:-${ATLAS_HOME}/conf/atlas-semantic-repair-logback.xml}"
if [ ! -f "${LOGBACK_FILE}" ] && [ -f "${SCRIPT_DIR}/../conf/atlas-semantic-repair-logback.xml" ]; then
  LOGBACK_FILE="${SCRIPT_DIR}/../conf/atlas-semantic-repair-logback.xml"
fi
if [ ! -f "${LOGBACK_FILE}" ] && [ -f "${ATLAS_HOME}/conf/atlas-semantic-indexer-logback.xml" ]; then
  LOGBACK_FILE="${ATLAS_HOME}/conf/atlas-semantic-indexer-logback.xml"
fi
LOGBACK_OPT=""
if [ -f "${LOGBACK_FILE}" ]; then
  LOGBACK_OPT="-Dlogback.configurationFile=${LOGBACK_FILE}"
fi

exec java ${JVM_OPENS} ${LOGBACK_OPT} -Datlas.home="${ATLAS_HOME}" \
  -Datlas.conf="${ATLAS_CONF}" \
  -cp "${CLASSPATH}" \
  org.apache.atlas.tools.SemanticRepair "$@"
