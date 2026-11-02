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

# Runs from the semantic-indexer tarball: bin/, conf/ (logback), lib/ (repair and indexer jars, all dependencies).
# ATLAS_CONF is the directory with atlas-application.properties (default: conf/ of the tarball).
INDEXER_HOME="${ATLAS_SEMANTIC_INDEXER_HOME:-$(cd "$(dirname "$0")/.." && pwd)}"
ATLAS_CONF="${ATLAS_CONF:-${INDEXER_HOME}/conf}"
LOGBACK_FILE="${ATLAS_SEMANTIC_REPAIR_LOGBACK:-${INDEXER_HOME}/conf/atlas-semantic-repair-logback.xml}"
ATLAS_SEMANTIC_REPAIR_HEAP="${ATLAS_SEMANTIC_REPAIR_HEAP:--Xms512m -Xmx1g}"
ATLAS_SEMANTIC_REPAIR_OPTS="${ATLAS_SEMANTIC_REPAIR_OPTS:-}"

JVM_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"

exec java ${ATLAS_SEMANTIC_REPAIR_HEAP} ${ATLAS_SEMANTIC_REPAIR_OPTS} ${JVM_OPENS} \
  -Dlogback.configurationFile="${LOGBACK_FILE}" \
  -Datlas.home="${INDEXER_HOME}" \
  -Datlas.conf="${ATLAS_CONF}" \
  -cp "${ATLAS_CONF}:${INDEXER_HOME}/conf:${INDEXER_HOME}/lib/*" \
  org.apache.atlas.tools.SemanticRepair "$@"
