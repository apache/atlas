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

resolve_indexer_home() {
  if [ -x /opt/atlas-semantic-indexer/bin/atlas_semantic_indexer.sh ]; then
    echo /opt/atlas-semantic-indexer
  elif [ -x /opt/apache-atlas-3.0.0-SNAPSHOT/bin/atlas_semantic_indexer.sh ]; then
    echo /opt/apache-atlas-3.0.0-SNAPSHOT
  else
    echo "${ATLAS_SEMANTIC_INDEXER_HOME:-/opt/atlas-semantic-indexer}"
  fi
}

RESOLVED_HOME="$(resolve_indexer_home)"
if [ ! -x "${ATLAS_SEMANTIC_INDEXER_HOME:-}/bin/atlas_semantic_indexer.sh" ]; then
  export ATLAS_SEMANTIC_INDEXER_HOME="${RESOLVED_HOME}"
fi
if [ ! -x "${ATLAS_SEMANTIC_INDEXER_HOME}/bin/atlas_semantic_indexer.sh" ]; then
  echo "Semantic indexer script not found under ${ATLAS_SEMANTIC_INDEXER_HOME}" >&2
  exit 1
fi
if [ "${ATLAS_SEMANTIC_INDEXER_HOME}" != "/opt/atlas-semantic-indexer" ] && [ ! -e /opt/atlas-semantic-indexer ]; then
  ln -sf "${ATLAS_SEMANTIC_INDEXER_HOME}" /opt/atlas-semantic-indexer 2>/dev/null || true
fi

export ATLAS_HOME="${ATLAS_SEMANTIC_INDEXER_HOME}"
export ATLAS_SEMANTIC_CONFIG="${ATLAS_SEMANTIC_CONFIG:-/home/atlas/config/atlas-semantic-docker.properties}"
export ATLAS_RUNTIME_CONF="${ATLAS_RUNTIME_CONF:-/home/atlas/conf/runtime}"
mkdir -p "${ATLAS_RUNTIME_CONF}"

/home/atlas/scripts/merge-semantic-config.sh

export ATLAS_CONF="${ATLAS_CONF:-${ATLAS_RUNTIME_CONF}}"

exec "${ATLAS_SEMANTIC_INDEXER_HOME}/bin/atlas_semantic_indexer.sh"
