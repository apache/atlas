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

set -e

ATLAS_HOME="${ATLAS_HOME:-/opt/atlas}"
SEMANTIC_OVERLAY="${ATLAS_SEMANTIC_CONFIG:-/home/atlas/config/atlas-semantic-docker.properties}"
# Bind-mounted conf/ files are root-owned; use a writable path for merged runtime config.
RUNTIME_CONF="${ATLAS_RUNTIME_CONF:-${ATLAS_HOME}/conf/runtime}"

if [ ! -f "${SEMANTIC_OVERLAY}" ]; then
  exit 0
fi

mkdir -p "${RUNTIME_CONF}"
cp "${ATLAS_HOME}/conf/atlas-application.properties" "${RUNTIME_CONF}/atlas-application.properties"
if [ -f "${ATLAS_HOME}/conf/users-credentials.properties" ]; then
  cp "${ATLAS_HOME}/conf/users-credentials.properties" "${RUNTIME_CONF}/users-credentials.properties"
fi

sed -i '/^atlas.search.semantic\./d;/^atlas.semantic.indexer\./d' \
  "${RUNTIME_CONF}/atlas-application.properties"
echo "" >> "${RUNTIME_CONF}/atlas-application.properties"
cat "${SEMANTIC_OVERLAY}" >> "${RUNTIME_CONF}/atlas-application.properties"

export ATLAS_CONF="${RUNTIME_CONF}"
