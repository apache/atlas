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
#

set -euo pipefail

show_usage() {
  cat <<'USAGE'
Usage: atlas-gremlin-cli.sh [options] [-- gremlin-cli-args]

Environment variables expected (set by user or system):
  ATLAS_CONF            Path to Atlas config directory (required unless -h)
  ATLAS_CLASSPATH       Optional classpath from an Atlas server (appended before tool jars)

Examples:
  ATLAS_CONF=/etc/atlas/conf ./atlas-gremlin-cli.sh -q "g.V().limit(5)"
  ./atlas-gremlin-cli.sh --help
USAGE
}

# quick -h/--help handling
for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    show_usage
    exit 0
  fi
done

# Find java binary
if [ -n "${JAVA_HOME:-}" ]; then
  JAVA_BIN="${JAVA_HOME}/bin/java"
else
  JAVA_BIN="$(command -v java || true)"
fi

if [ -z "${JAVA_BIN}" ] || [ ! -x "${JAVA_BIN}" ]; then
  echo "java not found. Please set JAVA_HOME or ensure java is on PATH." >&2
  exit 1
fi

# Require ATLAS_CONF by default (keeps behaviour same as original unless user requests help)
if [ -z "${ATLAS_CONF:-}" ]; then
  echo "ATLAS_CONF is not set. Example: export ATLAS_CONF=/etc/atlas/conf" >&2
  echo "This script will set: -Datlas.conf=\$ATLAS_CONF" >&2
  exit 2
fi

LOGFILE_DIR="${LOGFILE_DIR:-/tmp/}"
LOGFILE_NAME="${LOGFILE_NAME:-atlas-gremlin-cli.log}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# In the distro tarball, the CLI jar is under ./lib next to this script.
if [[ ! -d "${SCRIPT_DIR}/lib" ]]; then
  echo "Could not locate ./lib next to this script. Are you running from the atlas-gremlin-cli tarball?" >&2
  exit 3
fi
TOOL_CP="${SCRIPT_DIR}/lib/*"

if [[ -n "${ATLAS_CLASSPATH:-}" ]]; then
  CLASSPATH="${ATLAS_CLASSPATH}:${TOOL_CP}"
else
  CLASSPATH="${TOOL_CP}"
fi

LOGBACK_CFG="${SCRIPT_DIR}/atlas-logback.xml"
if [[ ! -f "${LOGBACK_CFG}" ]]; then
  echo "Could not locate ${LOGBACK_CFG}. Are you running from the atlas-gremlin-cli tarball?" >&2
  exit 4
fi
exec "${JAVA_BIN}" \
  -cp "${CLASSPATH}" \
  -Dlogback.configurationFile="${LOGBACK_CFG}" \
  -Datlas.log.dir="${LOGFILE_DIR}" \
  -Datlas.log.file="${LOGFILE_NAME}" \
  -Datlas.conf="${ATLAS_CONF}" \
  org.apache.atlas.tools.GremlinCli "$@"

