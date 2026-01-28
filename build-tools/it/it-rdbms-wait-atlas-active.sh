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

STATUS_URL="${ATLAS_IT_STATUS_URL:-http://localhost:31000/api/atlas/admin/status}"
MAX_SECONDS="${ATLAS_IT_WAIT_ACTIVE_SECONDS:-180}"

for i in $(seq 1 "${MAX_SECONDS}"); do
  out="$(curl -fsS "${STATUS_URL}" 2>/dev/null || true)"
  echo "${out}" | grep -q ACTIVE && exit 0
  sleep 1
done

echo "Atlas did not become ACTIVE in time" >&2
curl -v "${STATUS_URL}" || true
exit 1


