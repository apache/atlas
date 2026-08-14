#!/bin/bash
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

# Docker healthcheck: HBase Master (16010) and RegionServer (16030) info pages.
check_url() {
  local url="$1"
  if command -v wget >/dev/null 2>&1; then
    wget -q --spider "${url}"
    return $?
  fi
  if command -v curl >/dev/null 2>&1; then
    curl -sf -o /dev/null "${url}"
    return $?
  fi
  return 1
}

check_url "http://localhost:16010/master-status" \
  && check_url "http://localhost:16030/rs-status"
