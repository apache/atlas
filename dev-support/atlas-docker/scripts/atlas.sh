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

set -e
set -x

ATLAS_APPLICATION_PROPERTIES=${ATLAS_HOME}/conf/atlas-application.properties
ATLAS_USER_CREDENTIALS=${ATLAS_HOME}/conf/users-credentials.properties
ATLAS_EXPANDED_WEBAPP=${ATLAS_HOME}/server/webapp/atlas/WEB-INF

if [ ! -r "${ATLAS_APPLICATION_PROPERTIES}" ]
then
  echo "Missing readable Atlas configuration: ${ATLAS_APPLICATION_PROPERTIES}" >&2
  exit 1
fi

if [ ! -r "${ATLAS_USER_CREDENTIALS}" ]
then
  echo "Missing readable Atlas user credentials: ${ATLAS_USER_CREDENTIALS}" >&2
  exit 1
fi

if [ ! -d "${ATLAS_EXPANDED_WEBAPP}" ]
then
  echo "Missing expanded Atlas webapp: ${ATLAS_EXPANDED_WEBAPP}" >&2
  exit 1
fi

su -c "cd ${ATLAS_HOME}/bin && ./atlas_start.py" atlas
ATLAS_PID=`ps -ef  | grep -v grep | grep -i "org.apache.atlas.Atlas" | awk '{print $2}'`

# prevent the container from exiting
tail --pid=$ATLAS_PID -f /dev/null
