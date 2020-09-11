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


if [ ! -e ${ATLAS_HOME}/.setupDone ]
then
  SETUP_ATLAS=true
else
  SETUP_ATLAS=false
fi

if [ "${SETUP_ATLAS}" == "true" ]
then
  encryptedPwd=$(${ATLAS_HOME}/bin/cputil.py -g -u admin -p atlasR0cks! -s)

  echo "admin=ADMIN::${encryptedPwd}" > ${ATLAS_HOME}/conf/users-credentials.properties

  chown -R atlas:atlas ${ATLAS_HOME}/

  touch ${ATLAS_HOME}/.setupDone
fi

su -c "cd ${ATLAS_HOME}/bin && ./atlas_start.py" atlas

# prevent the container from exiting
/bin/bash
