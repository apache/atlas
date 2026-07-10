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

service ssh start

if [ ! -e ${HBASE_HOME}/.setupDone ]
then
  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" hbase
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" hbase
  su -c "chmod 0600 ~/.ssh/authorized_keys" hbase

  echo "ssh" > /etc/pdsh/rcmd_default

  ${ATLAS_SCRIPTS}/atlas-hbase-setup.sh

  touch ${HBASE_HOME}/.setupDone
fi

su -c "${HBASE_HOME}/bin/start-hbase.sh" hbase

echo "Waiting for HBase Master and RegionServer (up to 180s)..."
READY=false
for attempt in $(seq 1 90); do
  if ${ATLAS_SCRIPTS}/atlas-hbase-healthcheck.sh 2>/dev/null; then
    echo "HBase master and regionserver ready (~$((attempt * 2))s)"
    READY=true
    break
  fi
  sleep 2
done

if [ "${READY}" != "true" ]; then
  echo "HBase health endpoints not ready within 180s" >&2
  ls -la ${HBASE_HOME}/logs/ 2>/dev/null || true
  tail -80 ${HBASE_HOME}/logs/* 2>/dev/null || true
  exit 1
fi

# Keep container alive while HBase JVMs are running (do not tail only HMaster PID).
while true; do
  MASTER_PID=$(ps -ef | grep -v grep | grep -i "org.apache.hadoop.hbase.master.HMaster" | awk '{print $2}')
  RS_PID=$(ps -ef | grep -v grep | grep -i "org.apache.hadoop.hbase.regionserver.HRegionServer" | awk '{print $2}')

  if [ -z "${MASTER_PID}" ] && [ -z "${RS_PID}" ]; then
    echo "HBase master and regionserver processes exited" >&2
    tail -80 ${HBASE_HOME}/logs/* 2>/dev/null || true
    exit 1
  fi

  sleep 30
done
