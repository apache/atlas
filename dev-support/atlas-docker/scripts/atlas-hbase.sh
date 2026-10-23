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

# Give SSH daemon time to fully bind before HBase tries to use it.
# On first run the key-generation/setup loop acts as a natural delay;
# on subsequent runs (setupDone exists) we need an explicit wait.
sleep 5

if [ ! -e ${HBASE_HOME}/.setupDone ]
then
  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" hbase
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" hbase
  su -c "chmod 0600 ~/.ssh/authorized_keys" hbase

  echo "ssh" > /etc/pdsh/rcmd_default

  ${ATLAS_SCRIPTS}/atlas-hbase-setup.sh

  touch ${HBASE_HOME}/.setupDone
fi

# Wait for ZooKeeper to be fully accepting connections before starting HBase.
# depends_on: service_started only means the ZK container is up — not that ZK
# is ready.  HBase master aborts if ZK is not yet accepting connections.
# Uses bash built-in /dev/tcp (no netcat required in the image).
echo "Waiting for ZooKeeper to be ready..."
ZK_WAIT=0
until bash -c "echo >/dev/tcp/atlas-zk.example.com/2181" 2>/dev/null; do
    sleep 2
    ZK_WAIT=$((ZK_WAIT + 2))
    echo "  ...ZooKeeper not ready yet (${ZK_WAIT}s)"
    if [ $ZK_WAIT -ge 120 ]; then
        echo "ERROR: ZooKeeper did not become ready after 120s" >&2
        exit 1
    fi
done
echo "ZooKeeper is ready (${ZK_WAIT}s)"

# Debug: verify Java is reachable for the hbase user
echo "[debug] JAVA_HOME=${JAVA_HOME}"
su -c "java -version" hbase 2>&1 || echo "[debug] java not found for hbase user"
echo "[debug] HBase logs dir: ${HBASE_HOME}/logs/"
ls -la ${HBASE_HOME}/logs/ 2>/dev/null || true

# Run HBase master in the foreground so all output goes directly to docker logs.
# This replaces the SSH/daemon approach which silently fails in Docker.
echo "Starting HBase Master in foreground..."
su -c "${HBASE_HOME}/bin/hbase master start" hbase &
HBASE_BG_PID=$!

# Also start the regionserver in background
sleep 10
su -c "${HBASE_HOME}/bin/hbase-daemon.sh start regionserver" hbase

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
