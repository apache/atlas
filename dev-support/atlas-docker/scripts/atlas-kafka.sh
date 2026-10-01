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

if [ ! -e ${KAFKA_HOME}/.setupDone ]
then
  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" kafka
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" kafka
  su -c "chmod 0600 ~/.ssh/authorized_keys" kafka

  echo "ssh" > /etc/pdsh/rcmd_default

  ${ATLAS_SCRIPTS}/atlas-kafka-setup.sh

  touch ${KAFKA_HOME}/.setupDone
fi

# After an unclean shutdown (SIGKILL, OOM, disk full, docker restart while the
# ZK session is still live) ZooKeeper keeps this broker's ephemeral /brokers/ids
# node. kafka-server-start then exits with NodeExistsException and the container
# stays down. Wait for the old session to expire; if it does not, delete the
# stale node — this stack has a single Kafka broker, and this process is it.
clear_stale_broker_zk_registration() {
  local props="${KAFKA_HOME}/config/server.properties"
  local zk_shell="${KAFKA_HOME}/bin/zookeeper-shell.sh"
  local zk broker_id listing i

  if [ ! -x "${zk_shell}" ] || [ ! -f "${props}" ]; then
    return 0
  fi

  zk=$(awk -F= '/^zookeeper.connect=/{print $2}' "${props}" | tail -1)
  broker_id=$(awk -F= '/^broker.id=/{print $2}' "${props}" | tail -1)
  zk=${zk:-atlas-zk.example.com:2181}
  broker_id=${broker_id:-0}

  broker_id_registered() {
    listing=$(timeout 8 bash -c "echo ls /brokers/ids | '${zk_shell}' '${zk}'" 2>/dev/null || true)
    echo "${listing}" | grep -E '^\[' | grep -qw "${broker_id}"
  }

  if ! broker_id_registered; then
    return 0
  fi

  echo "atlas-kafka: waiting for stale ZooKeeper registration /brokers/ids/${broker_id} to expire"
  for i in $(seq 1 15); do
    sleep 2
    if ! broker_id_registered; then
      echo "atlas-kafka: /brokers/ids/${broker_id} is free"
      return 0
    fi
  done

  echo "atlas-kafka: deleting stale ZooKeeper registration /brokers/ids/${broker_id}"
  timeout 8 bash -c "echo delete /brokers/ids/${broker_id} | '${zk_shell}' '${zk}'" || true
}

clear_stale_broker_zk_registration

su -c "cd ${KAFKA_HOME} && CLASSPATH=${KAFKA_HOME}/config ./bin/kafka-server-start.sh config/server.properties" kafka
