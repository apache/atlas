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

set -x

if [ ! -e ${ATLAS_HOME}/.setupDone ]
then
  SETUP_ATLAS=true
else
  SETUP_ATLAS=false
fi

if [ "${SETUP_ATLAS}" == "true" ]
then
  encryptedPwd=$(${ATLAS_HOME}/bin/cputil.py -g -u admin -p atlasR0cks! -s | tail -1)

  echo "admin=ADMIN::${encryptedPwd}" > ${ATLAS_HOME}/conf/users-credentials.properties

  sed -i "s/atlas.graph.storage.hostname=.*$/atlas.graph.storage.hostname=atlas-zk.example.com:2181/"             /opt/atlas/conf/atlas-application.properties
  sed -i "s/atlas.audit.hbase.zookeeper.quorum=.*$/atlas.audit.hbase.zookeeper.quorum=atlas-zk.example.com:2181/" /opt/atlas/conf/atlas-application.properties

  sed -i "s/^atlas.graph.index.search.solr.mode=cloud/# atlas.graph.index.search.solr.mode=cloud/"                                              /opt/atlas/conf/atlas-application.properties
  sed -i "s/^# *atlas.graph.index.search.solr.mode=http/atlas.graph.index.search.solr.mode=http/"                                               /opt/atlas/conf/atlas-application.properties
  sed -i "s/^.*atlas.graph.index.search.solr.http-urls=.*$/atlas.graph.index.search.solr.http-urls=http:\/\/atlas-solr.example.com:8983\/solr/" /opt/atlas/conf/atlas-application.properties

  sed -i "s/atlas.notification.embedded=.*$/atlas.notification.embedded=false/"                            /opt/atlas/conf/atlas-application.properties
  sed -i "s/atlas.kafka.zookeeper.connect=.*$/atlas.kafka.zookeeper.connect=atlas-zk.example.com:2181/"    /opt/atlas/conf/atlas-application.properties
  sed -i "s/atlas.kafka.bootstrap.servers=.*$/atlas.kafka.bootstrap.servers=atlas-kafka.example.com:9092/" /opt/atlas/conf/atlas-application.properties

  echo ""                                                     >> /opt/atlas/conf/atlas-application.properties
  echo "atlas.graph.storage.hbase.compression-algorithm=NONE" >> /opt/atlas/conf/atlas-application.properties
  echo "atlas.graph.graph.replace-instance-if-exists=true"    >> /opt/atlas/conf/atlas-application.properties

  if [ "${ATLAS_BACKEND}" == "postgres" ]
  then
    # set RDBMS as backend and entity-audit store
    sed -i "s/^atlas.graph.storage.backend=hbase2/# atlas.graph.storage.backend=hbase2/"                                                            /opt/atlas/conf/atlas-application.properties
    sed -i "s/atlas.EntityAuditRepository.impl=.*$/# atlas.EntityAuditRepository.impl=org.apache.atlas.repository.audit.HBaseBasedAuditRepository/" /opt/atlas/conf/atlas-application.properties

    cat <<EOF >> /opt/atlas/conf/atlas-application.properties

atlas.graph.storage.backend=rdbms
atlas.graph.storage.rdbms.jpa.hikari.driverClassName=org.postgresql.Driver
atlas.graph.storage.rdbms.jpa.hikari.jdbcUrl=jdbc:postgresql://atlas-db/atlas
atlas.graph.storage.rdbms.jpa.hikari.username=atlas
atlas.graph.storage.rdbms.jpa.hikari.password=atlasR0cks!
atlas.graph.storage.rdbms.jpa.hikari.maximumPoolSize=40
atlas.graph.storage.rdbms.jpa.hikari.minimumIdle=5
atlas.graph.storage.rdbms.jpa.hikari.idleTimeout=300000
atlas.graph.storage.rdbms.jpa.hikari.connectionTestQuery=select 1
atlas.graph.storage.rdbms.jpa.hikari.maxLifetime=1800000
atlas.graph.storage.rdbms.jpa.hikari.connectionTimeout=30000
atlas.graph.storage.rdbms.jpa.javax.persistence.jdbc.dialect=org.eclipse.persistence.platform.database.PostgreSQLPlatform
atlas.graph.storage.rdbms.jpa.javax.persistence.schema-generation.database.action=create
atlas.graph.storage.rdbms.jpa.javax.persistence.schema-generation.create-database-schemas=true
atlas.graph.storage.rdbms.jpa.javax.persistence.schema-generation.create-source=script
atlas.graph.storage.rdbms.jpa.javax.persistence.schema-generation.create-script-source=META-INF/postgres/create_schema.sql
atlas.EntityAuditRepository.impl=org.apache.atlas.repository.audit.rdbms.RdbmsBasedAuditRepository
EOF
  fi

  chown -R atlas:atlas ${ATLAS_HOME}/

  touch ${ATLAS_HOME}/.setupDone
fi

su -c "cd ${ATLAS_HOME}/bin && ./atlas_start.py" atlas
ATLAS_PID=`ps -ef  | grep -v grep | grep -i "org.apache.atlas.Atlas" | awk '{print $2}'`

# prevent the container from exiting
tail --pid=$ATLAS_PID -f /dev/null
