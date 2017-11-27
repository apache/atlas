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

Follow the instructions below to rebuid Atlas index data from the data
stored in data-store (like HBase).

Please note that these steps have been verified in an Atlas environment
with Titan 0.5.4, HBase and Solr. For other variations, please refer to
the relevant documentation of the graph DB.

1. Setup titan-0.5.4 in the host where Atlas is installed
   - Download titan-0.5.4 Hadoop-2 distribution from http://s3.thinkaurelius.com/downloads/titan/titan-0.5.4-hadoop2.zip
   - cd /tmp/
   - unzip titan-0.5.4-hadoop2.zip

2. Add Atlas index repair kit to titan-0.5.4 installation, by running the following commands:
   - cd /tmp/titan-0.5.4-hadoop2
   - tar xvf atlas-index-repair-kit.tar

3. Update atlas-conf/atlas-titan.properties with details to connect to data-store (like HBase). For example:
     storage.backend=hbase
     storage.hostname=fqdn
     storage.hbase.table=atlas_titan

4. Update bin/atlas-gremlin.sh to set the following variables at the top. For example:
     ATLAS_WEBAPP_DIR=/home/atlas/atlas-server/server/webapp
     STORE_CONF_DIR=/etc/hbase/conf

5. If Atlas is run in a kerberized environment, setup the following:
   5.1. Update atlas-conf/atlas-titan.properties with necessary kerberos details for the data-store and index. For example:
         hbase.security.authentication=kerberos
         hbase.security.authorization=true
         hbase.rpc.engine=org.apache.hadoop.hbase.ipc.SecureRpcEngine
         index.search.backend=solr5
         index.search.solr.mode=cloud
         index.search.solr.zookeeper-url=fqdn:2181/infra-solr

   5.2. Copy necessary configuration files from the deployment to atlas-conf folder. For example:
         hadoop-client/conf/core-site.xml
         hadoop-client/conf/hdfs-site.xml
         hadoop-client/conf/yarn-site.xml
         ambari-infra-solr/conf/infra_solr_jaas.conf
         ambari-infra-solr/conf/security.json

   5.3. Update bin/atlas-gremlin.sh to set the following variable at the top. For example:
         JAAS_CONF_FILE=infra_solr_jaas.conf

   5.4. Kinit as atlas user with the command like:
         kinit -kt /etc/security/keytabs/atlas.service.keytab atlas/fqdn@EXAMPLE.COM

6. Ensure home directory for 'atlas' user exists in HDFS and this directory is owned by 'atlas' user
     su hdfs
     hdfs dfs -mkdir /user/atlas
     hdfs dfs -chown atlas:hdfs -R /user/atlas

7. Start Gremlin shell by executing the following command:
     bin/atlas-gremlin.sh bin/atlas-index-repair.groovy

8. Start index repair by entering the following in the Gremlin shell:
     repairAtlasIndex("atlas-conf/atlas-titan.properties")
