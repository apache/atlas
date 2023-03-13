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

mkdir -p ~/.m2/repository/org/keycloak

wget  https://atlan-public.s3.eu-west-1.amazonaws.com/artifact/keycloak-15.0.2.1.zip
unzip -o keycloak-15.0.2.1.zip -d ~/.m2/repository/org

echo "Maven Building"

if [ "$1" == "build_without_dashboard" ]; then
  mvn -pl '!addons/hdfs-model,!addons/hive-bridge,!addons/hive-bridge-shim,!addons/falcon-bridge-shim,!addons/falcon-bridge,!addons/sqoop-bridge,!addons/sqoop-bridge-shim,!addons/hbase-bridge,!addons/hbase-bridge-shim,!addons/hbase-testing-util,!addons/kafka-bridge,!addons/impala-hook-api,!addons/impala-bridge-shim,!addons/impala-bridge,!dashboardv2,!dashboardv3' -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true package -Pdist
else
  mvn -pl '!addons/hdfs-model,!addons/hive-bridge,!addons/hive-bridge-shim,!addons/falcon-bridge-shim,!addons/falcon-bridge,!addons/sqoop-bridge,!addons/sqoop-bridge-shim,!addons/hbase-bridge,!addons/hbase-bridge-shim,!addons/hbase-testing-util,!addons/kafka-bridge,!addons/impala-hook-api,!addons/impala-bridge-shim,!addons/impala-bridge' -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipEnunciate=true package -Pdist
fi

echo "[DEBUG listing distro/target"
ls distro/target

echo "[DEBUG] listing local directory"
ls target

