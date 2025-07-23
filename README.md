<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Apache Atlas Overview
=====================
[![License](https://img.shields.io/:license-Apache%202-green.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/apache-atlas?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/apache-atlas/)
[![Documentation](https://img.shields.io/badge/docs-apache.org-blue.svg)](https://atlas.apache.org)
[![Wiki](https://img.shields.io/badge/atlas-wiki-orange)](https://cwiki.apache.org/confluence/display/ATLAS/)

Apache Atlas framework is an extensible set of core
foundational governance services – enabling enterprises to effectively and
efficiently meet their compliance requirements within Hadoop and allows
integration with the whole enterprise data ecosystem.

This will provide true visibility in Hadoop by using both a prescriptive
and forensic model, along with technical and operational audit as well as
lineage enriched by business taxonomical metadata.  It also enables any
metadata consumer to work inter-operably without discrete interfaces to
each other -- the metadata store is common.

The metadata veracity is maintained by leveraging Apache Ranger to prevent
non-authorized access paths to data at runtime.
Security is both role based (RBAC) and attribute based (ABAC).



#### NOTE
Apache Atlas allows contributions via pull requests (PRs) on GitHub. Alternatively, use [this](https://reviews.apache.org) to submit changes for review using the Review Board.
Also create a [atlas jira](https://issues.apache.org/jira/browse/ATLAS) to go along with the review and mention it in the pull request/review board review.


Building Atlas in Docker
=============

Instructions to build and run atlas in docker: `dev-support/atlas-docker/README.md`

Regular Build Process
=============

1. Get Atlas sources to your local directory, for example with following commands
   ```
   cd <your-local-directory>
   git clone https://github.com/apache/atlas.git
   cd atlas
   
   # Checkout the branch or tag you would like to build

   # to checkout a branch
   git checkout <branch>

   # to checkout a tag
   git checkout tags/<tag>
   ```

2. Execute the following commands to build Apache Atlas
   ```
   export MAVEN_OPTS="-Xms2g -Xmx2g"
   ```
- Apache Atlas supports building with **Java 8**, **Java 11**, and **Java 17**. Set the appropriate `JAVA_HOME` and `MAVEN_OPTS` before building.

- **For Java 8 / Java 11:**
  ```bash
  export MAVEN_OPTS="-Xms2g -Xmx2g"
  ```

- **For Java 17:**
  ```bash
  export MAVEN_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/java.nio.channels.spi=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.base/sun.security.x509=ALL-UNNAMED \
  --add-modules=java.sql -Xms2g -Xmx2g"
  ```

- After setting the correct `MAVEN_OPTS`, run:
   ```bash
    mvn clean install
    mvn clean package -Pdist
   ```

3. After above build commands successfully complete, you should see the following files
   ```
   distro/target/apache-atlas-<version>-bin.tar.gz
   distro/target/apache-atlas-<version>-hbase-hook.tar.gz
   distro/target/apache-atlas-<version>-hive-hook.tar.gz
   distro/target/apache-atlas-<version>-impala-hook.tar.gz
   distro/target/apache-atlas-<version>-kafka-hook.tar.gz
   distro/target/apache-atlas-<version>-server.tar.gz
   distro/target/apache-atlas-<version>-sources.tar.gz
   distro/target/apache-atlas-<version>-sqoop-hook.tar.gz
   distro/target/apache-atlas-<version>-storm-hook.tar.gz
   distro/target/apache-atlas-<version>-falcon-hook.tar.gz
   distro/target/apache-atlas-<version>-couchbase-hook.tar.gz
   ```

4. For more details on installing and running Apache Atlas, please refer to https://atlas.apache.org/#/Installation
