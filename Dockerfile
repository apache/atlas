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

FROM maven:3.6-slim as staging

WORKDIR /src/
COPY . .
RUN mkdir /poms/ \
    && find ./ -type d -exec mkdir -p '/poms/{}' \; \
    && find ./ -name pom.xml -exec cp -r '{}' '/poms/{}' \;

FROM maven:3.6-slim as builder

WORKDIR /src/

COPY --from=staging /poms/ ./
RUN mvn dependency:go-offline

COPY --from=staging /src/ ./

RUN export MAVEN_OPTS="-Xms2g -Xmx2g" \
    && mvn clean -DskipTests package -Pdist

FROM openjdk:11.0-slim as production

WORKDIR /app/

COPY --from=builder /src/distro/target/*.tar.gz /app/

RUN (for TARBALL in ./*-server.tar.gz; do tar -zxf "$TARBALL"; done) \
    && rm *.tar.gz \
    && ls \
    && mv apache-atlas-*/* ./ \
    && apt-get update \
    && apt-get -y install \
        dumb-init \
        python \
    && rm -rf /var/lib/apt/lists/*

RUN sed -i 's/atlas.graph.storage.backend=hbase2/atlas.graph.storage.backend=cassandra/g' conf/atlas-application.properties
RUN mv /app/conf/cassandra.yml.template /app/conf/cassandra.yml \
    && sed -i 's/127.0.0.1/cassandra/g' /app/conf/cassandra.yml

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/bin/bash", "-c", "/app/bin/atlas_start.py & tail -fF /app/logs/application.log"]