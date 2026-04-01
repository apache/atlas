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

# FROM maven:3.8.6-jdk-8-slim AS stage-atlas
# LABEL maintainer="aare.puussaar@gmail.com"
# ARG VERSION=3.0.0-SNAPSHOT

# ENV	MAVEN_OPTS="-Xms4g -Xmx4g -Dhttp.socketTimeout=60000 -Dhttp.connectionTimeout=60000 -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 -Dmaven.wagon.http.retryHandler.count=3"

# WORKDIR /atlas
# COPY . /atlas/

# RUN apt-get update \
#     && apt-get install -y wget git unzip python python3 build-essential

# RUN git clone http://github.com/aarepuu/atlas.git 

# # get keycloak
# RUN mkdir -p ~/.m2/repository/org/keycloak
# RUN wget  https://atlan-public.s3.eu-west-1.amazonaws.com/artifact/keycloak-15.0.2.1.zip
# RUN unzip -o keycloak-15.0.2.1.zip -d ~/.m2/repository/org

# RUN echo "Maven Building"
# # RUN	mvn clean -DskipTests -Drat.skip=true package -Pdist \
# # 	&& mv distro/target/apache-atlas-${VERSION}-server.tar.gz /apache-atlas.tar.gz

# RUN mvn clean -pl '!test-tools,!addons/hdfs-model,!addons/hive-bridge,!addons/hive-bridge-shim,!addons/falcon-bridge-shim,!addons/falcon-bridge,!addons/sqoop-bridge,!addons/sqoop-bridge-shim,!addons/hbase-bridge,!addons/hbase-bridge-shim,!addons/hbase-testing-util,!addons/kafka-bridge,!addons/impala-hook-api,!addons/impala-bridge-shim,!addons/impala-bridge' -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipEnunciate=true package -Pdist \
# && mv distro/target/apache-atlas-${VERSION}-server.tar.gz /apache-atlas.tar.gz

FROM scratch
FROM ubuntu:22.04
ARG VERSION=3.0.0-SNAPSHOT

# COPY --from=stage-atlas /apache-atlas.tar.gz /apache-atlas.tar.gz

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install apt-utils \
    && apt-get -y install \
        wget \
        python3 \
		python3-pip \
        openjdk-8-jdk-headless \
        net-tools \
        curl \
    && cd / \
    && export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-$(dpkg --print-architecture)" \
    && apt-get clean 

RUN ln -s /usr/bin/python3 /usr/bin/python

COPY ./distro/target/apache-atlas-${VERSION}-server.tar.gz /apache-atlas.tar.gz

RUN groupadd atlas && \
	useradd -m -d /opt/atlas -s /bin/bash -g atlas atlas

USER atlas
RUN cd /opt \
	&& tar xzf /apache-atlas.tar.gz -C /opt/atlas --strip-components=1

USER root
RUN rm -rf /apache-atlas.tar.gz && \
    chown -R atlas:atlas /opt/atlas
USER atlas

RUN cd /opt/atlas/bin \
    && ./atlas_start.py -setup || true
    