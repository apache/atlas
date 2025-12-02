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

FROM scratch
FROM ubuntu:22.04
LABEL maintainer="engineering@atlan.com"
ARG VERSION=3.0.0-SNAPSHOT

COPY distro/target/apache-atlas-3.0.0-SNAPSHOT-server.tar.gz  /apache-atlas-3.0.0-SNAPSHOT-server.tar.gz

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install apt-utils \
    && apt-get -y install \
        wget \
        python2 \
        openjdk-17-jdk-headless \
        patch \
        netcat \
        curl \
    && cd / \
    && export MAVEN_OPTS="-Xms2g -Xmx2g" \
    && export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \
    && tar -xzvf /apache-atlas-3.0.0-SNAPSHOT-server.tar.gz -C /opt \
    && mv /opt/apache-atlas-${VERSION} /opt/apache-atlas \
    && apt-get clean \
    && rm -rf /apache-atlas-3.0.0-SNAPSHOT-server.tar.gz

RUN apt install unzip

RUN ln -s /usr/bin/python2 /usr/bin/python

COPY atlas-hub/atlas_start.py.patch atlas-hub/atlas_config.py.patch /opt/apache-atlas/bin/
COPY atlas-hub/pre-conf/atlas-logback.xml /opt/apache-atlas/conf/
COPY atlas-hub/pre-conf/atlas-auth/ /opt/apache-atlas/conf/

RUN mkdir /opt/apache-atlas/libext
RUN curl https://repo1.maven.org/maven2/org/jolokia/jolokia-jvm/1.6.2/jolokia-jvm-1.6.2-agent.jar -o /opt/apache-atlas/libext/jolokia-jvm-agent.jar

RUN cd /opt/apache-atlas/bin \
    && ./atlas_start.py -setup || true

VOLUME ["/opt/apache-atlas/conf", "/opt/apache-atlas/logs"]
