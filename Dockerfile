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

FROM maven:3.6.3-jdk-11-slim AS build-env
ARG PROFILE=-Pdist
ARG SKIPS="-DskipTests -Drat.skip=true -DskipCheck=true -DskipITs=true -DskipUTs=true -DskipEnunciate=true -Dcheckstyle.skip=true"

ENV ATLAS_WORKDIR=/opt/atlas

## code didn t changed
ADD 3party-licenses $ATLAS_WORKDIR/3party-licenses
ADD addons $ATLAS_WORKDIR/addons
ADD authorization $ATLAS_WORKDIR/authorization
ADD build-tools $ATLAS_WORKDIR/build-tools
ADD client $ATLAS_WORKDIR/client
ADD common $ATLAS_WORKDIR/common
ADD dev-support $ATLAS_WORKDIR/dev-support
ADD docs $ATLAS_WORKDIR/docs
ADD graphdb $ATLAS_WORKDIR/graphdb
ADD intg $ATLAS_WORKDIR/intg
ADD notification $ATLAS_WORKDIR/notification
ADD plugin-classloader $ATLAS_WORKDIR/plugin-classloader
ADD repository $ATLAS_WORKDIR/repository
ADD server-api $ATLAS_WORKDIR/server-api
ADD test-tools $ATLAS_WORKDIR/test-tools
ADD tools $ATLAS_WORKDIR/tools

## code that changed
ADD distro $ATLAS_WORKDIR/distro
ADD dashboardv2 $ATLAS_WORKDIR/dashboardv2
ADD dashboardv3 $ATLAS_WORKDIR/dashboardv3
ADD webapp $ATLAS_WORKDIR/webapp
ADD pom.xml $ATLAS_WORKDIR/pom.xml

WORKDIR $ATLAS_WORKDIR

RUN mvn clean package $SKIPS $PROFILE -pl '!docs'

RUN mkdir build
RUN cp -r distro/target/*bin.tar.gz $ATLAS_WORKDIR/build

##################################### new image #######################
FROM ubuntu:18.04
ENV ATLAS_WORKDIR=/home/ubuntu

# Legacy, maybe we can remove this
# creating user because solr doesn't like to be started as root
RUN useradd -rm -d /home/ubuntu -s /bin/bash -g root -u 1000 ubuntu

########### install #################

RUN apt-get update && apt-get install -y openjdk-8-jdk python vim

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH /usr/java/bin:/usr/local/apache-maven/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

USER ubuntu
WORKDIR $ATLAS_WORKDIR

RUN mkdir build
RUN mkdir atlas-bin

COPY --from=build-env /opt/atlas/build ./build/

RUN tar xzf build/*bin.tar.gz --strip-components 1 -C $ATLAS_WORKDIR/atlas-bin
# Clean up
RUN rm -rf build

# Set env variables, add it to the path, and start Atlas.
ENV MANAGE_LOCAL_SOLR true
ENV MANAGE_LOCAL_HBASE true

ENV PATH /home/ubuntu/atlas-bin/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

EXPOSE 21000
# This setup allow us to have direct logs
ENV DRY=true
CMD ["/bin/bash", "-c", "/home/ubuntu/atlas-bin/bin/startAtlas.sh"]
