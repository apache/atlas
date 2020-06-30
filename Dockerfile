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

FROM ubuntu:18.04

# --build-arg MVN_JOB=8 to built with option -T 8 default to 1
ARG MVN_JOB=1
# --build-arg BRANCH=master to build specific branc default to master
ARG BRANCH=master
# creating user because solr doesn't like to be started as root
RUN useradd -rm -d /home/ubuntu -s /bin/bash -g root -u 1000 ubuntu

# Install Git, which is missing from the Ubuntu base images.
RUN apt-get update && apt-get install -y git python vim

# Install Java.
RUN apt-get update && apt-get install -y openjdk-8-jdk
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# Install Maven.

RUN apt-get update && apt-get install -y maven
ENV MAVEN_HOME /usr/share/maven

# Add Java and Maven to the path.
ENV PATH /usr/java/bin:/usr/local/apache-maven/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ARG PROFILE=-Pdist
# switch to user ubuntu
USER ubuntu
# Working directory
WORKDIR /home/ubuntu

# Memory requirements & -Drat.skip=true to disable license check
ENV MAVEN_OPTS "-Xms2g -Xmx2g -Drat.skip=true"
# RUN export MAVEN_OPTS="-Xms2g -Xmx2g"

# Pull down Atlas and build it into /root/atlas-bin.
RUN git clone https://github.com/heetch/atlas.git -b heetch-atlas-2.0

RUN echo 'package-lock=false' >> ./atlas/.npmrc
RUN echo 'package-lock.json' >> ./atlas/.gitignore

# Remove -DskipTests if unit tests are to be included
RUN mvn -T $MVN_JOB -DskipTests $PROFILE -f ./atlas/pom.xml clean install
RUN mkdir -p atlas-bin
RUN tar xzf /home/ubuntu/atlas/distro/target/*bin.tar.gz --strip-components 1 -C /home/ubuntu/atlas-bin

# Set env variables, add it to the path, and start Atlas.
ENV MANAGE_LOCAL_SOLR true
ENV MANAGE_LOCAL_HBASE true
ENV PATH /home/ubuntu/atlas-bin/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

EXPOSE 21000
ENV DRY=true
CMD ["/bin/bash", "-c", "/home/ubuntu/atlas-bin/bin/atlas_start.py; tail -fF /home/ubuntu/atlas-bin/logs/application.log"]
