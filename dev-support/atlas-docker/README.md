<!---
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

## Overview

Docker files in this folder create docker images and run them to build Apache Atlas, deploy Apache Atlas and dependent services in containers.

## Usage

1. Ensure that you have recent version of Docker installed from [docker.io](http://www.docker.io) (as of this writing: Engine 20.10.5, Compose 1.28.5).
   Make sure to configure docker with at least 6gb of memory.

2. Set this folder as your working directory.

3. Update environment variables in .env file, if necessary

4. Execute following command to download necessary archives to setup Atlas/HDFS/HBase/Kafka services:
   ~~~
   chmod +x download-archives.sh
   ./download-archives.sh
   ~~~

5.  Execute following commands to set environment variables to build Apache Atlas docker containers:
   ~~~
   export DOCKER_BUILDKIT=1
   export COMPOSE_DOCKER_CLI_BUILD=1
   ~~~

6. Build and deploy Apache Atlas in containers using docker compose

   6.1. Build atlas-base image with the following commenr
        docker compose -f docker-compose.atlas-base.yml build

   6.2. Ensure that the `${HOME}/.m2` directory exists and Execute following command to build Apache Atlas:
        mkdir -p ${HOME}/.m2
        docker compose docker-compose.atlas-build.yml up

   Time taken to complete the build might vary (upto an hour), depending on status of ${HOME}/.m2 directory cache.

   6.3. To install and start Atlas using Postgres as backend store, execute following commands:
	export ATLAS_BACKEND=postgres
        docker compose -f docker-compose.atlas.yml up -d --wait

   6.4. To install and start Atlas using HBase as backend store, execute following commands:
	export ATLAS_BACKEND=hbase
	docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-hadoop.yml up -d --wait

   Apache Atlas will be installed at /opt/atlas/, and logs are at /var/logs/atlas directory.

7. Atlas Admin can be accessed at http://localhost:21000 (admin/atlasR0cks!)
