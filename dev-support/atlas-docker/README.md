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

1. Ensure that you have recent version of Docker installed from [docker.io](http://www.docker.io) (as of this writing: Engine 19.03, Compose 1.26.2).

2. Set this folder as your working directory.

3. Update environment variables in .env file, if necessary

4. Using docker-compose is the simpler way to build and deploy Apache Atlas in containers.

   4.1. Execute following command to build Apache Atlas:

        docker-compose -f docker-compose.atlas-base.yml -f docker-compose.atlas-build.yml up

   Time taken to complete the build might vary (upto an hour), depending on status of ${HOME}/.m2 directory cache.

   4.2. Execute following command to install and start Atlas in a container:

        docker-compose -f docker-compose.atlas-base.yml -f docker-compose.atlas.yml up -d

   Apache Atlas will be installed at /opt/atlas/, and logs are at /var/logs/atlas directory.

5. Alternatively docker command can be used to build and deploy Apache Atlas.

   5.1. Execute following command to build Docker image **atlas-base**:

        docker build -f Dockerfile.atlas-base -t atlas-base .

   This might take about 10 minutes to complete.

   5.2. Execute following command to build Docker image **atlas-build**:

        docker build -f Dockerfile.atlas-build -t atlas-build .

   5.3. Build Apache Atlas in a container with one of the following commands:

        docker run -it --rm -v ${HOME}/.m2:/home/atlas/.m2:delegated -v $(pwd)/scripts:/home/atlas/scripts -v $(pwd)/../..:/home/atlas/src:delegated -v $(pwd)/patches:/home/atlas/patches -v $(pwd)/dist:/home/atlas/dist --env-file ./.env atlas-build

   Time taken to complete the build might vary (upto an hour), depending on status of ${HOME}/.m2 directory cache.

   5.4. Execute following command to build Docker image **atlas**:

        docker build -f Dockerfile.atlas --build-arg ATLAS_VERSION=3.0.0-SNAPSHOT -t atlas .

   This might take about 10 minutes to complete.

   5.5. Execute following command to install and run Atlas services in a container:

        docker run -it -d --name atlas --hostname atlas.example.com -p 21000:21000 -v $(pwd)/data:/home/atlas/data atlas

   This might take few minutes to complete.

6. Atlas Admin can be accessed at http://localhost:21000 (admin/atlasR0cks!)
