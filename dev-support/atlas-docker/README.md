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

## Configuration

Atlas server configuration is mounted from:

```text
config/atlas/${ATLAS_BACKEND}/${ATLAS_INDEX_BACKEND}/atlas-application.properties
```

Authentication credentials are mounted from `config/atlas/users-credentials.properties`.

| Variable | Values | Controls |
|----------|--------|----------|
| `ATLAS_BACKEND` | `hbase`, `postgres` | Graph storage backend |
| `ATLAS_INDEX_BACKEND` | `solr`, `opensearch` | Search index backend |

Defaults in `.env`: `ATLAS_BACKEND=hbase`, `ATLAS_INDEX_BACKEND=solr`.

**Important:** Switching `ATLAS_INDEX_BACKEND` requires a fresh Atlas data directory (`rm -rf ./data`) because JanusGraph validates the index backend on first init.

For OpenSearch development, set `BUILD_HOST_SRC=true` when building so the container uses your local source tree (includes OpenSearch backend changes).

See also [OpenSearch session context](../opensearch-docker/OPENSEARCH_SESSION_CONTEXT.md) for C6 REST validation after the stack is up.

## Usage

1. Ensure that you have recent version of Docker installed from [docker.io](http://www.docker.io) (as of this writing: Engine 20.10.5, Compose 1.28.5).
   Make sure to configure docker with at least 6gb of memory.

2. Set this folder as your working directory.

3. Update environment variables in `.env`, if necessary

4. Execute following command to download necessary archives to setup Atlas/HDFS/HBase/Kafka services:

   ```shell
   chmod +x download-archives.sh
   ./download-archives.sh
   ```

5. Execute following commands to set environment variables to build Apache Atlas docker containers:

   ```shell
   export DOCKER_BUILDKIT=1
   export COMPOSE_DOCKER_CLI_BUILD=1
   ```

6. Build and deploy Apache Atlas in containers using docker compose

   1. Build atlas-base image with the following command:

      ```shell
      docker compose -f docker-compose.atlas-base.yml build
      ```

   2. Ensure that the `${HOME}/.m2` directory exists and execute following command to build Apache Atlas:

      ```shell
      mkdir -p ${HOME}/.m2
      docker compose -f docker-compose.atlas-build.yml up
      ```

   Time taken to complete the build might vary (upto an hour), depending on status of `${HOME}/.m2` directory cache.

   3. To install and start Atlas using Postgres as backend store (Solr index):

      ```shell
      export ATLAS_BACKEND=postgres
      export ATLAS_INDEX_BACKEND=solr
      docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-postgres.yml up -d --wait
      ```

      The Postgres overlay runs `config/init_postgres.sh` as a one-shot initialization service before Atlas starts.
      This creates the required roles, databases, and Atlas RDBMS schema.

   4. To install and start Atlas using HBase as backend store (Solr index):

      ```shell
      export ATLAS_BACKEND=hbase
      export ATLAS_INDEX_BACKEND=solr
      docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-hadoop.yml up -d --wait
      ```

   5. To install and start Atlas using **HBase + OpenSearch** (recommended for OpenSearch backend validation):

      ```shell
      export ATLAS_BACKEND=hbase
      export ATLAS_INDEX_BACKEND=opensearch
      export COMPOSE_PROFILES=opensearch-index
      export BUILD_HOST_SRC=true
      rm -rf ./data
      docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-hadoop.yml build
      docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-hadoop.yml up -d --wait
      ```

   6. To install and start Atlas using **Postgres + OpenSearch**:

      ```shell
      export ATLAS_BACKEND=postgres
      export ATLAS_INDEX_BACKEND=opensearch
      export COMPOSE_PROFILES=opensearch-index
      export BUILD_HOST_SRC=true
      rm -rf ./data
      docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-postgres.yml build
      docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-postgres.yml up -d --wait
      ```

   **Verify OpenSearch + Atlas:**

   ```shell
   curl -s http://localhost:9200/
   curl -s http://localhost:15601/api/status
   curl -u admin:atlasR0cks! http://localhost:21000/api/atlas/admin/version
   curl -u admin:atlasR0cks! "http://localhost:21000/api/atlas/v2/search/quick?query=*&limit=1"
   ```

   OpenSearch Dashboards UI: http://localhost:15601 (no login when security plugin is disabled)

   If Dashboards fails to load in Chrome with `[I18n] A locale must be a non-empty string`, clear site
   data for the Dashboards URL (Chrome → Settings → Privacy → Site settings → View all → localhost),
   use a private window, or recreate the container after pulling the updated compose port mapping.

   If Docker Compose hangs on `Pulling` for locally built images, use:

   ```shell
   export COMPOSE_PULL_POLICY=never
   ```

   Apache Atlas will be installed at `/opt/atlas/`, and logs are at `/var/log/atlas` directory.

7. Atlas Admin can be accessed at http://localhost:21000 (admin/atlasR0cks!)
