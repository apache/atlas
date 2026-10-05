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

   Atlas server configuration is mounted from `config/atlas/${ATLAS_BACKEND}/atlas-application.properties`.
   The file authentication credentials are mounted from `config/atlas/users-credentials.properties`.

    1. Build atlas-base image with the following command:

       ```shell
       docker compose -f docker-compose.atlas-base.yml build
       ```

    2. Ensure that the `${HOME}/.m2` directory exists and execute following command to build Apache Atlas:

       ```shell
       mkdir -p ${HOME}/.m2
       docker compose -f docker-compose.atlas-build.yml up
       ```

   Time taken to complete the build might vary (upto an hour), depending on status of ${HOME}/.m2 directory cache.

    3. To install and start Atlas using Postgres as backend store, execute following commands:

       ```shell
       export ATLAS_BACKEND=postgres
       docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-postgres.yml up -d --wait
       ```

       The Postgres overlay runs `config/init_postgres.sh` as a one-shot initialization service before Atlas starts.
       This creates the required roles, databases, and Atlas RDBMS schema.

    4. To install and start Atlas using HBase as backend store, execute following commands:

       ```shell
       export ATLAS_BACKEND=hbase
       docker compose -f docker-compose.atlas.yml -f docker-compose.atlas-hadoop.yml up -d --wait
       ```

   Apache Atlas will be installed at /opt/atlas/, and logs are at /var/log/atlas directory.

7. Atlas Admin can be accessed at http://localhost:21000 (admin/atlasR0cks!)

## Atlas Modular Run Time Architecture (AMRA)

Use this section to run AMRA locally with Docker Compose.

AMRA splits Atlas into run-mode roles so multiple nodes can share one backend
store without ZooKeeper leader election:

| `RUN_MODE` | Role |
|---|---|
| `INITIALIZER` | One-shot: graph index setup, type-def bootstrap, patches → exit `0` |
| `METADATA_SERVER` | REST / search / entity CRUD / tasks / import-export / index recovery |
| `NOTIFICATION_PROCESSOR` | Hook Kafka consumer only (writes entities to the graph) |
| `MONOLITHIC` | Full legacy stack in one JVM per replica (backward-compatible) |

Default startup scripts use **modular** topology
(`INITIALIZER` + `METADATA_SERVER` + `NOTIFICATION_PROCESSOR` + LB).
Pass `RUN_MODE=MONOLITHIC` to start the monolithic multi-replica topology instead.

### Active-Active configuration model

Active-active uses layered properties files:

- Common properties: `config/atlas/active-active/common/atlas-application.properties`
- HBase backend overrides: `config/atlas/active-active/hbase/atlas-application.properties`
- Postgres backend overrides: `config/atlas/active-active/postgres/atlas-application.properties`

Additional active-active env knobs live in `.env.active-active` (used together with `.env`):

- `ATLAS_BACKEND` — `hbase` or `postgres`
- `METADATA_SERVER_REPLICAS` / `NOTIFICATION_PROC_REPLICAS`
- Patch/recovery toggles: `ATLAS_REBUILD_INDEX`, `ATLAS_UPDATE_COMPOSITE_INDEX_STATUS`, `ATLAS_INDEX_RECOVERY_ENABLE`

At container startup, `scripts/atlas-active-active.sh`:

1. Combines common + backend properties into `/opt/atlas/conf/atlas-application.properties`
2. Applies run-mode runtime properties (index recovery, HA server id, rebuild toggles)
3. Enables header-based auth + disables CSRF on metadata/monolithic nodes
   (stateless LB round-robin; no session affinity required)
4. Starts Atlas with `-DRUN_MODE=<mode>`

Claim stale-threshold defaults (used to resume stuck in-progress work after a crash):

```text
atlas.async.import.claim.stale.threshold.ms=3600000
atlas.tasks.claim.stale.threshold.ms=3600000
```

### Prerequisites (one-time or when code changes)

Run from `dev-support/atlas-docker`:

```shell
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

# Build base image if missing/outdated
docker compose -f docker-compose.atlas-base.yml build atlas-base

# Build Atlas distro if needed
mkdir -p ${HOME}/.m2
docker compose -f docker-compose.atlas-build.yml up
```

Important:

- Re-run `./download-archives.sh` whenever `.env` archive versions change
  (for example after pulling/merging updates from `master`).
- If versions change but `downloads/` still has old files, Docker image builds can
  fail with errors like:
  `COPY ./downloads/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz ... not found`.

### Startup: Modular Active-Active with HBase backend

Recommended (scripted):

```shell
# from dev-support/atlas-docker
./scripts/atlas-start-active-active-hbase.sh
```

Optional replica overrides:

```shell
METADATA_REPLICAS=3 NOTIFICATION_REPLICAS=2 ./scripts/atlas-start-active-active-hbase.sh
```

What the script does:

1. Sets `ATLAS_BACKEND=hbase` in `.env.active-active`
2. Starts infra (`atlas-hadoop`, `atlas-zk`, `atlas-kafka`, `atlas-solr`, `atlas-backend`, `atlas-db`)
3. Runs one-shot `atlas-initializer`
4. Starts metadata + notification replicas and LB

Manual equivalent:

```shell
docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml up -d \
  atlas-hadoop atlas-zk atlas-kafka atlas-solr atlas-backend atlas-db

docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml up -d --force-recreate atlas-initializer

docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml up -d --force-recreate \
  --scale atlas-metadata-server=2 --scale atlas-notification-proc=2 \
  atlas-metadata-server atlas-notification-proc atlas-lb
```

### Startup: Modular Active-Active with Postgres backend

Recommended (scripted):

```shell
# from dev-support/atlas-docker
./scripts/atlas-start-active-active-postgres.sh
```

Optional replica overrides:

```shell
METADATA_REPLICAS=3 NOTIFICATION_REPLICAS=2 ./scripts/atlas-start-active-active-postgres.sh
```

What the script does:

1. Sets `ATLAS_BACKEND=postgres` in `.env.active-active`
2. Starts infra using:
    - `docker-compose.atlas-active-active.yml`
    - `docker-compose.atlas-active-active-postgres.yml`
3. Runs `atlas-db-init` one-shot service (creates roles/db/schema)
4. Runs one-shot `atlas-initializer`
5. Starts metadata + notification replicas and LB

Manual equivalent:

```shell
docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml -f docker-compose.atlas-active-active-postgres.yml up -d \
  atlas-hadoop atlas-zk atlas-kafka atlas-solr atlas-backend atlas-db

docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml -f docker-compose.atlas-active-active-postgres.yml up -d \
  atlas-db-init

docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml -f docker-compose.atlas-active-active-postgres.yml up -d --force-recreate \
  atlas-initializer

docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml -f docker-compose.atlas-active-active-postgres.yml up -d --force-recreate \
  --scale atlas-metadata-server=2 --scale atlas-notification-proc=2 \
  atlas-metadata-server atlas-notification-proc atlas-lb
```

### Startup: Monolithic Active-Active replicas

Monolithic mode runs the full Atlas stack in each replica JVM (`RUN_MODE=MONOLITHIC`),
using `docker-compose.atlas-monolithic.yml` (+ postgres overlay when needed).
No separate initializer / notification-processor services.

```shell
# HBase backend
RUN_MODE=MONOLITHIC REPLICAS=2 ./scripts/atlas-start-active-active-hbase.sh

# Postgres backend
RUN_MODE=MONOLITHIC REPLICAS=2 ./scripts/atlas-start-active-active-postgres.sh
```

### Validate startup (both backends)

```shell
docker compose -f docker-compose.atlas-active-active.yml ps
curl -s http://localhost:21000/api/atlas/admin/status
docker inspect atlas-initializer --format '{{.State.Status}} exitCode={{.State.ExitCode}}'
```

For monolithic topology, use:

```shell
docker compose -f docker-compose.atlas-monolithic.yml ps
curl -s http://localhost:21000/api/atlas/admin/status
```

Expected:

- `/api/atlas/admin/status` returns `{"Status":"ACTIVE"}`
- Modular: `atlas-initializer` ends as `exited exitCode=0`
- Modular: metadata server containers become `healthy`
- Monolithic: `atlas-monolithic-server` replicas become healthy

### Switching backend cleanly

When switching from one backend to the other, stop active-active stack first:

```shell
docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml \
  -f docker-compose.atlas-active-active-postgres.yml down
```

Optional full cleanup (fresh state):

```shell
docker compose --env-file .env --env-file .env.active-active \
  -f docker-compose.atlas-active-active.yml \
  -f docker-compose.atlas-active-active-postgres.yml down -v
```

For monolithic stacks, also include the monolithic compose files when bringing down.
Then start with the desired backend script (`hbase` or `postgres`).

### Auth / CSRF notes for AMRA

Metadata and monolithic containers enable header-based authentication at startup:

```text
atlas.authn.header.enabled=true
atlas.authn.header.username=x-awc-username
atlas.authn.header.roles=x-awc-roles
atlas.authn.header.requestid=x-awc-requestid
atlas.rest-csrf.enabled=false
```

This keeps LB traffic stateless across replicas (no sticky sessions).

### Docker commands

```shell
docker exec -it <container> bash
docker logs -f <container>
docker inspect <container> --format '{{.State.Status}} exitCode={{.State.ExitCode}}'
docker cp <container>:<src_path> <local_dest_path> 2>/dev/null
```

Find IP addresses of notification processor containers:

```shell
docker inspect -f '{{.Name}} {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  atlas-docker-atlas-notification-proc-1 atlas-docker-atlas-notification-proc-2
```

Check Kafka groups/consumers:

```shell
docker exec -it atlas-kafka bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group atlas
```

### Containers

Initializer:

```text
atlas-initializer
```

Metadata servers:

```text
atlas-docker-atlas-metadata-server-1
atlas-docker-atlas-metadata-server-2
```

Notification processors:

```text
atlas-docker-atlas-notification-proc-1
atlas-docker-atlas-notification-proc-2
```

Monolithic replicas:

```text
atlas-docker-atlas-monolithic-server-1
atlas-docker-atlas-monolithic-server-2
```

Kafka:

```shell
docker exec -it atlas-kafka bash
```

Hive:

```shell
docker exec -it atlas-hive bash
```

Logs path inside Atlas container:

```text
/opt/atlas/logs/
```

Atlas LB URL:

```text
http://localhost:21000
```

### Troubleshooting

#### Quick health checklist

Run these first for a quick environment sanity check:

```shell
docker compose -f docker-compose.atlas-active-active.yml ps
curl -s http://localhost:21000/api/atlas/admin/status
docker inspect atlas-initializer --format '{{.State.Status}} exitCode={{.State.ExitCode}}'
docker exec -it atlas-solr bash -lc "curl -s 'http://localhost:8983/solr/admin/cores?action=STATUS&wt=json'"
docker exec -it atlas-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group atlas
```

#### Initializer timeout

If `atlas-initializer` appears to hang or exits before completion:

```shell
docker logs -f atlas-initializer
docker inspect atlas-initializer --format '{{.State.Status}} exitCode={{.State.ExitCode}}'
```

Expected final state:

```text
exited exitCode=0
```

If it repeatedly fails, restart only the initializer:

```shell
docker compose -f docker-compose.atlas-active-active.yml up -d --force-recreate atlas-initializer
```

#### Missing archive during docker build

Symptom (example):

```text
Dockerfile.atlas-kafka: COPY ./downloads/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz ... not found
```

Cause:

- `downloads/` has stale archives that do not match versions currently set in `.env`.

Fix:

```shell
# from dev-support/atlas-docker
./download-archives.sh
```

Then retry the startup script.

#### CSRF popup in UI

If UI requests fail with:

```text
Missing header or invalid Header value for CSRF Vulnerability Protection
```

verify CSRF setting in Atlas config:

```shell
docker exec -it atlas-docker-atlas-metadata-server-1 bash -lc "grep '^atlas.rest-csrf.enabled=' /opt/atlas/conf/atlas-application.properties"
```

AMRA startup disables CSRF on metadata/monolithic nodes:

```text
atlas.rest-csrf.enabled=false
```

After config changes, recreate metadata servers and LB:

```shell
docker compose -f docker-compose.atlas-active-active.yml up -d --force-recreate atlas-metadata-server atlas-lb
```

In active-active mode, ensure FQDN aliases resolve for backend services (`atlas-hbase.example.com`, `atlas-kafka.example.com`, `atlas-solr.example.com`, `atlas-zk.example.com`) in the compose network.
