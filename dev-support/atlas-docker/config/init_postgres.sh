#!/bin/bash

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

set -euo pipefail

: "${POSTGRES_HOST:=atlas-db}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_USER:=postgres}"
: "${POSTGRES_DB:=postgres}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set}"
: "${HIVE_DB_PASSWORD:?HIVE_DB_PASSWORD must be set}"
: "${ATLAS_DB_PASSWORD:?ATLAS_DB_PASSWORD must be set}"
: "${ATLAS_SCHEMA_FILE:?ATLAS_SCHEMA_FILE must be set}"

export PGPASSWORD="${POSTGRES_PASSWORD}"

psql_cmd=(
  psql
  -v ON_ERROR_STOP=1
  --host "${POSTGRES_HOST}"
  --port "${POSTGRES_PORT}"
  --username "${POSTGRES_USER}"
  --dbname "${POSTGRES_DB}"
)

atlas_psql_cmd=(
  psql
  -v ON_ERROR_STOP=1
  --host "${POSTGRES_HOST}"
  --port "${POSTGRES_PORT}"
  --username atlas
  --dbname atlas
)

create_role() {
  local role_name=$1
  local role_password=$2
  local role_exists
  local role_sql

  role_exists=$("${psql_cmd[@]}" --set=role_name="${role_name}" -At <<'EOSQL'
SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'role_name';
EOSQL
)

  if [ "${role_exists}" != "1" ]
  then
    role_sql=$("${psql_cmd[@]}" --set=role_name="${role_name}" --set=role_password="${role_password}" -At <<'EOSQL'
SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'role_name', :'role_password');
EOSQL
)
  else
    role_sql=$("${psql_cmd[@]}" --set=role_name="${role_name}" --set=role_password="${role_password}" -At <<'EOSQL'
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'role_name', :'role_password');
EOSQL
)
  fi

  "${psql_cmd[@]}" -c "${role_sql}"
}

create_database() {
  local database_name=$1
  local owner_name=$2
  local database_exists
  local database_sql
  local grant_sql

  database_exists=$("${psql_cmd[@]}" --set=database_name="${database_name}" -At <<'EOSQL'
SELECT 1 FROM pg_database WHERE datname = :'database_name';
EOSQL
)

  if [ "${database_exists}" != "1" ]
  then
    database_sql=$("${psql_cmd[@]}" --set=database_name="${database_name}" --set=owner_name="${owner_name}" -At <<'EOSQL'
SELECT format('CREATE DATABASE %I OWNER %I', :'database_name', :'owner_name');
EOSQL
)
    "${psql_cmd[@]}" -c "${database_sql}"
  fi

  grant_sql=$("${psql_cmd[@]}" --set=database_name="${database_name}" --set=owner_name="${owner_name}" -At <<'EOSQL'
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', :'database_name', :'owner_name');
EOSQL
)
  "${psql_cmd[@]}" -c "${grant_sql}"
  "${psql_cmd[@]}" --dbname "${database_name}" -c "GRANT ALL ON SCHEMA public TO public;"
}

create_role hive "${HIVE_DB_PASSWORD}"
create_database hive hive

create_role atlas "${ATLAS_DB_PASSWORD}"
create_database atlas atlas

PGPASSWORD="${ATLAS_DB_PASSWORD}" "${atlas_psql_cmd[@]}" --file "${ATLAS_SCHEMA_FILE}"
