-- Licensed to the Apache Software Foundation(ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
--(the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- DB objects for Atlas entity audit
CREATE SEQUENCE IF NOT EXISTS atlas_entity_audit_seq INCREMENT BY 1 CACHE 1000;

CREATE TABLE IF NOT EXISTS atlas_entity_audit(id BIGINT, entity_id VARCHAR(64) NOT NULL, event_time BIGINT NOT NULL, event_idx INT NOT NULL, user_name  VARCHAR(64) NOT NULL, operation VARCHAR(64) NOT NULL, details TEXT DEFAULT NULL, entity TEXT DEFAULT NULL, audit_type INT NOT NULL, PRIMARY KEY(id));

CREATE INDEX IF NOT EXISTS atlas_entity_audit_idx_entity_id            ON atlas_entity_audit (entity_id);
CREATE INDEX IF NOT EXISTS atlas_entity_audit_idx_event_time           ON atlas_entity_audit (event_time);
CREATE INDEX IF NOT EXISTS atlas_entity_audit_idx_user_name            ON atlas_entity_audit (user_name);
CREATE INDEX IF NOT EXISTS atlas_entity_audit_idx_entity_id_event_time ON atlas_entity_audit (entity_id, event_time);


-- DB objects for JanusGraph backend store
CREATE SEQUENCE IF NOT EXISTS janus_store_seq INCREMENT BY 1 CACHE 1;
CREATE SEQUENCE IF NOT EXISTS janus_key_seq INCREMENT BY 1 CACHE 1000;
CREATE SEQUENCE IF NOT EXISTS janus_column_seq INCREMENT BY 1 CACHE 1000;

CREATE TABLE IF NOT EXISTS janus_store(id BIGINT, name VARCHAR(255) NOT NULL, PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS janus_key(id BIGINT, store_id BIGINT NOT NULL, name BYTEA NOT NULL, PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS janus_column(id BIGINT DEFAULT NEXTVAL('janus_column_seq'::regclass), key_id BIGINT NOT NULL, name BYTEA NOT NULL, val BYTEA NOT NULL, PRIMARY KEY(id));

CREATE UNIQUE INDEX IF NOT EXISTS janus_store_uk_name      ON janus_store(name);
CREATE UNIQUE INDEX IF NOT EXISTS janus_key_uk_store_name  ON janus_key(store_id, name);
CREATE UNIQUE INDEX IF NOT EXISTS janus_column_uk_key_name ON janus_column(key_id, name);

CREATE INDEX IF NOT EXISTS janus_key_idx_store_id  ON janus_key (store_id);
CREATE INDEX IF NOT EXISTS janus_column_idx_key_id ON janus_column (key_id);


-- DB objects for JanusGraph unique key constraints
CREATE SEQUENCE IF NOT EXISTS janus_unique_vertex_key_seq INCREMENT BY 1 CACHE 1000;
CREATE SEQUENCE IF NOT EXISTS janus_unique_vertex_type_key_seq INCREMENT BY 1 CACHE 1000;
CREATE SEQUENCE IF NOT EXISTS janus_unique_edge_key_seq INCREMENT BY 1 CACHE 1000;
CREATE SEQUENCE IF NOT EXISTS janus_unique_edge_type_key_seq INCREMENT BY 1 CACHE 1000;

CREATE TABLE IF NOT EXISTS janus_unique_vertex_key(id BIGINT DEFAULT NEXTVAL('janus_unique_vertex_key_seq'::regclass), vertex_id BIGINT NOT NULL, key_name VARCHAR(255) NOT NULL, val TEXT NOT NULL, PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS janus_unique_vertex_type_key(id BIGINT DEFAULT NEXTVAL('janus_unique_vertex_type_key_seq'::regclass), vertex_id BIGINT NOT NULL, type_name VARCHAR(255) NOT NULL, key_name VARCHAR(255) NOT NULL, val TEXT NOT NULL, PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS janus_unique_edge_key(id BIGINT DEFAULT NEXTVAL('janus_unique_edge_key_seq'::regclass), edge_id BIGINT NOT NULL, key_name VARCHAR(255) NOT NULL, val TEXT NOT NULL, PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS janus_unique_edge_type_key(id BIGINT DEFAULT NEXTVAL('janus_unique_edge_type_key_seq'::regclass), edge_id BIGINT NOT NULL, type_name VARCHAR(255) NOT NULL, key_name VARCHAR(255) NOT NULL, val TEXT NOT NULL, PRIMARY KEY(id));

CREATE UNIQUE INDEX IF NOT EXISTS janus_unique_vertex_key_uk      ON janus_unique_vertex_key(key_name, val);
CREATE UNIQUE INDEX IF NOT EXISTS janus_unique_vertex_type_key_uk ON janus_unique_vertex_type_key(type_name, key_name, val);
CREATE UNIQUE INDEX IF NOT EXISTS janus_unique_edge_key_uk        ON janus_unique_edge_key(key_name, val);
CREATE UNIQUE INDEX IF NOT EXISTS janus_unique_edge_type_key_uk   ON janus_unique_edge_type_key(type_name, key_name, val);

CREATE INDEX IF NOT EXISTS janus_unique_vertex_key_idx_vertex_id      ON janus_unique_vertex_key (vertex_id);
CREATE INDEX IF NOT EXISTS janus_unique_vertex_type_key_idx_vertex_id ON janus_unique_vertex_type_key (vertex_id);
CREATE INDEX IF NOT EXISTS janus_unique_edge_key_idx_edge_id          ON janus_unique_edge_key (edge_id);
CREATE INDEX IF NOT EXISTS janus_unique_edge_type_key_idx_edge_id     ON janus_unique_edge_type_key (edge_id);
