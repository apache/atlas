-- Licensed to the Apache Software Foundation(ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
--(the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, softwaren
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE SEQUENCE IF NOT EXISTS janus_store_seq  CACHE 1;
CREATE SEQUENCE IF NOT EXISTS janus_key_seq    CACHE 1000;
CREATE SEQUENCE IF NOT EXISTS janus_column_seq CACHE 1000;

CREATE TABLE IF NOT EXISTS janus_store(
    id   BIGINT       DEFAULT nextval('janus_store_seq'::regclass),
    name VARCHAR(512) NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT janus_store_uk_name UNIQUE(name));

CREATE TABLE IF NOT EXISTS janus_key(
    id       BIGINT DEFAULT nextval('janus_key_seq'::regclass),
    store_id BIGINT NOT NULL,
    name     BYTEA  NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT janus_key_uk_store_name UNIQUE(store_id, name),
    CONSTRAINT janus_key_fk_store      FOREIGN KEY(store_id) REFERENCES janus_store(id));

CREATE TABLE IF NOT EXISTS janus_column(
    id BIGINT DEFAULT nextval('janus_column_seq'::regclass),
    key_id    BIGINT NOT NULL,
    name      BYTEA  NOT NULL,
    val       BYTEA  NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT janus_column_uk_key_name UNIQUE(key_id, name),
    CONSTRAINT janus_column_fk_key FOREIGN KEY(key_id) REFERENCES janus_key(id));

CREATE INDEX IF NOT EXISTS janus_key_idx_store_id  ON janus_key (store_id);
CREATE INDEX IF NOT EXISTS janus_column_idx_key_id ON janus_column (key_id);
