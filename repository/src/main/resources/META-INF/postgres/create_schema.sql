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

CREATE SEQUENCE IF NOT EXISTS atlas_entity_audit_seq CACHE 1000;

CREATE TABLE IF NOT EXISTS atlas_entity_audit(id BIGINT DEFAULT nextval('atlas_entity_audit_seq'::regclass), entity_id VARCHAR(64) NOT NULL, event_time BIGINT NOT NULL, event_idx INT NOT NULL, user_name VARCHAR(64) NOT NULL, operation INT NOT NULL, details TEXT DEFAULT NULL, entity TEXT DEFAULT NULL, audit_type INT NOT NULL, PRIMARY KEY(id));
CREATE INDEX IF NOT EXISTS atlas_entity_audit_idx_entity_id            ON atlas_entity_audit (entity_id);
CREATE INDEX IF NOT EXISTS atlas_entity_audit_idx_event_time           ON atlas_entity_audit (event_time);
CREATE INDEX IF NOT EXISTS atlas_entity_audit_idx_user_name            ON atlas_entity_audit (user_name);
CREATE INDEX IF NOT EXISTS atlas_entity_audit_idx_entity_id_event_time ON atlas_entity_audit (entity_id, event_time);
