/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.audit;

import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.repository.Constants;
import org.apache.commons.configuration2.MapConfiguration;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class AbstractStorageBasedAuditRepositoryTest {
    @Test
    public void listEvents_fallsBackToV1WhenV2IsEmpty() throws Exception {
        TestRepository repository = new TestRepository();
        EntityAuditEvent v1Event = new EntityAuditEvent();
        v1Event.setEntityId("entity-1");

        repository.v2Events = Collections.emptyList();
        repository.v1Events = Collections.singletonList(v1Event);

        List<Object> result = repository.listEvents("entity-1", null, (short) 10);

        assertEquals(result.size(), 1);
        assertEquals(((EntityAuditEvent) result.get(0)).getEntityId(), "entity-1");
    }

    @Test
    public void repositoryMaxSize_readsConfiguredLimit() {
        TestRepository repository = new TestRepository();
        MapConfiguration config = new MapConfiguration(Collections.singletonMap("atlas.hbase.client.keyvalue.maxsize", 2048L));

        repository.setApplicationProperties(config);

        assertEquals(repository.repositoryMaxSize(), 2048L);
    }

    @Test
    public void getAuditExcludeAttributes_readsAndCachesConfiguredAttributes() {
        TestRepository repository = new TestRepository();
        Map<String, Object> entries = new HashMap<>();
        entries.put("atlas.audit.hbase.entity.hive_table.attributes.exclude", new String[] {"owner", "location"});
        MapConfiguration config = new MapConfiguration(entries);

        repository.setApplicationProperties(config);

        List<String> excludes = repository.getAuditExcludeAttributes("hive_table");
        assertEquals(excludes, Arrays.asList("owner", "location"));

        // cache should preserve the original value
        config.setProperty("atlas.audit.hbase.entity.hive_table.attributes.exclude", new String[] {"changed"});
        assertEquals(repository.getAuditExcludeAttributes("hive_table"), Arrays.asList("owner", "location"));
    }

    @Test
    public void keyParsing_extractsTimestampAndIndexOrDefaults() {
        TestRepository repository = new TestRepository();

        assertEquals(repository.extractTimestamp("entity:12345:7:999"), 12345L);
        assertEquals(repository.extractIndex("entity:12345:7:999"), 7);
        assertEquals(repository.extractTimestamp("invalid"), 0L);
        assertEquals(repository.extractIndex("invalid"), 0);
        assertEquals(repository.extractTimestamp(null), 0L);
        assertEquals(repository.extractIndex(null), 0);
    }

    private static final class TestRepository extends AbstractStorageBasedAuditRepository {
        private List<EntityAuditEvent>   v1Events = Collections.emptyList();
        private List<EntityAuditEventV2> v2Events = Collections.emptyList();

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void putEventsV1(List<EntityAuditEvent> events) {
        }

        @Override
        public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) {
            return v1Events;
        }

        @Override
        public void putEventsV2(List<EntityAuditEventV2> events) {
        }

        @Override
        public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String startKey, short maxResultCount) {
            return v2Events;
        }

        @Override
        public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) {
            return v2Events;
        }

        @Override
        public List<EntityAuditEventV2> deleteEventsV2(String entityId, Set<EntityAuditEventV2.EntityAuditActionV2> entityAuditActions, short auditCount, int ttlInDays, boolean createEventsAgeoutAllowed, Constants.AtlasAuditAgingType auditAgingType) {
            return Collections.emptyList();
        }

        @Override
        public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
            return Collections.emptySet();
        }

        long extractTimestamp(String key) {
            return getTimestampFromKey(key);
        }

        int extractIndex(String key) {
            return getIndexFromKey(key);
        }
    }
}
