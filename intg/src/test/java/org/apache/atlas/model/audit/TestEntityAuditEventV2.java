/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.audit;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEntityAuditEventV2 {
    private EntityAuditEventV2 auditEvent;
    private static final String TEST_ENTITY_ID = "test-entity-123";
    private static final long TEST_TIMESTAMP = System.currentTimeMillis();
    private static final String TEST_USER = "testUser";
    private static final String TEST_DETAILS = "Test audit details";

    @BeforeMethod
    public void setUp() {
        auditEvent = new EntityAuditEventV2();
    }

    @Test
    public void testDefaultConstructor() {
        EntityAuditEventV2 event = new EntityAuditEventV2();

        assertNull(event.getEntityId());
        assertEquals(event.getTimestamp(), 0L);
        assertNull(event.getUser());
        assertNull(event.getAction());
        assertNull(event.getDetails());
        assertNull(event.getEventKey());
        assertNull(event.getEntity());
        assertNull(event.getType());
    }

    @Test
    public void testParameterizedConstructorWithoutType() {
        AtlasEntity entity = new AtlasEntity();
        EntityAuditEventV2 event = new EntityAuditEventV2(TEST_ENTITY_ID, TEST_TIMESTAMP, TEST_USER,
                EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, TEST_DETAILS, entity);

        assertEquals(event.getEntityId(), TEST_ENTITY_ID);
        assertEquals(event.getTimestamp(), TEST_TIMESTAMP);
        assertEquals(event.getUser(), TEST_USER);
        assertEquals(event.getAction(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);
        assertEquals(event.getDetails(), TEST_DETAILS);
        assertEquals(event.getEntity(), entity);
        assertEquals(event.getType(), EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V2);
    }

    @Test
    public void testParameterizedConstructorWithType() {
        AtlasEntity entity = new AtlasEntity();
        EntityAuditEventV2 event = new EntityAuditEventV2(TEST_ENTITY_ID, TEST_TIMESTAMP, TEST_USER,
                EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, TEST_DETAILS, entity,
                EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V1);

        assertEquals(event.getEntityId(), TEST_ENTITY_ID);
        assertEquals(event.getTimestamp(), TEST_TIMESTAMP);
        assertEquals(event.getUser(), TEST_USER);
        assertEquals(event.getAction(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE);
        assertEquals(event.getDetails(), TEST_DETAILS);
        assertEquals(event.getEntity(), entity);
        assertEquals(event.getType(), EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V1);
    }

    @Test
    public void testEntityIdGetterSetter() {
        assertNull(auditEvent.getEntityId());

        auditEvent.setEntityId(TEST_ENTITY_ID);
        assertEquals(auditEvent.getEntityId(), TEST_ENTITY_ID);

        auditEvent.setEntityId(null);
        assertNull(auditEvent.getEntityId());
    }

    @Test
    public void testTimestampGetterSetter() {
        assertEquals(auditEvent.getTimestamp(), 0L);

        auditEvent.setTimestamp(TEST_TIMESTAMP);
        assertEquals(auditEvent.getTimestamp(), TEST_TIMESTAMP);

        auditEvent.setTimestamp(-1L);
        assertEquals(auditEvent.getTimestamp(), -1L);

        auditEvent.setTimestamp(Long.MAX_VALUE);
        assertEquals(auditEvent.getTimestamp(), Long.MAX_VALUE);
    }

    @Test
    public void testUserGetterSetter() {
        assertNull(auditEvent.getUser());

        auditEvent.setUser(TEST_USER);
        assertEquals(auditEvent.getUser(), TEST_USER);

        auditEvent.setUser("");
        assertEquals(auditEvent.getUser(), "");

        auditEvent.setUser(null);
        assertNull(auditEvent.getUser());
    }

    @Test
    public void testActionGetterSetter() {
        assertNull(auditEvent.getAction());

        auditEvent.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);
        assertEquals(auditEvent.getAction(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);

        auditEvent.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE);
        assertEquals(auditEvent.getAction(), EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE);

        auditEvent.setAction(null);
        assertNull(auditEvent.getAction());
    }

    @Test
    public void testDetailsGetterSetter() {
        assertNull(auditEvent.getDetails());

        auditEvent.setDetails(TEST_DETAILS);
        assertEquals(auditEvent.getDetails(), TEST_DETAILS);

        auditEvent.setDetails("");
        assertEquals(auditEvent.getDetails(), "");

        auditEvent.setDetails(null);
        assertNull(auditEvent.getDetails());
    }

    @Test
    public void testEventKeyGetterSetter() {
        assertNull(auditEvent.getEventKey());

        String eventKey = "event-key-123";
        auditEvent.setEventKey(eventKey);
        assertEquals(auditEvent.getEventKey(), eventKey);

        auditEvent.setEventKey(null);
        assertNull(auditEvent.getEventKey());
    }

    @Test
    public void testEntityGetterSetter() {
        assertNull(auditEvent.getEntity());

        AtlasEntity entity = new AtlasEntity();
        auditEvent.setEntity(entity);
        assertEquals(auditEvent.getEntity(), entity);

        auditEvent.setEntity(null);
        assertNull(auditEvent.getEntity());
    }

    @Test
    public void testTypeGetterSetter() {
        assertNull(auditEvent.getType());

        auditEvent.setType(EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V2);
        assertEquals(auditEvent.getType(), EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V2);

        auditEvent.setType(EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V1);
        assertEquals(auditEvent.getType(), EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V1);

        auditEvent.setType(null);
        assertNull(auditEvent.getType());
    }

    @Test
    public void testGetEntityDefinitionString() {
        assertNull(auditEvent.getEntityDefinitionString());

        AtlasEntity entity = new AtlasEntity();
        entity.setTypeName("TestType");
        auditEvent.setEntity(entity);

        String entityDefinition = auditEvent.getEntityDefinitionString();
        assertNotNull(entityDefinition);
        assertTrue(entityDefinition.contains("TestType"));
    }

    @Test
    public void testSetEntityDefinition() {
        String entityJson = "{\"typeName\":\"TestType\",\"guid\":\"test-guid\"}";
        auditEvent.setEntityDefinition(entityJson);

        AtlasEntity entity = auditEvent.getEntity();
        assertNotNull(entity);
        assertEquals(entity.getTypeName(), "TestType");
    }

    @Test
    public void testGetEntityHeader() {
        assertNull(auditEvent.getEntityHeader());

        // Test with details containing JSON
        String detailsWithJson = "Some details {\"typeName\":\"TestType\",\"guid\":\"test-guid\"}";
        auditEvent.setDetails(detailsWithJson);

        AtlasEntityHeader header = auditEvent.getEntityHeader();
        assertNotNull(header);
        assertEquals(header.getTypeName(), "TestType");
    }

    @Test
    public void testGetEntityHeaderWithoutJson() {
        auditEvent.setDetails("Simple details without JSON");
        assertNull(auditEvent.getEntityHeader());
    }

    @Test
    public void testGetEntityHeaderWithEmptyDetails() {
        auditEvent.setDetails("");
        assertNull(auditEvent.getEntityHeader());
    }

    @Test
    public void testClear() {
        // Set all fields
        AtlasEntity entity = new AtlasEntity();
        auditEvent.setEntityId(TEST_ENTITY_ID);
        auditEvent.setTimestamp(TEST_TIMESTAMP);
        auditEvent.setUser(TEST_USER);
        auditEvent.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);
        auditEvent.setDetails(TEST_DETAILS);
        auditEvent.setEventKey("event-key");
        auditEvent.setEntity(entity);
        auditEvent.setType(EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V2);

        // Clear all fields
        auditEvent.clear();

        // Verify all fields are cleared
        assertNull(auditEvent.getEntityId());
        assertEquals(auditEvent.getTimestamp(), 0L);
        assertNull(auditEvent.getUser());
        assertNull(auditEvent.getAction());
        assertNull(auditEvent.getDetails());
        assertNull(auditEvent.getEventKey());
        assertNull(auditEvent.getEntity());
        assertNull(auditEvent.getType());
    }

    @Test
    public void testHashCodeConsistency() {
        int hashCode1 = auditEvent.hashCode();
        int hashCode2 = auditEvent.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(auditEvent.equals(auditEvent));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(auditEvent.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(auditEvent.equals("not an audit event"));
    }

    @Test
    public void testEqualsWithDifferentValues() {
        EntityAuditEventV2 event1 = createTestEvent();
        EntityAuditEventV2 event2 = createTestEvent();
        event2.setUser("differentUser");

        assertFalse(event1.equals(event2));
        assertFalse(event2.equals(event1));
    }

    @Test
    public void testToString() {
        EntityAuditEventV2 event = createTestEvent();
        String result = event.toString();

        assertNotNull(result);
        assertTrue(result.contains("EntityAuditEventV2"));
        assertTrue(result.contains(TEST_ENTITY_ID));
        assertTrue(result.contains(TEST_USER));
        assertTrue(result.contains("ENTITY_CREATE"));
        assertTrue(result.contains(TEST_DETAILS));
    }

    @Test
    public void testSortEventsByTimestamp() {
        List<EntityAuditEventV2> events = createTestEventList();

        EntityAuditEventV2.sortEvents(events, EntityAuditEventV2.SORT_COLUMN_TIMESTAMP, false);

        // Verify ascending order by timestamp
        assertTrue(events.get(0).getTimestamp() <= events.get(1).getTimestamp());
        assertTrue(events.get(1).getTimestamp() <= events.get(2).getTimestamp());
    }

    @Test
    public void testSortEventsByTimestampDescending() {
        List<EntityAuditEventV2> events = createTestEventList();

        EntityAuditEventV2.sortEvents(events, EntityAuditEventV2.SORT_COLUMN_TIMESTAMP, true);

        // Verify descending order by timestamp
        assertTrue(events.get(0).getTimestamp() >= events.get(1).getTimestamp());
        assertTrue(events.get(1).getTimestamp() >= events.get(2).getTimestamp());
    }

    @Test
    public void testSortEventsByUser() {
        List<EntityAuditEventV2> events = createTestEventList();

        EntityAuditEventV2.sortEvents(events, EntityAuditEventV2.SORT_COLUMN_USER, false);

        // Verify ascending order by user (case insensitive)
        String user1 = events.get(0).getUser();
        String user2 = events.get(1).getUser();
        assertTrue(user1.compareToIgnoreCase(user2) <= 0);
    }

    @Test
    public void testSortEventsByAction() {
        List<EntityAuditEventV2> events = createTestEventList();

        EntityAuditEventV2.sortEvents(events, EntityAuditEventV2.SORT_COLUMN_ACTION, false);

        // Verify ascending order by action
        String action1 = events.get(0).getAction().toString();
        String action2 = events.get(1).getAction().toString();
        assertTrue(action1.compareToIgnoreCase(action2) <= 0);
    }

    @Test
    public void testSortEventsWithInvalidColumn() {
        List<EntityAuditEventV2> events = createTestEventList();

        // Invalid column should default to timestamp sorting
        EntityAuditEventV2.sortEvents(events, "invalid_column", false);

        // Should still sort by timestamp
        assertTrue(events.get(0).getTimestamp() <= events.get(1).getTimestamp());
    }

    @Test
    public void testEntityAuditActionV2FromString() {
        assertEquals(EntityAuditEventV2.EntityAuditActionV2.fromString("ENTITY_CREATE"),
                EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);
        assertEquals(EntityAuditEventV2.EntityAuditActionV2.fromString("ENTITY_UPDATE"),
                EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE);
        assertEquals(EntityAuditEventV2.EntityAuditActionV2.fromString("ENTITY_DELETE"),
                EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE);
        assertEquals(EntityAuditEventV2.EntityAuditActionV2.fromString("ENTITY_PURGE"),
                EntityAuditEventV2.EntityAuditActionV2.ENTITY_PURGE);
        assertEquals(EntityAuditEventV2.EntityAuditActionV2.fromString("TAG_ADD"),
                EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_ADD);
        assertEquals(EntityAuditEventV2.EntityAuditActionV2.fromString("TAG_DELETE"),
                EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_DELETE);
        assertEquals(EntityAuditEventV2.EntityAuditActionV2.fromString("TAG_UPDATE"),
                EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_UPDATE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEntityAuditActionV2FromStringInvalid() {
        EntityAuditEventV2.EntityAuditActionV2.fromString("INVALID_ACTION");
    }

    @Test
    public void testEntityAuditActionV2Values() {
        EntityAuditEventV2.EntityAuditActionV2[] actions = EntityAuditEventV2.EntityAuditActionV2.values();
        assertTrue(actions.length > 0);

        // Verify some expected actions exist
        boolean foundCreate = false;
        boolean foundUpdate = false;
        boolean foundDelete = false;

        for (EntityAuditEventV2.EntityAuditActionV2 action : actions) {
            if (action == EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE) {
                foundCreate = true;
            } else if (action == EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE) {
                foundUpdate = true;
            } else if (action == EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE) {
                foundDelete = true;
            }
        }

        assertTrue(foundCreate);
        assertTrue(foundUpdate);
        assertTrue(foundDelete);
    }

    @Test
    public void testEntityAuditTypeValues() {
        EntityAuditEventV2.EntityAuditType[] types = EntityAuditEventV2.EntityAuditType.values();
        assertEquals(types.length, 2);

        assertEquals(types[0], EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V1);
        assertEquals(types[1], EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V2);
    }

    @Test
    public void testComparators() {
        EntityAuditEventV2 event1 = new EntityAuditEventV2();
        event1.setUser("alice");
        event1.setTimestamp(1000L);
        event1.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);

        EntityAuditEventV2 event2 = new EntityAuditEventV2();
        event2.setUser("bob");
        event2.setTimestamp(2000L);
        event2.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE);

        // Test UserComparator
        EntityAuditEventV2.UserComparator userComparator = new EntityAuditEventV2.UserComparator();
        assertTrue(userComparator.compare(event1, event2) < 0); // alice < bob

        // Test ActionComparator
        EntityAuditEventV2.ActionComparator actionComparator = new EntityAuditEventV2.ActionComparator();
        assertTrue(actionComparator.compare(event1, event2) < 0); // CREATE < UPDATE

        // Test TimestampComparator
        EntityAuditEventV2.TimestampComparator timestampComparator = new EntityAuditEventV2.TimestampComparator();
        assertTrue(timestampComparator.compare(event1, event2) < 0); // 1000 < 2000
    }

    @Test
    public void testUserComparatorWithSameUser() {
        EntityAuditEventV2 event1 = new EntityAuditEventV2();
        event1.setUser("alice");
        event1.setTimestamp(1000L);

        EntityAuditEventV2 event2 = new EntityAuditEventV2();
        event2.setUser("alice");
        event2.setTimestamp(2000L);

        EntityAuditEventV2.UserComparator userComparator = new EntityAuditEventV2.UserComparator();
        assertTrue(userComparator.compare(event1, event2) < 0); // Falls back to timestamp comparison
    }

    @Test
    public void testActionComparatorWithSameAction() {
        EntityAuditEventV2 event1 = new EntityAuditEventV2();
        event1.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);
        event1.setTimestamp(1000L);

        EntityAuditEventV2 event2 = new EntityAuditEventV2();
        event2.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);
        event2.setTimestamp(2000L);

        EntityAuditEventV2.ActionComparator actionComparator = new EntityAuditEventV2.ActionComparator();
        assertTrue(actionComparator.compare(event1, event2) < 0); // Falls back to timestamp comparison
    }

    @Test
    public void testSortConstants() {
        assertEquals(EntityAuditEventV2.SORT_COLUMN_USER, "user");
        assertEquals(EntityAuditEventV2.SORT_COLUMN_ACTION, "action");
        assertEquals(EntityAuditEventV2.SORT_COLUMN_TIMESTAMP, "timestamp");
    }

    @Test
    public void testSerializable() {
        assertNotNull(auditEvent);
    }

    @Test
    public void testClearable() {
        auditEvent.clear();
        assertNull(auditEvent.getEntityId());
    }

    private EntityAuditEventV2 createTestEvent() {
        EntityAuditEventV2 event = new EntityAuditEventV2();
        AtlasEntity entity = new AtlasEntity();
        entity.setTypeName("TestType");

        event.setEntityId(TEST_ENTITY_ID);
        event.setTimestamp(TEST_TIMESTAMP);
        event.setUser(TEST_USER);
        event.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);
        event.setDetails(TEST_DETAILS);
        event.setEventKey("event-key");
        event.setEntity(entity);
        event.setType(EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V2);

        return event;
    }

    private List<EntityAuditEventV2> createTestEventList() {
        List<EntityAuditEventV2> events = new ArrayList<>();

        EntityAuditEventV2 event1 = new EntityAuditEventV2();
        event1.setUser("charlie");
        event1.setTimestamp(3000L);
        event1.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE);

        EntityAuditEventV2 event2 = new EntityAuditEventV2();
        event2.setUser("alice");
        event2.setTimestamp(1000L);
        event2.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE);

        EntityAuditEventV2 event3 = new EntityAuditEventV2();
        event3.setUser("bob");
        event3.setTimestamp(2000L);
        event3.setAction(EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE);

        events.add(event1);
        events.add(event2);
        events.add(event3);

        return events;
    }
}
