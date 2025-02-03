/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.audit;

import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.commons.lang.time.DateUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AuditRepositoryTestBase {
    protected EntityAuditRepository eventRepository;

    @BeforeTest
    public void setUp() throws Exception {
        eventRepository = new InMemoryEntityAuditRepository();
    }

    @Test
    public void testAddEventsV1() throws Exception {
        EntityAuditEvent event = new EntityAuditEvent(rand(), System.currentTimeMillis(), "u1",
                EntityAuditEvent.EntityAuditAction.ENTITY_CREATE, "d1", new Referenceable(rand()));

        eventRepository.putEventsV1(event);

        List<EntityAuditEvent> events = eventRepository.listEventsV1(event.getEntityId(), null, (short) 10);

        assertEquals(events.size(), 1);
        assertEventV1Equals(events.get(0), event);
    }

    @Test
    public void testListPaginationV1() throws Exception {
        String                 id1            = "id1" + rand();
        String                 id2            = "id2" + rand();
        String                 id3            = "id3" + rand();
        long                   ts             = System.currentTimeMillis();
        Referenceable          entity         = new Referenceable(rand());
        List<EntityAuditEvent> expectedEvents = new ArrayList<>(3);

        for (int i = 0; i < 3; i++) {
            //Add events for both ids
            EntityAuditEvent event = new EntityAuditEvent(id2, ts - i, "user" + i, EntityAuditEvent.EntityAuditAction.ENTITY_UPDATE, "details" + i, entity);

            eventRepository.putEventsV1(event);
            expectedEvents.add(event);
            eventRepository.putEventsV1(new EntityAuditEvent(id1, ts - i, "user" + i, EntityAuditEvent.EntityAuditAction.TAG_DELETE, "details" + i, entity));
            eventRepository.putEventsV1(new EntityAuditEvent(id3, ts - i, "user" + i, EntityAuditEvent.EntityAuditAction.TAG_ADD, "details" + i, entity));
        }

        //Use ts for which there is no event - ts + 2
        List<EntityAuditEvent> events = eventRepository.listEventsV1(id2, null, (short) 3);
        assertEquals(events.size(), 3);
        assertEventV1Equals(events.get(0), expectedEvents.get(0));
        assertEventV1Equals(events.get(1), expectedEvents.get(1));
        assertEventV1Equals(events.get(2), expectedEvents.get(2));

        //Use last event's timestamp for next list(). Should give only 1 event and shouldn't include events from other id
        events = eventRepository.listEventsV1(id2, events.get(2).getEventKey(), (short) 3);
        assertEquals(events.size(), 1);
        assertEventV1Equals(events.get(0), expectedEvents.get(2));
    }

    @Test
    public void testInvalidEntityIdV1() throws Exception {
        List<EntityAuditEvent> events = eventRepository.listEventsV1(rand(), null, (short) 3);

        assertEquals(events.size(), 0);
    }

    @Test
    public void testAddEventsV2() throws Exception {
        EntityAuditEventV2 event = new EntityAuditEventV2(rand(), System.currentTimeMillis(), "u1",
                EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, "d1", new AtlasEntity(rand()));

        eventRepository.putEventsV2(event);

        List<EntityAuditEventV2> events = eventRepository.listEventsV2(event.getEntityId(), null, null, (short) 10);

        assertEquals(events.size(), 1);
        assertEventV2Equals(events.get(0), event);
    }

    @Test
    public void testListPaginationV2() throws Exception {
        String                   id1            = "id1" + rand();
        String                   id2            = "id2" + rand();
        String                   id3            = "id3" + rand();
        long                     ts             = System.currentTimeMillis();
        AtlasEntity              entity         = new AtlasEntity(rand());
        List<EntityAuditEventV2> expectedEvents = new ArrayList<>(3);

        for (int i = 0; i < 3; i++) {
            //Add events for both ids
            EntityAuditEventV2 event = new EntityAuditEventV2(id2, ts - i, "user" + i, EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, "details" + i, entity);

            eventRepository.putEventsV2(event);
            expectedEvents.add(event);
            eventRepository.putEventsV2(new EntityAuditEventV2(id1, ts - i, "user" + i, EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE, "details" + i, entity));
            eventRepository.putEventsV2(new EntityAuditEventV2(id3, ts - i, "user" + i, EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, "details" + i, entity));
        }

        //Use ts for which there is no event - ts + 2
        List<EntityAuditEventV2> events = eventRepository.listEventsV2(id2, null, null, (short) 3);
        assertEquals(events.size(), 3);
        assertEventV2Equals(events.get(0), expectedEvents.get(0));
        assertEventV2Equals(events.get(1), expectedEvents.get(1));
        assertEventV2Equals(events.get(2), expectedEvents.get(2));

        //Use last event's timestamp for next list(). Should give only 1 event and shouldn't include events from other id
        events = eventRepository.listEventsV2(id2, null, events.get(2).getEventKey(), (short) 3);
        assertEquals(events.size(), 1);
        assertEventV2Equals(events.get(0), expectedEvents.get(2));
    }

    @Test
    public void testInvalidEntityIdV2() throws Exception {
        List<EntityAuditEvent> events = eventRepository.listEventsV1(rand(), null, (short) 3);

        assertEquals(events.size(), 0);
    }

    @Test
    public void testSortListV2() throws Exception {
        String                   id1            = "id1" + rand();
        long                     ts             = System.currentTimeMillis();
        AtlasEntity              entity         = new AtlasEntity(rand());
        List<EntityAuditEventV2> expectedEvents = new ArrayList<>(3);

        expectedEvents.add(new EntityAuditEventV2(id1, ts, "user-a", EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, "details-1", entity));
        expectedEvents.add(new EntityAuditEventV2(id1, ts + 1, "user-C", EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE, "details-2", entity));
        expectedEvents.add(new EntityAuditEventV2(id1, ts + 2, "User-b", EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, "details-3", entity));
        for (EntityAuditEventV2 event : expectedEvents) {
            eventRepository.putEventsV2(event);
        }

        List<EntityAuditEventV2> events = eventRepository.listEventsV2(id1, null, "timestamp", false, 0, (short) 2);
        assertEquals(events.size(), 2);
        assertEventV2Equals(events.get(0), expectedEvents.get(0));
        assertEventV2Equals(events.get(1), expectedEvents.get(1));

        events = eventRepository.listEventsV2(id1, null, "user", false, 0, (short) 3);
        assertEquals(events.size(), 3);
        assertEventV2Equals(events.get(0), expectedEvents.get(0));
        assertEventV2Equals(events.get(1), expectedEvents.get(2));
        assertEventV2Equals(events.get(2), expectedEvents.get(1));

        events = eventRepository.listEventsV2(id1, null, "action", false, 0, (short) 3);
        assertEquals(events.size(), 3);
        assertEventV2Equals(events.get(0), expectedEvents.get(2));
        assertEventV2Equals(events.get(1), expectedEvents.get(1));
        assertEventV2Equals(events.get(2), expectedEvents.get(0));
    }

    @Test
    public void testDeleteEventsV2() throws Exception {
        String      id1       = "id1" + rand();
        int         ttlInDays = 1;
        long        ts        = System.currentTimeMillis() - (ttlInDays * DateUtils.MILLIS_PER_DAY);
        AtlasEntity entity    = new AtlasEntity(rand());

        int                      j              = 0;
        List<EntityAuditEventV2> expectedEvents = new ArrayList<>();
        expectedEvents.add(new EntityAuditEventV2(id1, ts + j++, "user-a", EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE, "details" + j, entity));
        expectedEvents.add(new EntityAuditEventV2(id1, ts + j++, "user-C", EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, "details" + j, entity));
        for (int i = 0; i < 5; i++) {
            expectedEvents.add(new EntityAuditEventV2(id1, ts + j++, "user" + i, EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, "details" + j, entity));
            expectedEvents.add(new EntityAuditEventV2(id1, ts + j++, "user" + i, EntityAuditEventV2.EntityAuditActionV2.CLASSIFICATION_ADD, "details" + j, entity));
        }
        expectedEvents.add(new EntityAuditEventV2(id1, ts + j++, "User-b", EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE, "details" + j, entity));
        for (EntityAuditEventV2 event : expectedEvents) {
            eventRepository.putEventsV2(event);
        }

        List<EntityAuditEventV2> events = eventRepository.listEventsV2(id1, null, "timestamp", false, 0, (short) -1);
        assertEquals(events.size(), 13);
        assertEventV2Equals(events.get(0), expectedEvents.get(0));
        assertEventV2Equals(events.get(1), expectedEvents.get(1));

        short                    expectedUpdateEventsCount = 2;
        List<EntityAuditEventV2> deletedUpdateEvents       = eventRepository.deleteEventsV2(id1, new HashSet<>(Arrays.asList(EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE)), expectedUpdateEventsCount, 0, false, AtlasAuditAgingType.CUSTOM);
        List<EntityAuditEventV2> remainingEvents           = events.stream().filter(x -> !deletedUpdateEvents.contains(x)).collect(Collectors.toList());
        List<EntityAuditEventV2> remainingUpdateEvents     = remainingEvents.stream().filter(x -> x.getAction() == EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE).collect(Collectors.toList());

        assertEquals(remainingUpdateEvents.size(), expectedUpdateEventsCount);

        short                    expectedEventsCount = 4;
        List<EntityAuditEventV2> deletedEvents       = eventRepository.deleteEventsV2(id1, null, expectedEventsCount, 0, false, AtlasAuditAgingType.DEFAULT);
        remainingEvents = events.stream().filter(x -> !deletedEvents.contains(x)).collect(Collectors.toList());

        assertEquals(remainingEvents.size(), expectedEventsCount + 1);
        assertEventV2Equals(remainingEvents.get(0), events.get(0));
        assertTrue(remainingEvents.contains(events.get(0)));

        List<EntityAuditEventV2> deletedEventsIncludingCreate = eventRepository.deleteEventsV2(id1, null, expectedEventsCount, 0, true, AtlasAuditAgingType.DEFAULT);
        remainingEvents = events.stream().filter(x -> !deletedEventsIncludingCreate.contains(x)).collect(Collectors.toList());

        assertEquals(remainingEvents.size(), expectedEventsCount);
        assertNotEquals(remainingEvents.get(3), events.get(0));
        assertFalse(remainingEvents.contains(events.get(0)));

        EntityAuditEventV2 latestEvent = new EntityAuditEventV2(id1, System.currentTimeMillis(), "User-b", EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE, "details" + j++, entity);
        eventRepository.putEventsV2(latestEvent);
        List<EntityAuditEventV2> allEvents = eventRepository.listEventsV2(id1, null, "timestamp", false, 0, (short) -1);

        List<EntityAuditEventV2> deletedEventsByTTL = eventRepository.deleteEventsV2(id1, null, expectedUpdateEventsCount, ttlInDays, true, AtlasAuditAgingType.DEFAULT);
        assertEquals(deletedEventsByTTL.size(), allEvents.size() - 1);

        List<EntityAuditEventV2> remainingEventsByTTL = allEvents.stream().filter(x -> !deletedEventsByTTL.contains(x)).collect(Collectors.toList());
        assertEquals(remainingEventsByTTL.size(), 1);
        assertEquals(latestEvent, remainingEventsByTTL.get(0));
    }

    protected void assertEventV1Equals(EntityAuditEvent actual, EntityAuditEvent expected) {
        if (expected != null) {
            assertNotNull(actual);
        }

        assertEquals(actual.getEntityId(), expected.getEntityId());
        assertEquals(actual.getAction(), expected.getAction());
        assertEquals(actual.getTimestamp(), expected.getTimestamp());
        assertEquals(actual.getDetails(), expected.getDetails());
    }

    protected void assertEventV2Equals(EntityAuditEventV2 actual, EntityAuditEventV2 expected) {
        if (expected != null) {
            assertNotNull(actual);
        }

        assertEquals(actual.getEntityId(), expected.getEntityId());
        assertEquals(actual.getAction(), expected.getAction());
        assertEquals(actual.getTimestamp(), expected.getTimestamp());
        assertEquals(actual.getDetails(), expected.getDetails());
    }

    private String rand() {
        return TestUtilsV2.randomString(10);
    }
}
