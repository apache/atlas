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
import org.apache.atlas.v1.model.instance.Referenceable;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class AuditRepositoryTestBase {
    protected EntityAuditRepository eventRepository;

    private String rand() {
         return TestUtilsV2.randomString(10);
    }

    @BeforeTest
    public void setUp() throws Exception{
        eventRepository = new InMemoryEntityAuditRepository();
    }

    @Test
    public void testAddEvents() throws Exception {
        EntityAuditEvent event = new EntityAuditEvent(rand(), System.currentTimeMillis(), "u1",
                EntityAuditEvent.EntityAuditAction.ENTITY_CREATE, "d1", new Referenceable(rand()));

        eventRepository.putEvents(event);

        List<EntityAuditEvent> events = eventRepository.listEvents(event.getEntityId(), null, (short) 10);

        assertEquals(events.size(), 1);
        assertEventEquals(events.get(0), event);
    }

    @Test
    public void testListPagination() throws Exception {
        String                 id1            = "id1" + rand();
        String                 id2            = "id2" + rand();
        String                 id3            = "id3" + rand();
        long                   ts             = System.currentTimeMillis();
        Referenceable          entity         = new Referenceable(rand());
        List<EntityAuditEvent> expectedEvents = new ArrayList<>(3);

        for (int i = 0; i < 3; i++) {
            //Add events for both ids
            EntityAuditEvent event = new EntityAuditEvent(id2, ts - i, "user" + i, EntityAuditEvent.EntityAuditAction.ENTITY_UPDATE, "details" + i, entity);

            eventRepository.putEvents(event);
            expectedEvents.add(event);
            eventRepository.putEvents(new EntityAuditEvent(id1, ts - i, "user" + i, EntityAuditEvent.EntityAuditAction.TAG_DELETE, "details" + i, entity));
            eventRepository.putEvents(new EntityAuditEvent(id3, ts - i, "user" + i, EntityAuditEvent.EntityAuditAction.TAG_ADD, "details" + i, entity));
        }

        //Use ts for which there is no event - ts + 2
        List<EntityAuditEvent> events = eventRepository.listEvents(id2, null, (short) 3);
        assertEquals(events.size(), 3);
        assertEventEquals(events.get(0), expectedEvents.get(0));
        assertEventEquals(events.get(1), expectedEvents.get(1));
        assertEventEquals(events.get(2), expectedEvents.get(2));

        //Use last event's timestamp for next list(). Should give only 1 event and shouldn't include events from other id
        events = eventRepository.listEvents(id2, events.get(2).getEventKey(), (short) 3);
        assertEquals(events.size(), 1);
        assertEventEquals(events.get(0), expectedEvents.get(2));
    }

    @Test
    public void testInvalidEntityId() throws Exception {
        List<EntityAuditEvent> events = eventRepository.listEvents(rand(), null, (short) 3);

        assertEquals(events.size(), 0);
    }

    protected void assertEventEquals(EntityAuditEvent actual, EntityAuditEvent expected) {
        if (expected != null) {
            assertNotNull(actual);
        }

        assertEquals(actual.getEntityId(), expected.getEntityId());
        assertEquals(actual.getAction(), expected.getAction());
        assertEquals(actual.getTimestamp(), expected.getTimestamp());
        assertEquals(actual.getDetails(), expected.getDetails());
    }
}
