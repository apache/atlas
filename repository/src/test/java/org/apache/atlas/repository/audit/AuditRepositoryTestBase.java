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

import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class AuditRepositoryTestBase {
    protected EntityAuditRepository eventRepository;

    private String rand() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    @Test
    public void testAddEvents() throws Exception {
        EntityAuditRepository.EntityAuditEvent event =
                new EntityAuditRepository.EntityAuditEvent(rand(), System.currentTimeMillis(), "u1",
                        EntityAuditRepository.EntityAuditAction.ENTITY_CREATE, "d1");

        eventRepository.putEvents(event);

        List<EntityAuditRepository.EntityAuditEvent> events =
                eventRepository.listEvents(event.entityId, System.currentTimeMillis(), (short) 10);
        assertEquals(events.size(), 1);
        assertEquals(events.get(0), event);
    }

    @Test
    public void testListPagination() throws Exception {
        String id1 = "id1" + rand();
        String id2 = "id2" + rand();
        String id3 = "id3" + rand();
        long ts = System.currentTimeMillis();
        List<EntityAuditRepository.EntityAuditEvent> expectedEvents = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            //Add events for both ids
            EntityAuditRepository.EntityAuditEvent event =
                    new EntityAuditRepository.EntityAuditEvent(id2, ts - i, "user" + i,
                            EntityAuditRepository.EntityAuditAction.ENTITY_UPDATE, "details" + i);
            eventRepository.putEvents(event);
            expectedEvents.add(event);
            eventRepository.putEvents(new EntityAuditRepository.EntityAuditEvent(id1, ts - i, "user" + i,
                    EntityAuditRepository.EntityAuditAction.TAG_DELETE, "details" + i));
            eventRepository.putEvents(new EntityAuditRepository.EntityAuditEvent(id3, ts - i, "user" + i,
                    EntityAuditRepository.EntityAuditAction.TAG_ADD, "details" + i));
        }

        //Use ts for which there is no event - ts + 2
        List<EntityAuditRepository.EntityAuditEvent> events = eventRepository.listEvents(id2, ts + 2, (short) 2);
        assertEquals(events.size(), 2);
        assertEquals(events.get(0), expectedEvents.get(0));
        assertEquals(events.get(1), expectedEvents.get(1));

        //Use last event's timestamp for next list(). Should give only 1 event and shouldn't include events from other id
        events = eventRepository.listEvents(id2, events.get(1).timestamp - 1, (short) 3);
        assertEquals(events.size(), 1);
        assertEquals(events.get(0), expectedEvents.get(2));
    }
}
