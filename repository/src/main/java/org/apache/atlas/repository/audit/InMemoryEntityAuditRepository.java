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

import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Entity audit repository where audit events are stored in-memory. Used only for integration tests
 */
public class InMemoryEntityAuditRepository implements EntityAuditRepository {
    private TreeMap<String, EntityAuditEvent> auditEvents = new TreeMap<>();

    @Override
    public void putEvents(EntityAuditEvent... events) throws AtlasException {
        putEvents(Arrays.asList(events));
    }

    @Override
    public synchronized void putEvents(List<EntityAuditEvent> events) throws AtlasException {
        for (EntityAuditEvent event : events) {
            String rowKey = event.getEntityId() + (Long.MAX_VALUE - event.getTimestamp());
            event.setEventKey(rowKey);
            auditEvents.put(rowKey, event);
        }
    }

    @Override
    public List<EntityAuditEvent> listEvents(String entityId, String startKey, short maxResults)
            throws AtlasException {
        List<EntityAuditEvent> events = new ArrayList<>();
        String myStartKey = startKey;
        if (myStartKey == null) {
            myStartKey = entityId;
        }
        SortedMap<String, EntityAuditEvent> subMap = auditEvents.tailMap(myStartKey);
        for (EntityAuditEvent event : subMap.values()) {
            if (events.size() < maxResults && event.getEntityId().equals(entityId)) {
                events.add(event);
            }
        }
        return events;
    }
}
