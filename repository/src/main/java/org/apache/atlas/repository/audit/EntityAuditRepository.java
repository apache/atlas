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
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Interface for repository for storing entity audit events
 */
public interface EntityAuditRepository {
    enum EntityAuditAction {
        ENTITY_CREATE, ENTITY_UPDATE, ENTITY_DELETE, TAG_ADD, TAG_DELETE;
    }

    /**
     * Structure of entity audit event
     */
    class EntityAuditEvent {
        String entityId;
        Long timestamp;
        String user;
        EntityAuditAction action;
        String details;

        public EntityAuditEvent() {
        }

        public EntityAuditEvent(String entityId, Long ts, String user, EntityAuditAction action, String details) {
            this.entityId = entityId;
            this.timestamp = ts;
            this.user = user;
            this.action = action;
            this.details = details;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof EntityAuditEvent)) {
                return false;
            }

            EntityAuditEvent otherEvent = (EntityAuditEvent) other;
            return StringUtils.equals(entityId, otherEvent.entityId) &&
                    (timestamp.longValue() == otherEvent.timestamp.longValue()) &&
                    StringUtils.equals(user, otherEvent.user) && (action == otherEvent.action) &&
                    StringUtils.equals(details, otherEvent.details);
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("EntityId=").append(entityId).append(";Timestamp=").append(timestamp).append(";User=")
                   .append(user).append(";Action=").append(action).append(";Details=").append(details);
            return builder.toString();
        }

        public String getEntityId() {
            return entityId;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public String getUser() {
            return user;
        }

        public EntityAuditAction getAction() {
            return action;
        }

        public String getDetails() {
            return details;
        }
    }

    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    void putEvents(EntityAuditEvent... events) throws AtlasException;

    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    void putEvents(List<EntityAuditEvent> events) throws AtlasException;

    /**
     * List events for the given entity id in decreasing order of timestamp, from the given timestamp. Returns n results
     * @param entityId entity id
     * @param ts starting timestamp for events
     * @param n number of events to be returned
     * @return list of events
     * @throws AtlasException
     */
    List<EntityAuditRepository.EntityAuditEvent> listEvents(String entityId, Long ts, short n) throws AtlasException;
}
