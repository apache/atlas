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

package org.apache.atlas;

import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.commons.lang.StringUtils;

/**
 * Structure of entity audit event
 */
public class EntityAuditEvent {
    public enum EntityAuditAction {
        ENTITY_CREATE, ENTITY_UPDATE, ENTITY_DELETE, TAG_ADD, TAG_DELETE
    }

    private String entityId;
    private long timestamp;
    private String user;
    private EntityAuditAction action;
    private String details;
    private String eventKey;
    private IReferenceableInstance entityDefinition;

    public EntityAuditEvent() {
    }

    public EntityAuditEvent(String entityId, Long ts, String user, EntityAuditAction action, String details,
                            IReferenceableInstance entityDefinition) throws AtlasException {
        this.entityId = entityId;
        this.timestamp = ts;
        this.user = user;
        this.action = action;
        this.details = details;
        this.entityDefinition = entityDefinition;
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
                    (timestamp == otherEvent.timestamp) &&
                    StringUtils.equals(user, otherEvent.user) &&
                    (action == otherEvent.action) &&
                    StringUtils.equals(details, otherEvent.details) &&
                    StringUtils.equals(eventKey, otherEvent.eventKey) &&
                    StringUtils.equals(getEntityDefinitionString(), otherEvent.getEntityDefinitionString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return SerDe.GSON.toJson(this);
    }

    public static EntityAuditEvent fromString(String eventString) {
        return SerDe.GSON.fromJson(eventString, EntityAuditEvent.class);
    }

    public String getEntityId() {
        return entityId;
    }

    public long getTimestamp() {
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

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setAction(EntityAuditAction action) {
        this.action = action;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public String getEventKey() {
        return eventKey;
    }

    public void setEventKey(String eventKey) {
        this.eventKey = eventKey;
    }

    public IReferenceableInstance getEntityDefinition() {
        return entityDefinition;
    }

    public String getEntityDefinitionString() {
        if (entityDefinition != null) {
            return InstanceSerialization.toJson(entityDefinition, true);
        }
        return null;
    }

    public void setEntityDefinition(String entityDefinition) {
        this.entityDefinition = InstanceSerialization.fromJsonReferenceable(entityDefinition, true);
    }
}
