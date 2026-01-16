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
package org.apache.atlas.repository.audit.rdbms.entity;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import java.util.Objects;

import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;

/**
 * RDBMS representation of a JanusGraph Column - name/value pair in a JanusGraph key
 *
 * @author Madhan Neethiraj &lt;madhan@apache.org&gt;
 */
@Entity
@Cacheable(false)
@Table(name = "atlas_entity_audit",
        indexes = {@Index(name = "atlas_entity_audit_idx_entity_id", columnList = "entity_id"),
                   @Index(name = "atlas_entity_audit_idx_event_time", columnList = "event_time"),
                   @Index(name = "atlas_entity_audit_idx_entity_id_event_time", columnList = "entity_id,event_time"),
                   @Index(name = "atlas_entity_audit_idx_user_name", columnList = "user_name")})
public class DbEntityAudit implements java.io.Serializable {
    @Id
    @SequenceGenerator(name = "atlas_entity_audit_seq", sequenceName = "atlas_entity_audit_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "atlas_entity_audit_seq")
    @Column(name = "id")
    protected Long id;

    @Column(name = "entity_id", nullable = false, length = 64)
    protected String entityId;

    @Column(name = "event_time", nullable = false)
    protected long eventTime;

    @Column(name = "event_idx", nullable = false)
    protected int eventIndex;

    @Column(name = "user_name", nullable = false, length = 64)
    protected String user;

    @Column(name = "operation", nullable = false, length = 64)
    protected String action;

    @Column(name = "details")
    @Lob
    protected String details;

    @Column(name = "entity")
    @Lob
    protected String entity;

    @Column(name = "audit_type")
    protected int auditType;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public int getEventIndex() {
        return eventIndex;
    }

    public void setEventIndex(int eventIndex) {
        this.eventIndex = eventIndex;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public void setAction(EntityAuditActionV2 actionEnum) {
        this.action = (actionEnum == null) ? null : actionEnum.name();
    }

    public EntityAuditActionV2 getActionEnum() {
        if (action == null) return null;
        try {
            return EntityAuditActionV2.valueOf(action);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public int getAuditType() {
        return auditType;
    }

    public void setAuditType(int auditType) {
        this.auditType = auditType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, entityId, eventTime, eventIndex, user, action, details, entity, auditType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof DbEntityAudit && getClass() == obj.getClass()) {
            DbEntityAudit other = (DbEntityAudit) obj;

            return Objects.equals(id, other.id) &&
                    Objects.equals(entityId, other.entityId) &&
                    eventTime == other.eventTime &&
                    eventIndex == other.eventIndex &&
                    Objects.equals(user, other.user) &&
                    Objects.equals(action, other.action) &&
                    Objects.equals(details, other.details) &&
                    Objects.equals(entity, other.entity) &&
                    auditType == other.auditType;
        }

        return false;
    }

    @Override
    public String toString() {
        return "DbEntityAudit(id=" + id + ", entityId=" + entityId + ", eventTime=" + eventTime + ", eventIndex=" + eventIndex + ", user=" + user + ", action=" + action + ", details=" + details + ", entity=" + entity + ", auditType=" + auditType + ")";
    }
}
