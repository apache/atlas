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
package org.apache.atlas.repository.audit.rdbms;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.repository.audit.AbstractStorageBasedAuditRepository;
import org.apache.atlas.repository.audit.rdbms.dao.DbEntityAuditDao;
import org.apache.atlas.repository.audit.rdbms.entity.DbEntityAudit;
import org.apache.commons.configuration.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Singleton;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
@Component
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepository.impl", isDefault = true)
@Order(0)
public class RdbmsBasedAuditRepository extends AbstractStorageBasedAuditRepository {
    @Override
    public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {
        // TODO: is V1 support needed anymore?
    }

    @Override
    public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) throws AtlasException {
        // TODO: is V1 support needed anymore?
        return Collections.emptyList();
    }

    @Override
    public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
        try (RdbmsTransaction trx = new RdbmsTransaction()) {
            DbEntityAuditDao dao = new DbEntityAuditDao(trx.getEntityManager());

            for (int i = 0; i < events.size(); i++) {
                EntityAuditEventV2 event   = events.get(i);
                DbEntityAudit      dbEvent = toDbEntityAudit(event);

                dbEvent.setEventIndex(i);

                dao.create(dbEvent);
            }

            trx.commit();
        } catch (Exception excp) {
            throw new AtlasBaseException("Error while persisting audit events", excp);
        }
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String startKey, short maxResultCount) throws AtlasBaseException {
        try (RdbmsTransaction trx = new RdbmsTransaction()) {
            DbEntityAuditDao dao = new DbEntityAuditDao(trx.getEntityManager());

            List<DbEntityAudit> dbEvents = dao.getByEntityIdActionStartTimeStartIdx(entityId, auditAction.ordinal(), getTimestampFromKey(startKey), getIndexFromKey(startKey), maxResultCount);

            return dbEvents.stream().map(RdbmsBasedAuditRepository::fromDbEntityAudit).collect(Collectors.toList());
        } catch (Exception excp) {
            throw new AtlasBaseException("Error while retrieving audit events", excp);
        }
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) throws AtlasBaseException {
        try (RdbmsTransaction trx = new RdbmsTransaction()) {
            DbEntityAuditDao dao = new DbEntityAuditDao(trx.getEntityManager());

            List<DbEntityAudit> dbEvents = dao.getByEntityIdAction(entityId, auditAction == null ? null : auditAction.ordinal(), offset, limit);

            return dbEvents.stream().map(RdbmsBasedAuditRepository::fromDbEntityAudit).collect(Collectors.toList());
        } catch (Exception excp) {
            throw new AtlasBaseException("Error while retrieving audit events", excp);
        }
    }

    @Override
    public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
        // TODO:
        return Collections.emptySet();
    }

    @Override
    public void start() throws AtlasException {
    }

    @Override
    public void stop() throws AtlasException {
    }

    public static DbEntityAudit toDbEntityAudit(EntityAuditEventV2 event) {
        DbEntityAudit ret = new DbEntityAudit();

        ret.setEntityId(event.getEntityId());
        ret.setEventTime(event.getTimestamp());
        ret.setUser(event.getUser());
        ret.setAction(event.getAction().ordinal());
        ret.setDetails(event.getDetails());

        if (event.getType() == null) {
            ret.setAuditType(EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V2.ordinal());
        } else {
            ret.setAuditType(event.getType().ordinal());
        }

        if (PERSIST_ENTITY_DEFINITION) {
            ret.setEntity(event.getEntityDefinitionString());
        }

        return ret;
    }

    public static EntityAuditEventV2 fromDbEntityAudit(DbEntityAudit dbEntityAudit) {
        EntityAuditEventV2 ret = new EntityAuditEventV2();

        ret.setEntityId(dbEntityAudit.getEntityId());
        ret.setTimestamp(dbEntityAudit.getEventTime());
        ret.setUser(dbEntityAudit.getUser());
        ret.setAction(EntityAuditEventV2.EntityAuditActionV2.values()[dbEntityAudit.getAction()]);
        ret.setDetails(dbEntityAudit.getDetails());
        ret.setType(EntityAuditEventV2.EntityAuditType.values()[dbEntityAudit.getAuditType()]);

        if (PERSIST_ENTITY_DEFINITION) {
            ret.setEntityDefinition(dbEntityAudit.getEntity());
        }

        return ret;
    }

    private Configuration getConfiguration() {
        try {
            return ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException("Failed to get application properties", e);
        }
    }
}
