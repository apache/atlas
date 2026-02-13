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
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.audit.AbstractStorageBasedAuditRepository;
import org.apache.atlas.repository.audit.rdbms.dao.DbEntityAuditDao;
import org.apache.atlas.repository.audit.rdbms.entity.DbEntityAudit;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Singleton;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.model.audit.EntityAuditEventV2.SORT_COLUMN_ACTION;
import static org.apache.atlas.model.audit.EntityAuditEventV2.SORT_COLUMN_TIMESTAMP;
import static org.apache.atlas.model.audit.EntityAuditEventV2.SORT_COLUMN_USER;

@Singleton
@Component
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepository.impl", isDefault = false)
@Order(0)
public class RdbmsBasedAuditRepository extends AbstractStorageBasedAuditRepository {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsBasedAuditRepository.class);

    @Override
    public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) throws AtlasException {
        throw new UnsupportedOperationException();
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

            List<DbEntityAudit> dbEvents = dao.getByEntityIdActionStartTimeStartIdx(entityId, auditAction == null ? null : auditAction.name(), getTimestampFromKey(startKey), getIndexFromKey(startKey), maxResultCount);

            return dbEvents.stream().map(RdbmsBasedAuditRepository::fromDbEntityAudit).collect(Collectors.toList());
        } catch (Exception excp) {
            throw new AtlasBaseException("Error while retrieving audit events", excp);
        }
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) throws AtlasBaseException {
        try (RdbmsTransaction trx = new RdbmsTransaction()) {
            DbEntityAuditDao dao = new DbEntityAuditDao(trx.getEntityManager());

            List<DbEntityAudit> dbEvents = dao.getByEntityIdAction(entityId, auditAction == null ? null : auditAction.name(), mapSortColumnToDbColumn(sortByColumn), sortOrderDesc, offset, limit);

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

    @Override
    public List<EntityAuditEventV2> deleteEventsV2(String entityId,
            Set<EntityAuditEventV2.EntityAuditActionV2> entityAuditActions,
            short allowedAuditCount,
            int ttlInDays,
            boolean createEventsAgeoutAllowed,
            Constants.AtlasAuditAgingType auditAgingType) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("deleteEventsV2");

        try (RdbmsTransaction trx = new RdbmsTransaction()) {
            DbEntityAuditDao dao = new DbEntityAuditDao(trx.getEntityManager());
            List<EntityAuditEventV2> deletedEvents = new ArrayList<>();

            boolean allowAgeoutByAuditCount = allowedAuditCount > 0 || (auditAgingType == Constants.AtlasAuditAgingType.SWEEP);
            List<String> ageoutActionsNotAllowed = createEventsAgeoutAllowed ? Collections.emptyList() :
                    Arrays.asList(EntityAuditEventV2.EntityAuditActionV2.ENTITY_CREATE.name(),
                            EntityAuditEventV2.EntityAuditActionV2.ENTITY_IMPORT_CREATE.name());

            List<DbEntityAudit> eventsEligibleForAgeout = new ArrayList<>();
            List<DbEntityAudit> eventsToKeep = new ArrayList<>();

            if (CollectionUtils.isEmpty(entityAuditActions)) {
                List<DbEntityAudit> dbEvents = dao.getLatestAuditsByEntityIdAction(entityId, null, ageoutActionsNotAllowed);

                splitEventsToKeepAndAgeoutByAuditCount(dbEvents, allowAgeoutByAuditCount, allowedAuditCount, eventsToKeep, eventsEligibleForAgeout);
            } else {
                for (EntityAuditEventV2.EntityAuditActionV2 action : entityAuditActions) {
                    String actionName = action == null ? null : action.name();
                    List<DbEntityAudit> dbEvents = dao.getLatestAuditsByEntityIdAction(entityId, actionName, ageoutActionsNotAllowed);

                    splitEventsToKeepAndAgeoutByAuditCount(dbEvents, allowAgeoutByAuditCount, allowedAuditCount, eventsToKeep, eventsEligibleForAgeout);
                }
            }

            if (CollectionUtils.isNotEmpty(eventsToKeep)) {
                //Limit events based on configured audit count by grouping events of all action types
                if (allowAgeoutByAuditCount && (auditAgingType == Constants.AtlasAuditAgingType.DEFAULT || CollectionUtils.isEmpty(entityAuditActions))) {
                    LOG.debug("Aging out audit events by audit count for entity: {}", entityId);

                    eventsToKeep.sort((a, b) -> {
                        int cmp = Long.compare(b.getEventTime(), a.getEventTime());
                        if (cmp == 0) {
                            cmp = Integer.compare(b.getEventIndex(), a.getEventIndex());
                        }
                        return cmp;
                    });

                    if (allowedAuditCount < eventsToKeep.size()) {
                        eventsEligibleForAgeout.addAll(eventsToKeep.subList(allowedAuditCount, eventsToKeep.size()));
                        eventsToKeep = eventsToKeep.subList(0, allowedAuditCount);
                    }
                }

                //TTL based aging
                if (ttlInDays > 0) {
                    LOG.debug("Aging out audit events by TTL for entity: {}", entityId);

                    LocalDateTime now = LocalDateTime.now();
                    boolean isTTLTestAutomation = AtlasConfiguration.ATLAS_AUDIT_AGING_TTL_TEST_AUTOMATION.getBoolean();
                    long ttlTimestamp = Timestamp.valueOf(isTTLTestAutomation ? now.minusMinutes(ttlInDays) : now.minusDays(ttlInDays)).getTime();

                    eventsToKeep.forEach(e -> {
                        if (e.getEventTime() < ttlTimestamp) {
                            eventsEligibleForAgeout.add(e);
                        }
                    });
                }
            }

            Map<Long, DbEntityAudit> uniqueEligible = new LinkedHashMap<>();
            for (DbEntityAudit db : eventsEligibleForAgeout) {
                if (db != null && db.getId() != null) {
                    uniqueEligible.put(db.getId(), db);
                }
            }

            LOG.debug("Deleting events from audit table: {}", uniqueEligible.keySet());
            for (DbEntityAudit dbEvent : uniqueEligible.values()) {
                try {
                    trx.getEntityManager().remove(dbEvent);
                    deletedEvents.add(fromDbEntityAudit(dbEvent));
                } catch (Exception e) {
                    throw new AtlasBaseException("Failed to remove audit event: " + dbEvent, e);
                }
            }

            trx.commit();
            return deletedEvents;
        } catch (Exception e) {
            throw new AtlasBaseException("Error while deleting audit events", e);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private void splitEventsToKeepAndAgeoutByAuditCount(List<DbEntityAudit> dbEvents, boolean allowAgeoutByAuditCount, int allowedAuditCount,
            List<DbEntityAudit> eventsToKeep, List<DbEntityAudit> eventsEligibleForAgeout) {
        if (CollectionUtils.isEmpty(dbEvents)) {
            return;
        }

        if (!allowAgeoutByAuditCount) {
            eventsToKeep.addAll(dbEvents);
        } else {
            int limit = Math.max(0, allowedAuditCount);
            for (int i = 0; i < dbEvents.size(); i++) {
                if (i < limit) {
                    eventsToKeep.add(dbEvents.get(i));
                } else {
                    eventsEligibleForAgeout.add(dbEvents.get(i));
                }
            }
        }
    }

    public static DbEntityAudit toDbEntityAudit(EntityAuditEventV2 event) {
        DbEntityAudit ret = new DbEntityAudit();

        ret.setEntityId(event.getEntityId());
        ret.setEventTime(event.getTimestamp());
        ret.setUser(event.getUser());
        ret.setAction(event.getAction() != null ? event.getAction().name() : null);
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
        ret.setAction(dbEntityAudit.getAction() != null ? EntityAuditEventV2.EntityAuditActionV2.valueOf(dbEntityAudit.getAction()) : null);
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

    private List<String> mapSortColumnToDbColumn(String sortByColumn) {
        if (StringUtils.isEmpty(sortByColumn)) {
            return null;
        }
        switch (sortByColumn) {
            case SORT_COLUMN_USER:
                return java.util.Arrays.asList("user", "eventTime", "eventIndex");
            case SORT_COLUMN_ACTION:
                return java.util.Arrays.asList("action", "eventTime", "eventIndex");
            case SORT_COLUMN_TIMESTAMP:
            default:
                return java.util.Arrays.asList("eventTime", "eventIndex");
        }
    }
}
