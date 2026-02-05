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
package org.apache.atlas.repository.audit.rdbms.dao;

import org.apache.atlas.repository.audit.rdbms.entity.DbEntityAudit;
import org.apache.commons.collections.CollectionUtils;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.List;

public class DbEntityAuditDao extends BaseDao<DbEntityAudit> {
    public DbEntityAuditDao(EntityManager em) {
        super(em);
    }

    public List<DbEntityAudit> getByEntityIdActionStartTimeStartIdx(String entityId, String action, long eventTimeStart, int eventIdxStart, int maxResults) {
        try {
            if (action == null) {
                return em.createNamedQuery("DbEntityAudit.getByEntityIdStartTimeStartIdx", DbEntityAudit.class)
                        .setParameter("entityId", entityId)
                        .setParameter("eventTimeStart", eventTimeStart)
                        .setParameter("eventIdxStart", eventIdxStart)
                        .setMaxResults(maxResults)
                        .getResultList();
            } else {
                return em.createNamedQuery("DbEntityAudit.getByEntityIdActionStartTimeStartIdx", DbEntityAudit.class)
                        .setParameter("entityId", entityId)
                        .setParameter("action", action)
                        .setParameter("eventTimeStart", eventTimeStart)
                        .setParameter("eventIdxStart", eventIdxStart)
                        .setMaxResults(maxResults)
                        .getResultList();
            }
        } catch (NoResultException excp) {
            // ignore
        }

        return Collections.emptyList();
    }

    public List<DbEntityAudit> getByEntityIdAction(String entityId, String action, List<String> sortByColumn, boolean sortOrder, int startIdx, int maxResults) {
        try {
            StringBuilder query = new StringBuilder("SELECT e FROM DbEntityAudit e WHERE e.entityId = :entityId");

            if (action != null) {
                query.append(" and e.action = :action");
                if (CollectionUtils.isNotEmpty(sortByColumn)) {
                    query.append(getOrderByQuery(sortByColumn, sortOrder));
                }
                return em.createQuery(query.toString(), DbEntityAudit.class)
                        .setParameter("entityId", entityId)
                        .setParameter("action", action)
                        .setFirstResult(startIdx)
                        .setMaxResults(maxResults)
                        .getResultList();
            } else {
                if (CollectionUtils.isNotEmpty(sortByColumn)) {
                    query.append(getOrderByQuery(sortByColumn, sortOrder));
                }
                return em.createQuery(query.toString(), DbEntityAudit.class)
                        .setParameter("entityId", entityId)
                        .setFirstResult(startIdx)
                        .setMaxResults(maxResults)
                        .getResultList();
            }
        } catch (NoResultException excp) {
            // ignore
        }

        return Collections.emptyList();
    }

    public String getOrderByQuery(List<String> sortByColumn, boolean sortOrderDesc) {
        StringBuilder orderByQuery = new StringBuilder(" ORDER BY ");
        for (int i = 0; i < sortByColumn.size(); i++) {
            orderByQuery.append("e.").append(sortByColumn.get(i));
            orderByQuery.append(sortOrderDesc ? " DESC" : " ASC");
            if (i != sortByColumn.size() - 1) {
                orderByQuery.append(", ");
            }
        }
        return orderByQuery.toString();
    }

    public List<DbEntityAudit> getLatestAuditsByEntityIdAction(String entityId, String action, List<String> filterActions) {
        try {
            StringBuilder query = new StringBuilder("SELECT e FROM DbEntityAudit e WHERE e.entityId = :entityId");

            if (action != null) {
                query.append(" and e.action = :action");
                query.append(" ORDER BY e.eventTime DESC, e.eventIndex DESC");
                return em.createQuery(query.toString(), DbEntityAudit.class)
                        .setParameter("entityId", entityId)
                        .setParameter("action", action)
                        .getResultList();
            } else {
                if (CollectionUtils.isNotEmpty(filterActions)) {
                    query.append(" and e.action NOT IN (");
                    for (int i = 0; i < filterActions.size(); i++) {
                        query.append("'").append(filterActions.get(i)).append("'");
                        if (i != filterActions.size() - 1) {
                            query.append(", ");
                        }
                    }
                    query.append(")");
                }
                query.append(" ORDER BY e.eventTime DESC, e.eventIndex DESC");
                return em.createQuery(query.toString(), DbEntityAudit.class)
                        .setParameter("entityId", entityId)
                        .getResultList();
            }
        } catch (NoResultException excp) {
            // ignore
        }

        return Collections.emptyList();
    }
}
