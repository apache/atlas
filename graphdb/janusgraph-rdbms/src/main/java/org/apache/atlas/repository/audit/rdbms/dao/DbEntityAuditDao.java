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

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.List;

public class DbEntityAuditDao extends BaseDao<DbEntityAudit> {
    public DbEntityAuditDao(EntityManager em) {
        super(em);
    }

    public List<DbEntityAudit> getByEntityIdActionStartTimeStartIdx(String entityId, int action, long eventTimeStart, int eventIdxStart, int maxResults) {
        try {
            return em.createNamedQuery("DbEntityAudit.getByEntityIdActionStartTimeStartIdx", DbEntityAudit.class)
                    .setParameter("entityId", entityId)
                    .setParameter("action", action)
                    .setParameter("eventTimeStart", eventTimeStart)
                    .setParameter("eventIdxStart", eventIdxStart)
                    .setMaxResults(maxResults)
                    .getResultList();
        } catch (NoResultException excp) {
            // ignore
        }

        return Collections.emptyList();
    }

    public List<DbEntityAudit> getByEntityIdAction(String entityId, Integer action, int startIdx, int maxResults) {
        try {
            if (action == null) {
                return em.createNamedQuery("DbEntityAudit.getByEntityId", DbEntityAudit.class)
                        .setParameter("entityId", entityId)
                        .setFirstResult(startIdx)
                        .setMaxResults(maxResults)
                        .getResultList();
            } else {
                return em.createNamedQuery("DbEntityAudit.getByEntityIdAction", DbEntityAudit.class)
                        .setParameter("entityId", entityId)
                        .setParameter("action", action)
                        .setFirstResult(startIdx)
                        .setMaxResults(maxResults)
                        .getResultList();
            }
        } catch (NoResultException excp) {
            // ignore
        }

        return Collections.emptyList();
    }
}
