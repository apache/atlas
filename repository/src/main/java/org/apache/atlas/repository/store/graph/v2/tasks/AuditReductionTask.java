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
package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_ACTION_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_COUNT_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TTL_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TYPE_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_REDUCTION_TYPE_NAME;
import static org.apache.atlas.repository.Constants.CREATE_EVENTS_AGEOUT_ALLOWED_KEY;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_AUDIT_REDUCTION_NAME;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.AGING_TYPE_PROPERTY_KEY_MAP;

public class AuditReductionTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(AuditReductionTask.class);

    private static final int GUID_BATCH_SIZE_PER_AGE_OUT_TASK = 100;

    private final EntityAuditRepository auditRepository;
    private final AtlasGraph            graph;

    public AuditReductionTask(AtlasTask task, EntityAuditRepository auditRepository, AtlasGraph graph) {
        super(task);

        this.auditRepository = auditRepository;
        this.graph           = graph;
    }

    @Override
    public AtlasTask.Status perform() throws Exception {
        RequestContext.clear();

        Map<String, Object> params = getTaskDef().getParameters();

        if (MapUtils.isEmpty(params)) {
            LOG.warn("Task: {}: Unable to process task: Parameters is not readable!", getTaskGuid());

            return FAILED;
        }

        String userName = getTaskDef().getCreatedBy();

        if (StringUtils.isEmpty(userName)) {
            LOG.warn("Task: {}: Unable to process task as user name is empty!", getTaskGuid());

            return FAILED;
        }

        RequestContext.get().setUser(userName, null);

        try {
            run(params);

            setStatus(COMPLETE);
        } catch (Exception e) {
            LOG.error("Task: {}: Error performing task!", getTaskGuid(), e);

            setStatus(FAILED);

            throw e;
        } finally {
            RequestContext.clear();
        }

        return getStatus();
    }

    public AtlasVertex findVertex() {
        AtlasGraphQuery       query   = graph.query().has(PROPERTY_KEY_AUDIT_REDUCTION_NAME, AUDIT_REDUCTION_TYPE_NAME);
        Iterator<AtlasVertex> results = query.vertices().iterator();

        return results.hasNext() ? results.next() : null;
    }

    protected void run(Map<String, Object> parameters) throws AtlasBaseException, IOException, AtlasException {
        AtlasVertex vertex = findVertex();

        if (vertex == null) {
            return;
        }

        Map<String, List<EntityAuditEventV2>> entitiesWithSucceededAgeout = new HashMap<>();

        AtlasAuditAgingType auditAgingType            = AtlasAuditAgingType.valueOf(String.valueOf(parameters.get(AUDIT_AGING_TYPE_KEY)));
        Set<String>         actionTypes               = new HashSet<>(((Collection<String>) parameters.get(AUDIT_AGING_ACTION_TYPES_KEY)));
        int                 auditCountInput           = (int) parameters.get(AUDIT_AGING_COUNT_KEY);
        short               auditCount                = auditCountInput > Short.MAX_VALUE ? Short.MAX_VALUE : auditCountInput < Short.MIN_VALUE ? Short.MIN_VALUE : (short) auditCountInput;
        int                 ttl                       = (int) parameters.get(AUDIT_AGING_TTL_KEY);
        boolean             createEventsAgeoutAllowed = (boolean) parameters.get(CREATE_EVENTS_AGEOUT_ALLOWED_KEY);
        String              vertexPropertyKeyForGuids = AGING_TYPE_PROPERTY_KEY_MAP.get(auditAgingType);

        List<String> entityGuidsEligibleForAuditAgeout = vertex.getProperty(vertexPropertyKeyForGuids, List.class);
        int          guidsCount                        = CollectionUtils.isNotEmpty(entityGuidsEligibleForAuditAgeout) ? entityGuidsEligibleForAuditAgeout.size() : 0;
        int          batchIndex                        = 1;

        Set<EntityAuditEventV2.EntityAuditActionV2> entityAuditActions = actionTypes.stream().map(EntityAuditEventV2.EntityAuditActionV2::fromString).collect(Collectors.toSet());

        for (int startIndex = 0; startIndex < guidsCount; ) {
            int          endIndex   = startIndex + GUID_BATCH_SIZE_PER_AGE_OUT_TASK < guidsCount ? startIndex + GUID_BATCH_SIZE_PER_AGE_OUT_TASK : guidsCount;
            List<String> guidsBatch = entityGuidsEligibleForAuditAgeout.subList(startIndex, endIndex);

            for (String guid : guidsBatch) {
                List<EntityAuditEventV2> deletedAuditEvents = auditRepository.deleteEventsV2(guid, entityAuditActions, auditCount, ttl, createEventsAgeoutAllowed, auditAgingType);

                entitiesWithSucceededAgeout.put(guid, deletedAuditEvents);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("{} Audit aging completed for batch-{} with guids: {}", auditAgingType, batchIndex, Arrays.toString(entitiesWithSucceededAgeout.keySet().toArray()));
            }

            entitiesWithSucceededAgeout.clear();

            startIndex = endIndex;

            batchIndex++;

            List<String> remainingGuids = startIndex < guidsCount ? new ArrayList<>(entityGuidsEligibleForAuditAgeout.subList(startIndex, guidsCount)) : null;

            vertex.setProperty(vertexPropertyKeyForGuids, remainingGuids);
        }
    }
}
