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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_ENTITY_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_EXCLUDE_ENTITY_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_SUBTYPES_INCLUDED_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TYPE_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_REDUCTION_TYPE_NAME;
import static org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_AUDIT_REDUCTION_NAME;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.AGING_TYPE_PROPERTY_KEY_MAP;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.ATLAS_AUDIT_REDUCTION;

public class AuditReductionEntityRetrievalTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(AuditReductionEntityRetrievalTask.class);

    private static final String VALUE_DELIMITER  = ",";
    private static final String ALL_ENTITY_TYPES = "_ALL_ENTITY_TYPES";
    private static final int    SEARCH_OFFSET    = 0;
    private static final int    SEARCH_LIMIT     = AtlasConfiguration.ATLAS_AUDIT_AGING_SEARCH_MAX_LIMIT.getInt();

    private final AtlasDiscoveryService discoveryService;
    private final AtlasTypeRegistry     typeRegistry;
    private final AtlasGraph            graph;

    public AuditReductionEntityRetrievalTask(AtlasTask task, AtlasGraph graph, AtlasDiscoveryService discoveryService, AtlasTypeRegistry typeRegistry) {
        super(task);
        this.graph            = graph;
        this.discoveryService = discoveryService;
        this.typeRegistry     = typeRegistry;
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

    protected void run(Map<String, Object> parameters) throws AtlasBaseException, IOException, AtlasException {
        try {
            AtlasTask auditAgingTask = createAgingTaskWithEligibleGUIDs(parameters);

            if (auditAgingTask != null) {
                LOG.info("{} task created for audit aging type-{}", ATLAS_AUDIT_REDUCTION, parameters.get(AUDIT_AGING_TYPE_KEY));
            }
        } catch (Exception e) {
            LOG.error("Error while retrieving entities eligible for audit aging and creating audit aging tasks", e);
        }
    }

    protected AtlasTask createAgingTaskWithEligibleGUIDs(Map<String, Object> parameters) throws AtlasBaseException {
        Set<String>         entityTypes      = new HashSet<>(((Collection<String>) parameters.get(AUDIT_AGING_ENTITY_TYPES_KEY)));
        AtlasAuditAgingType auditAgingType   = (AtlasAuditAgingType) parameters.get(AUDIT_AGING_TYPE_KEY);
        boolean             subTypesIncluded = (boolean) parameters.get(AUDIT_AGING_SUBTYPES_INCLUDED_KEY);

        SearchParameters searchEntitiesToReduceAudit = new SearchParameters();

        searchEntitiesToReduceAudit.setTypeName(ALL_ENTITY_TYPES);
        searchEntitiesToReduceAudit.setOffset(SEARCH_OFFSET);
        searchEntitiesToReduceAudit.setLimit(SEARCH_LIMIT);
        searchEntitiesToReduceAudit.setIncludeSubTypes(subTypesIncluded);

        if (CollectionUtils.isNotEmpty(entityTypes)) {
            if (!validateTypesAndIncludeSubTypes(entityTypes, auditAgingType, subTypesIncluded)) {
                LOG.error("All entity type names provided for audit aging type-{} are invalid", auditAgingType);

                return null;
            }

            String queryString = String.join(VALUE_DELIMITER, entityTypes);

            if (auditAgingType == AtlasAuditAgingType.DEFAULT && StringUtils.isNotEmpty(queryString)) {
                queryString = "!" + queryString;
            }

            searchEntitiesToReduceAudit.setQuery(queryString);
        }

        LOG.info("Getting GUIDs eligible for Audit aging type-{} with SearchParameters: {}", auditAgingType, searchEntitiesToReduceAudit);

        Set<String> guids = discoveryService.searchGUIDsWithParameters(auditAgingType, entityTypes, searchEntitiesToReduceAudit);

        AtlasVertex auditReductionVertex = getOrCreateVertex();

        AtlasTask ageoutTask = updateVertexWithGuidsAndCreateAgingTask(auditReductionVertex, AGING_TYPE_PROPERTY_KEY_MAP.get(auditAgingType), guids, parameters);

        /** For DEFAULT audit aging, "entityTypes" should be excluded from _ALL_ENTITY_TYPES i.e., negating the queryString
         *  Including AUDIT_AGING_EXCLUDE_ENTITY_TYPES_KEY to indicate the same in AtlasTask response to user
         */
        if (ageoutTask != null) {
            if (auditAgingType == AtlasAuditAgingType.DEFAULT && CollectionUtils.isNotEmpty(entityTypes)) {
                ageoutTask.getParameters().put(AUDIT_AGING_EXCLUDE_ENTITY_TYPES_KEY, true);
            } else {
                ageoutTask.getParameters().put(AUDIT_AGING_EXCLUDE_ENTITY_TYPES_KEY, false);
            }
        }

        return ageoutTask;
    }

    private boolean validateTypesAndIncludeSubTypes(Set<String> entityTypes, AtlasAuditAgingType auditAgingType, boolean subTypesIncluded) throws AtlasBaseException {
        Collection<String> allEntityTypeNames     = typeRegistry.getAllEntityDefNames();
        Set<String>        entityTypesToSearch    = new HashSet<>();
        Set<String>        invalidEntityTypeNames = new HashSet<>();

        entityTypes.forEach(entityType -> {
            if (entityType.endsWith("*")) {
                String suffix = entityType.replace("*", "");

                entityTypesToSearch.addAll(allEntityTypeNames.stream().filter(e -> e.startsWith(suffix)).collect(Collectors.toSet()));
            } else if (allEntityTypeNames.contains(entityType)) {
                entityTypesToSearch.add(entityType);
            } else {
                invalidEntityTypeNames.add(entityType);
            }
        });

        if (auditAgingType != AtlasAuditAgingType.DEFAULT) {
            if (CollectionUtils.isNotEmpty(invalidEntityTypeNames)) {
                LOG.warn("Invalid entity type name(s) {} provided for aging type-{}", String.join(VALUE_DELIMITER, invalidEntityTypeNames), auditAgingType);
            }

            if (CollectionUtils.isEmpty(entityTypesToSearch)) {
                return false;
            }
        }

        entityTypes.clear();
        entityTypes.addAll(subTypesIncluded ? AtlasEntityType.getEntityTypesAndAllSubTypes(entityTypesToSearch, typeRegistry) : entityTypesToSearch);

        return true;
    }

    @GraphTransaction
    private AtlasTask updateVertexWithGuidsAndCreateAgingTask(AtlasVertex vertex, String vertexProperty, Set<String> guids, Map<String, Object> params) throws AtlasBaseException {
        List<String> guidsEligibleForAuditReduction = vertex.getProperty(vertexProperty, List.class);

        if (CollectionUtils.isEmpty(guidsEligibleForAuditReduction) && CollectionUtils.isEmpty(guids)) {
            return null;
        }

        if (CollectionUtils.isEmpty(guidsEligibleForAuditReduction)) {
            guidsEligibleForAuditReduction = new ArrayList<>();
        }

        if (CollectionUtils.isNotEmpty(guids)) {
            guidsEligibleForAuditReduction.addAll(guids);
            setEncodedProperty(vertex, vertexProperty, guidsEligibleForAuditReduction);
        }

        return discoveryService.createAndQueueAuditReductionTask(params, ATLAS_AUDIT_REDUCTION);
    }

    private AtlasVertex getOrCreateVertex() {
        AtlasGraphQuery       query                = graph.query().has(PROPERTY_KEY_AUDIT_REDUCTION_NAME, AUDIT_REDUCTION_TYPE_NAME);
        Iterator<AtlasVertex> results              = query.vertices().iterator();
        AtlasVertex           auditReductionVertex = results.hasNext() ? results.next() : null;

        if (auditReductionVertex == null) {
            auditReductionVertex = graph.addVertex();

            setEncodedProperty(auditReductionVertex, PROPERTY_KEY_AUDIT_REDUCTION_NAME, AUDIT_REDUCTION_TYPE_NAME);
        }

        return auditReductionVertex;
    }
}
