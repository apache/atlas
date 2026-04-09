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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.tasks.TaskFactory;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_CUSTOM;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_GUIDS_TO_SWEEPOUT;

@Component
public class AuditReductionTaskFactory implements TaskFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AuditReductionTaskFactory.class);

    public static final  int    MAX_PENDING_TASKS_ALLOWED;
    public static final  String ATLAS_AUDIT_REDUCTION                  = "ATLAS_AUDIT_REDUCTION";
    public static final  String ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL = "AUDIT_REDUCTION_ENTITY_RETRIEVAL";

    public static final Map<AtlasAuditAgingType, String> AGING_TYPE_PROPERTY_KEY_MAP = new HashMap<>();

    private static final int           MAX_PENDING_TASKS_ALLOWED_DEFAULT = 50;
    private static final String        MAX_PENDING_TASKS_ALLOWED_KEY     = "atlas.audit.reduction.max.pending.tasks";
    private static final List<String>  SUPPORTED_TYPES                   = new ArrayList<>(Arrays.asList(ATLAS_AUDIT_REDUCTION, ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL));

    private final EntityAuditRepository auditRepository;
    private final AtlasGraph            graph;
    private final AtlasDiscoveryService discoveryService;
    private final AtlasTypeRegistry     typeRegistry;

    @Inject
    public AuditReductionTaskFactory(EntityAuditRepository auditRepository, AtlasGraph graph, AtlasDiscoveryService discoveryService, AtlasTypeRegistry typeRegistry) {
        this.auditRepository  = auditRepository;
        this.graph            = graph;
        this.discoveryService = discoveryService;
        this.typeRegistry     = typeRegistry;
    }

    @Override
    public AbstractTask create(AtlasTask task) {
        String taskType = task.getType();
        String taskGuid = task.getGuid();

        switch (taskType) {
            case ATLAS_AUDIT_REDUCTION:
                return new AuditReductionTask(task, auditRepository, graph);

            case ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL:
                return new AuditReductionEntityRetrievalTask(task, graph, discoveryService, typeRegistry);

            default:
                LOG.warn("Type: {} - {} not found!. The task will be ignored.", taskType, taskGuid);
                return null;
        }
    }

    @Override
    public List<String> getSupportedTypes() {
        return SUPPORTED_TYPES;
    }

    static {
        Configuration configuration = null;

        try {
            configuration = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }

        if (configuration != null) {
            MAX_PENDING_TASKS_ALLOWED = configuration.getInt(MAX_PENDING_TASKS_ALLOWED_KEY, MAX_PENDING_TASKS_ALLOWED_DEFAULT);
        } else {
            MAX_PENDING_TASKS_ALLOWED = MAX_PENDING_TASKS_ALLOWED_DEFAULT;
        }

        AGING_TYPE_PROPERTY_KEY_MAP.put(AtlasAuditAgingType.DEFAULT, PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_DEFAULT);
        AGING_TYPE_PROPERTY_KEY_MAP.put(AtlasAuditAgingType.SWEEP, PROPERTY_KEY_GUIDS_TO_SWEEPOUT);
        AGING_TYPE_PROPERTY_KEY_MAP.put(AtlasAuditAgingType.CUSTOM, PROPERTY_KEY_GUIDS_TO_AGEOUT_BY_CUSTOM);
    }
}
