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
package org.apache.atlas.repository.audit;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.model.audit.AuditReductionCriteria;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.atlas.repository.Constants.AUDIT_AGING_ACTION_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_COUNT_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_ENTITY_TYPES_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_SUBTYPES_INCLUDED_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TTL_KEY;
import static org.apache.atlas.repository.Constants.AUDIT_AGING_TYPE_KEY;
import static org.apache.atlas.repository.Constants.CREATE_EVENTS_AGEOUT_ALLOWED_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.AuditReductionTaskFactory.ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL;

@Component
public class AtlasAuditReductionService implements SchedulingConfigurer {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuditReductionService.class);

    private static final String VALUE_DELIMITER                           = ",";
    private static final String ATLAS_AUDIT_SWEEP_OUT_ENTITY_TYPES        = "atlas.audit.sweep.out.entity.types";
    private static final String ATLAS_AUDIT_SWEEP_OUT_ACTION_TYPES        = "atlas.audit.sweep.out.action.types";
    private static final String ATLAS_AUDIT_CUSTOM_AGEOUT_ENTITY_TYPES    = "atlas.audit.custom.ageout.entity.types";
    private static final String ATLAS_AUDIT_CUSTOM_AGEOUT_ACTION_TYPES    = "atlas.audit.custom.ageout.action.types";
    private static final String ATLAS_AUDIT_AGING_SCHEDULER_INITIAL_DELAY = "atlas.audit.aging.scheduler.initial.delay.in.min";
    private static final int    MIN_TTL_TO_MAINTAIN                       = AtlasConfiguration.MIN_TTL_TO_MAINTAIN.getInt();
    private static final int    MIN_AUDIT_COUNT_TO_MAINTAIN               = AtlasConfiguration.MIN_AUDIT_COUNT_TO_MAINTAIN.getInt();

    private final AtlasGraph            graph;
    private final AtlasDiscoveryService discoveryService;
    private final AtlasTypeRegistry     typeRegistry;
    private final Configuration         atlasConfiguration;

    private AuditReductionCriteria    ageoutCriteriaByConfig;
    private List<Map<String, Object>> ageoutTypeCriteriaMap;

    @Inject
    public AtlasAuditReductionService(Configuration config, AtlasGraph graph, AtlasDiscoveryService discoveryService, AtlasTypeRegistry typeRegistry) {
        this.atlasConfiguration = config;
        this.graph              = graph;
        this.discoveryService   = discoveryService;
        this.typeRegistry       = typeRegistry;
    }

    public List<AtlasTask> startAuditAgingByConfig() {
        List<AtlasTask> tasks = null;

        try {
            if (ageoutCriteriaByConfig == null) {
                ageoutCriteriaByConfig = convertConfigToAuditReductionCriteria();

                LOG.info("Audit aging is enabled by configuration");
            }

            LOG.info("Audit aging is triggered with configuration: {}", ageoutCriteriaByConfig.toString());

            if (ageoutTypeCriteriaMap == null) {
                ageoutTypeCriteriaMap = buildAgeoutCriteriaForAllAgingTypes(ageoutCriteriaByConfig);
            }

            tasks = startAuditAgingByCriteria(ageoutTypeCriteriaMap);
        } catch (Exception e) {
            LOG.error("Error while aging out audits by configuration: ", e);
        }

        return tasks;
    }

    public List<AtlasTask> startAuditAgingByCriteria(List<Map<String, Object>> ageoutTypeCriteriaMap) {
        if (CollectionUtils.isEmpty(ageoutTypeCriteriaMap)) {
            return null;
        }

        List<AtlasTask> tasks = new ArrayList<>();

        try {
            for (Map<String, Object> eachCriteria : ageoutTypeCriteriaMap) {
                AtlasTask auditAgingTask = discoveryService.createAndQueueAuditReductionTask(eachCriteria, ATLAS_AUDIT_REDUCTION_ENTITY_RETRIEVAL);

                if (auditAgingTask != null) {
                    tasks.add(auditAgingTask);
                }
            }
        } catch (Exception e) {
            LOG.error("Error while aging out audits by criteria: ", e);
        }

        return tasks;
    }

    public List<Map<String, Object>> buildAgeoutCriteriaForAllAgingTypes(AuditReductionCriteria auditReductionCriteria) {
        if (auditReductionCriteria == null || !auditReductionCriteria.isAuditAgingEnabled()) {
            return null;
        }

        List<Map<String, Object>> auditAgeoutCriteriaByType = new ArrayList<>();
        Set<String>               defaultAgeoutActionTypes = Arrays.stream(EntityAuditEventV2.EntityAuditActionV2.values()).map(Enum::toString).collect(Collectors.toSet());

        boolean createEventsAgeoutAllowed = auditReductionCriteria.isCreateEventsAgeoutAllowed();
        boolean subTypesIncluded          = auditReductionCriteria.isSubTypesIncluded();
        boolean ignoreDefaultAgeoutTTL    = auditReductionCriteria.ignoreDefaultAgeoutTTL();

        boolean     defaultAgeoutEnabled    = auditReductionCriteria.isDefaultAgeoutEnabled();
        int         defaultAgeoutTTL        = ignoreDefaultAgeoutTTL ? 0 : getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_TTL, auditReductionCriteria.getDefaultAgeoutTTLInDays(), MIN_TTL_TO_MAINTAIN);
        int         defaultAgeoutAuditCount = auditReductionCriteria.getDefaultAgeoutAuditCount() <= 0 ? auditReductionCriteria.getDefaultAgeoutAuditCount() : getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_COUNT, auditReductionCriteria.getDefaultAgeoutAuditCount(), MIN_AUDIT_COUNT_TO_MAINTAIN);
        int         customAgeoutTTL         = auditReductionCriteria.getCustomAgeoutTTLInDays() <= 0 ? auditReductionCriteria.getCustomAgeoutTTLInDays() : getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_CUSTOM_AGEOUT_TTL, auditReductionCriteria.getCustomAgeoutTTLInDays(), MIN_TTL_TO_MAINTAIN);
        int         customAgeoutAuditCount  = auditReductionCriteria.getCustomAgeoutAuditCount() <= 0 ? auditReductionCriteria.getCustomAgeoutAuditCount() : getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_CUSTOM_AGEOUT_COUNT, auditReductionCriteria.getCustomAgeoutAuditCount(), MIN_AUDIT_COUNT_TO_MAINTAIN);
        Set<String> customAgeoutEntityTypes = getUniqueListOf(auditReductionCriteria.getCustomAgeoutEntityTypes());
        Set<String> customAgeoutActionTypes = getValidActionTypes(AtlasAuditAgingType.CUSTOM, getUniqueListOf(auditReductionCriteria.getCustomAgeoutActionTypes()));

        Set<String> defaultAgeoutEntityTypesToExclude = new HashSet<>(customAgeoutEntityTypes);

        if (CollectionUtils.isEmpty(customAgeoutEntityTypes)) {
            defaultAgeoutActionTypes.removeAll(customAgeoutActionTypes);
        }

        boolean isSweepOutEnabled = auditReductionCriteria.isAuditSweepoutEnabled();

        if (isSweepOutEnabled) {
            Set<String> sweepOutEntityTypes = getUniqueListOf(auditReductionCriteria.getSweepoutEntityTypes());
            Set<String> sweepOutActionTypes = getValidActionTypes(AtlasAuditAgingType.SWEEP, getUniqueListOf(auditReductionCriteria.getSweepoutActionTypes()));

            if (CollectionUtils.isNotEmpty(sweepOutEntityTypes) || CollectionUtils.isNotEmpty(sweepOutActionTypes)) {
                Map<String, Object> sweepAgeoutCriteria = getAgeoutCriteriaMap(AtlasAuditAgingType.SWEEP, 0, 0, sweepOutEntityTypes, sweepOutActionTypes, createEventsAgeoutAllowed, subTypesIncluded);

                auditAgeoutCriteriaByType.add(sweepAgeoutCriteria);
            } else {
                LOG.error("Sweepout of audits is skipped.At least one of two properties: entity types/action types should be configured.");
            }

            defaultAgeoutEntityTypesToExclude.addAll(sweepOutEntityTypes);
            customAgeoutEntityTypes.removeAll(sweepOutEntityTypes);

            if (CollectionUtils.isEmpty(sweepOutEntityTypes)) {
                defaultAgeoutActionTypes.removeAll(sweepOutActionTypes);
                customAgeoutActionTypes.removeAll(sweepOutActionTypes);
            }
        }

        if ((customAgeoutTTL > 0 || customAgeoutAuditCount > 0) && (CollectionUtils.isNotEmpty(customAgeoutEntityTypes) || CollectionUtils.isNotEmpty(customAgeoutActionTypes))) {
            Map<String, Object> customAgeoutCriteria = getAgeoutCriteriaMap(AtlasAuditAgingType.CUSTOM, customAgeoutTTL, customAgeoutAuditCount, customAgeoutEntityTypes, customAgeoutActionTypes, createEventsAgeoutAllowed, subTypesIncluded);

            auditAgeoutCriteriaByType.add(customAgeoutCriteria);
        } else if (customAgeoutTTL <= 0 && customAgeoutAuditCount <= 0 && CollectionUtils.isEmpty(customAgeoutEntityTypes) && CollectionUtils.isEmpty(customAgeoutActionTypes)) {
            //Do Nothing
        } else if (customAgeoutTTL <= 0 && customAgeoutAuditCount <= 0) {
            LOG.error("Custom Audit aging is skipped.At least one of two properties: TTL/Audit Count should be configured.");
        } else {
            LOG.error("Custom Audit aging is skipped.At least one of two properties: entity types/action types should be configured.");
        }

        if (defaultAgeoutEnabled) {
            if (ignoreDefaultAgeoutTTL) {
                LOG.info("'{}' config property or 'ignoreDefaultAgeoutTTL' property in API configured as: {}, Default audit aging will be done by audit count only", AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_IGNORE_TTL.getPropertyName(), ignoreDefaultAgeoutTTL);
            }

            /**In case of default ageout with all available audit actions, query to ATLAS_ENTITY_AUDIT_EVENTS table
             * without any action type provides data for all audit actions and is more performant than
             * multiple queries to ATLAS_ENTITY_AUDIT_EVENTS with each action type
             */
            if (defaultAgeoutActionTypes.size() == EntityAuditEventV2.EntityAuditActionV2.values().length) {
                defaultAgeoutActionTypes.clear();
            }

            if (!ignoreDefaultAgeoutTTL || defaultAgeoutAuditCount > 0) {
                Map<String, Object> defaultAgeoutCriteria = getAgeoutCriteriaMap(AtlasAuditAgingType.DEFAULT, defaultAgeoutTTL, defaultAgeoutAuditCount, defaultAgeoutEntityTypesToExclude, defaultAgeoutActionTypes, createEventsAgeoutAllowed, subTypesIncluded);

                auditAgeoutCriteriaByType.add(defaultAgeoutCriteria);
            } else {
                LOG.error("Default Audit aging is skipped. Valid audit count should be configured when TTL criteria is ignored.");
            }
        }

        return auditAgeoutCriteriaByType;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        if (!AtlasConfiguration.ATLAS_AUDIT_AGING_ENABLED.getBoolean()) {
            LOG.warn("Audit aging is not enabled");

            return;
        }

        IntervalTask task = new IntervalTask(this::startAuditAgingByConfig, getAuditAgingFrequencyInMillis(), getAuditAgingInitialDelayInMillis());

        taskRegistrar.addFixedRateTask(task);
    }

    private AuditReductionCriteria convertConfigToAuditReductionCriteria() {
        boolean auditAgingEnabled         = AtlasConfiguration.ATLAS_AUDIT_AGING_ENABLED.getBoolean();
        boolean createAuditsAgeoutAllowed = AtlasConfiguration.ATLAS_AUDIT_CREATE_EVENTS_AGEOUT_ALLOWED.getBoolean();
        boolean subTypesIncluded          = AtlasConfiguration.ATLAS_AUDIT_AGING_SUBTYPES_INCLUDED.getBoolean();
        boolean ignoreDefaultAgeoutTTL    = AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_IGNORE_TTL.getBoolean();

        int defaultAgeoutTTLConfigured        = AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_TTL.getInt();
        int defaultAgeoutAuditCountConfigured = AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_COUNT.getInt();

        int customAgeoutTTLConfigured        = AtlasConfiguration.ATLAS_AUDIT_CUSTOM_AGEOUT_TTL.getInt();
        int customAgeoutAuditCountConfigured = AtlasConfiguration.ATLAS_AUDIT_CUSTOM_AGEOUT_COUNT.getInt();

        boolean defaultAgeoutEnabled    = AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_ENABLED.getBoolean();
        int     defaultAgeoutTTL        = getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_TTL, defaultAgeoutTTLConfigured, MIN_TTL_TO_MAINTAIN);
        int     defaultAgeoutAuditCount = defaultAgeoutAuditCountConfigured <= 0 ? defaultAgeoutAuditCountConfigured : getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_DEFAULT_AGEOUT_COUNT, defaultAgeoutAuditCountConfigured, MIN_AUDIT_COUNT_TO_MAINTAIN);
        int     customAgeoutTTL         = customAgeoutTTLConfigured <= 0 ? customAgeoutTTLConfigured : getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_CUSTOM_AGEOUT_TTL, customAgeoutTTLConfigured, MIN_TTL_TO_MAINTAIN);
        int     customAgeoutAuditCount  = customAgeoutAuditCountConfigured <= 0 ? customAgeoutAuditCountConfigured : getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_CUSTOM_AGEOUT_COUNT, customAgeoutAuditCountConfigured, MIN_AUDIT_COUNT_TO_MAINTAIN);

        String customAgeoutEntityTypes = getStringOf(ATLAS_AUDIT_CUSTOM_AGEOUT_ENTITY_TYPES);
        String customAgeoutActionTypes = getStringOf(ATLAS_AUDIT_CUSTOM_AGEOUT_ACTION_TYPES);

        AuditReductionCriteria auditReductionCriteria = new AuditReductionCriteria();

        auditReductionCriteria.setAuditAgingEnabled(auditAgingEnabled);
        auditReductionCriteria.setCreateEventsAgeoutAllowed(createAuditsAgeoutAllowed);
        auditReductionCriteria.setSubTypesIncluded(subTypesIncluded);
        auditReductionCriteria.setIgnoreDefaultAgeoutTTL(ignoreDefaultAgeoutTTL);

        auditReductionCriteria.setDefaultAgeoutEnabled(defaultAgeoutEnabled);
        auditReductionCriteria.setDefaultAgeoutTTLInDays(defaultAgeoutTTL);
        auditReductionCriteria.setDefaultAgeoutAuditCount(defaultAgeoutAuditCount);

        auditReductionCriteria.setCustomAgeoutTTLInDays(customAgeoutTTL);
        auditReductionCriteria.setCustomAgeoutAuditCount(customAgeoutAuditCount);
        auditReductionCriteria.setCustomAgeoutEntityTypes(customAgeoutEntityTypes);
        auditReductionCriteria.setCustomAgeoutActionTypes(customAgeoutActionTypes);

        boolean isSweepOutEnabled = AtlasConfiguration.ATLAS_AUDIT_SWEEP_OUT.getBoolean();

        auditReductionCriteria.setAuditSweepoutEnabled(isSweepOutEnabled);

        if (isSweepOutEnabled) {
            String sweepoutEntityTypes = getStringOf(ATLAS_AUDIT_SWEEP_OUT_ENTITY_TYPES);
            String sweepoutActionTypes = getStringOf(ATLAS_AUDIT_SWEEP_OUT_ACTION_TYPES);

            auditReductionCriteria.setSweepoutEntityTypes(sweepoutEntityTypes);
            auditReductionCriteria.setSweepoutActionTypes(sweepoutActionTypes);
        }

        return auditReductionCriteria;
    }

    private Map<String, Object> getAgeoutCriteriaMap(AtlasAuditAgingType agingOption, int ttl, int minCount, Set<String> entityTypes, Set<String> actionTypes, boolean createEventsAgeoutAllowed, boolean subTypesIncluded) {
        Map<String, Object> auditAgingOptions = new HashMap<>();

        auditAgingOptions.put(AUDIT_AGING_TYPE_KEY, agingOption);
        auditAgingOptions.put(AUDIT_AGING_TTL_KEY, ttl);
        auditAgingOptions.put(AUDIT_AGING_COUNT_KEY, minCount);
        auditAgingOptions.put(AUDIT_AGING_ENTITY_TYPES_KEY, entityTypes);
        auditAgingOptions.put(AUDIT_AGING_ACTION_TYPES_KEY, actionTypes);
        auditAgingOptions.put(CREATE_EVENTS_AGEOUT_ALLOWED_KEY, createEventsAgeoutAllowed);
        auditAgingOptions.put(AUDIT_AGING_SUBTYPES_INCLUDED_KEY, subTypesIncluded);

        return auditAgingOptions;
    }

    private int getGuaranteedMinValueOf(AtlasConfiguration configuration, int configuredValue, int minValueToMaintain) {
        if (configuredValue < minValueToMaintain) {
            LOG.info("Minimum value for '{}' should be {}", configuration.getPropertyName(), minValueToMaintain);
        }

        return configuredValue < minValueToMaintain ? minValueToMaintain : configuredValue;
    }

    private String getStringOf(String configProperty) {
        String configuredValue = null;

        if (StringUtils.isNotEmpty(configProperty)) {
            configuredValue = String.join(VALUE_DELIMITER, (List) atlasConfiguration.getList(configProperty));
        }

        return configuredValue;
    }

    private Set<String> getUniqueListOf(String value) {
        Set<String> configuredValues = null;

        if (StringUtils.isNotEmpty(value)) {
            configuredValues = Stream.of(value.split(VALUE_DELIMITER)).map(String::trim).collect(Collectors.toSet());
        }

        return configuredValues == null ? new HashSet<>() : configuredValues;
    }

    private Set<String> getValidActionTypes(AtlasAuditAgingType auditAgingType, Set<String> actionTypes) {
        if (CollectionUtils.isEmpty(actionTypes)) {
            return Collections.emptySet();
        }

        Set<String> allActionTypes     = Arrays.stream(EntityAuditEventV2.EntityAuditActionV2.values()).map(Enum::toString).collect(Collectors.toSet());
        Set<String> entityAuditActions = new HashSet<>();
        Set<String> invalidActionTypes = new HashSet<>();

        for (String actionType : actionTypes) {
            Set<String>  matchedActionTypes;
            final String actionTypeToMatch = actionType.contains("*") ? actionType.replace("*", "") : actionType;

            if (actionTypeToMatch.startsWith("*")) {
                matchedActionTypes = allActionTypes.stream().filter(x -> x.contains(actionTypeToMatch)).collect(Collectors.toSet());
            } else {
                matchedActionTypes = allActionTypes.stream().filter(x -> x.startsWith(actionTypeToMatch)).collect(Collectors.toSet());
            }

            if (CollectionUtils.isEmpty(matchedActionTypes)) {
                invalidActionTypes.add(actionType);
            } else {
                entityAuditActions.addAll(matchedActionTypes);
            }
        }

        if (CollectionUtils.isNotEmpty(actionTypes) && CollectionUtils.isEmpty(entityAuditActions)) {
            throw new IllegalArgumentException("No enum constant " + EntityAuditEventV2.EntityAuditActionV2.class.getCanonicalName() + "." + String.join(VALUE_DELIMITER, invalidActionTypes));
        } else {
            LOG.info("Action type name(s) {} provided for aging type-{}", String.join(VALUE_DELIMITER, entityAuditActions), auditAgingType);
        }

        if (CollectionUtils.isNotEmpty(invalidActionTypes)) {
            LOG.warn("Invalid action type name(s) {} provided for aging type-{}", String.join(VALUE_DELIMITER, invalidActionTypes), auditAgingType);
        }

        return entityAuditActions;
    }

    private long getAuditAgingFrequencyInMillis() {
        int frequencyInDays = getGuaranteedMinValueOf(AtlasConfiguration.ATLAS_AUDIT_AGING_SCHEDULER_FREQUENCY, AtlasConfiguration.ATLAS_AUDIT_AGING_SCHEDULER_FREQUENCY.getInt(), 1);

        return frequencyInDays * DateUtils.MILLIS_PER_DAY;
    }

    private long getAuditAgingInitialDelayInMillis() {
        int initialDelayInMins = 1;

        try {
            initialDelayInMins = ApplicationProperties.get().getInt(ATLAS_AUDIT_AGING_SCHEDULER_INITIAL_DELAY, 1);
        } catch (AtlasException ex) {
            LOG.error("Error while fetching application properties", ex);
        }

        return (initialDelayInMins < 1 ? 1 : initialDelayInMins) * DateUtils.MILLIS_PER_MINUTE;
    }
}
