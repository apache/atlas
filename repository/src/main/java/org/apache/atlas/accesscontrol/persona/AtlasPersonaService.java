/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.accesscontrol.persona;

import org.apache.atlas.RequestContext;
import org.apache.atlas.accesscontrol.AccessControlUtil;
import org.apache.atlas.accesscontrol.aliasstore.ESAliasStore;
import org.apache.atlas.accesscontrol.persona.callable.CleanRoleWorker;
import org.apache.atlas.accesscontrol.persona.callable.CreateRangerPolicyWorker;
import org.apache.atlas.accesscontrol.persona.callable.UpdateRangerPolicyWorker;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasConfiguration.RANGER_ATLAS_SERVICE_TYPE;
import static org.apache.atlas.AtlasConfiguration.RANGER_HEKA_SERVICE_TYPE;
import static org.apache.atlas.AtlasErrorCode.ATTRIBUTE_UPDATE_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_ENTITY_READ;
import static org.apache.atlas.accesscontrol.AccessControlUtil.POLICY_QN_FORMAT;
import static org.apache.atlas.accesscontrol.AccessControlUtil.RANGER_POLICY_TYPE_ACCESS;
import static org.apache.atlas.accesscontrol.AccessControlUtil.RANGER_POLICY_TYPE_DATA_MASK;
import static org.apache.atlas.accesscontrol.AccessControlUtil.fetchRangerPoliciesByLabel;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getActions;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getAssets;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getDataPolicyMaskType;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getESAliasName;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getIsAllow;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getIsEnabled;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getName;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getPolicies;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getPolicyType;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getQualifiedName;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getTenantId;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getUUID;
import static org.apache.atlas.accesscontrol.AccessControlUtil.isDataPolicy;
import static org.apache.atlas.accesscontrol.AccessControlUtil.submitCallablesAndWaitToFinish;
import static org.apache.atlas.accesscontrol.AccessControlUtil.validateUniquenessByName;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.CLASSIFICATION_ACTIONS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.ENTITY_ACTIONS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.LABEL_ACTIONS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.LABEL_PREFIX_PERSONA;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.LABEL_TYPE_PERSONA;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.TERM_ACTIONS;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getLabelsForPersonaPolicy;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getPersonaLabel;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getPersonaPolicyLabel;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getPersonaRoleId;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getRoleName;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;


public class AtlasPersonaService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPersonaService.class);

    private final AtlasGraph graph;
    private final ESAliasStore aliasStore;
    private final EntityGraphRetriever entityRetriever;

    private static AtlasRangerService atlasRangerService = null;

    public AtlasPersonaService(AtlasGraph graph, EntityGraphRetriever entityRetriever) {

        this.graph = graph;
        this.entityRetriever = entityRetriever;
        this.aliasStore = new ESAliasStore(graph, entityRetriever);

        atlasRangerService = new AtlasRangerService();
    }

    public void createPersona(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPersona");
        LOG.info("Creating Persona");
        AtlasEntityWithExtInfo entityWithExtInfo = context.getPersonaExtInfo();
        context.setCreateNewPersona(true);

        try {
            validateUniquenessByName(graph, getName(entityWithExtInfo.getEntity()), PERSONA_ENTITY_TYPE);

            String tenantId = getTenantId(context.getPersona());
            if (StringUtils.isEmpty(tenantId)) {
                tenantId = "tenant";
            }
            entityWithExtInfo.getEntity().setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
            entityWithExtInfo.getEntity().setAttribute("enabled", true);

            RangerRole rangerRole = atlasRangerService.createRangerRole(context);
            context.getPersona().getAttributes().put("rangerRoleId", rangerRole.getId());

            aliasStore.createAlias(context);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public void updatePersona(PersonaContext context, AtlasEntityWithExtInfo existingPersonaWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePersona");
        LOG.info("Updating Persona");

        AtlasEntity persona = context.getPersona();

        AtlasEntity existingPersonaEntity = existingPersonaWithExtInfo.getEntity();

        try {
            if (!AtlasEntity.Status.ACTIVE.equals(existingPersonaEntity.getStatus())) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Persona not Active");
            }

            if (getPersonaRoleId(persona) != getPersonaRoleId(existingPersonaEntity)) {
                throw new AtlasBaseException(ATTRIBUTE_UPDATE_NOT_SUPPORTED, PERSONA_ENTITY_TYPE, "rangerRoleId");
            }

            if (getIsEnabled(existingPersonaEntity) != getIsEnabled(persona)) {
                if (getIsEnabled(context.getPersona())) {
                    enablePersona(existingPersonaWithExtInfo);
                } else {
                    disablePersona(existingPersonaWithExtInfo);
                }
            }

            if (!getName(persona).equals(getName(existingPersonaEntity))) {
                validateUniquenessByName(graph, getName(persona), PERSONA_ENTITY_TYPE);
            }

            atlasRangerService.updateRangerRole(context);

            aliasStore.updateAlias(context);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public void deletePersona(AtlasEntityWithExtInfo personaExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePersona");
        AtlasEntity persona = personaExtInfo.getEntity();

        try {
            if(!persona.getTypeName().equals(PERSONA_ENTITY_TYPE)) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + PERSONA_ENTITY_TYPE);
            }

            if(!persona.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
                LOG.info("Persona with guid {} is already deleted/purged", persona.getGuid());
                return;
            }

            cleanRoleFromAllRangerPolicies(personaExtInfo);

            atlasRangerService.deleteRangerRole(getPersonaRoleId(persona));

            aliasStore.deleteAlias(getESAliasName(persona));

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void cleanRoleFromAllRangerPolicies(AtlasEntityWithExtInfo personaExtInfo) throws AtlasBaseException {
        AtlasEntity persona = personaExtInfo.getEntity();

        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                null,
                null,
                getPersonaLabel(persona.getGuid()));

        List<String> allPolicyGuids = getPolicies(personaExtInfo).stream()
                .map(x -> getPersonaPolicyLabel(x.getGuid()))
                .collect(Collectors.toList());

        cleanRoleFromExistingPolicies(personaExtInfo.getEntity(), rangerPolicies, allPolicyGuids);
    }

    private void enablePersona(AtlasEntityWithExtInfo existingPersonaWithExtInfo) throws AtlasBaseException {
        List<AtlasEntity> personaPolicies = getPolicies(existingPersonaWithExtInfo);

        for (AtlasEntity personaPolicy : personaPolicies) {
            PersonaContext contextItr = new PersonaContext(existingPersonaWithExtInfo, personaPolicy);
            contextItr.setAllowPolicy(getIsAllow(personaPolicy));
            contextItr.setAllowPolicyUpdate();
            contextItr.setCreateNewPersonaPolicy(true);

            List<RangerPolicy> provisionalRangerPolicies = PersonaServiceHelper.personaPolicyToRangerPolicies(contextItr, getActions(personaPolicy));

            createPersonaPolicy(contextItr, provisionalRangerPolicies);
        }
    }

    private void disablePersona(AtlasEntityWithExtInfo existingPersonaWithExtInfo) throws AtlasBaseException {
        cleanRoleFromAllRangerPolicies(existingPersonaWithExtInfo);
    }

    public void createPersonaPolicy(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPersonaPolicy");

        AtlasEntityWithExtInfo personaWithExtInfo = context.getPersonaExtInfo();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        context.setCreateNewPersonaPolicy(true);
        context.setAllowPolicy(getIsAllow(personaPolicy));
        context.setAllowPolicyUpdate();

        personaPolicy.setAttribute(QUALIFIED_NAME, String.format(POLICY_QN_FORMAT, getQualifiedName(personaWithExtInfo.getEntity()), getUUID()));

        try {
            PersonaServiceHelper.validatePersonaPolicy(context, entityRetriever, atlasRangerService);

            List<RangerPolicy> provisionalRangerPolicies = PersonaServiceHelper.personaPolicyToRangerPolicies(context, getActions(context.getPersonaPolicy()));

            if (CollectionUtils.isNotEmpty(provisionalRangerPolicies)) {
                createPersonaPolicy(context, provisionalRangerPolicies);
            } else {
                throw new AtlasBaseException("provisionalRangerPolicies could not be empty");
            }

            if (context.isMetadataPolicy() || context.isGlossaryPolicy()) {
                aliasStore.updateAlias(context);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public void updatePersonaPolicy(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePersonaPolicy");
        Map<RangerPolicy, RangerPolicy> provisionalToRangerPoliciesMap = new HashMap<>();

        AtlasEntity personaPolicy = context.getPersonaPolicy();

        context.setAllowPolicy(getIsAllow(personaPolicy));
        context.setAllowPolicyUpdate();

        try {
            validatePersonaPolicyUpdate(context);

            List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                    context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                    null,
                    getPersonaPolicyLabel(personaPolicy.getGuid()));

            List<RangerPolicy> provisionalRangerPolicies = PersonaServiceHelper.personaPolicyToRangerPolicies(context, getActions(context.getPersonaPolicy()));

            if (context.isUpdateIsAllow() || isAssetUpdate(context) || isDataPolicyTypeUpdate(context)) {
                //remove role from existing policies & create new Ranger policies
                List<String> removePolicyGuids = Collections.singletonList(getPersonaPolicyLabel(personaPolicy.getGuid()));
                cleanRoleFromExistingPolicies(context.getPersona(), rangerPolicies, removePolicyGuids);

                for (RangerPolicy provisionalRangerPolicy : provisionalRangerPolicies) {
                    provisionalToRangerPoliciesMap.put(provisionalRangerPolicy, null);
                }

            } else {
                provisionalToRangerPoliciesMap = mapPolicies(context, provisionalRangerPolicies, rangerPolicies);
            }

            if (MapUtils.isNotEmpty(provisionalToRangerPoliciesMap)) {
                processUpdatePolicies(context, provisionalToRangerPoliciesMap);
            }

            processActionsRemoval(context);

            if (context.isMetadataPolicy() || context.isGlossaryPolicy()) {
                aliasStore.updateAlias(context);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }


    private void validatePersonaPolicyUpdate(PersonaContext context) throws AtlasBaseException {
        PersonaServiceHelper.validatePersonaPolicy(context, entityRetriever, atlasRangerService);

        if (!getPolicyType(context.getPersonaPolicy()).equals(getPolicyType(context.getExistingPersonaPolicy()))) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy type change not Allowed");
        }

        if (!AtlasEntity.Status.ACTIVE.equals(context.getExistingPersonaPolicy().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Entity not Active");
        }
    }

    private List<RangerPolicy> createPersonaPolicy(PersonaContext context, List<RangerPolicy> provisionalRangerPolicies) throws AtlasBaseException {
        List<RangerPolicy> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(provisionalRangerPolicies)) {
            submitCallablesAndWaitToFinish("createPersonaPolicyWorker",
                    provisionalRangerPolicies.stream()
                            .map(x -> new CreateRangerPolicyWorker(context, x, atlasRangerService))
                            .collect(Collectors.toList()));
        } else {
            LOG.error("No provisional policy to create on Ranger");
        }

        return ret;
    }

    private boolean isAssetUpdate(PersonaContext context) {
        return !CollectionUtils.isEqualCollection(getAssets(context.getExistingPersonaPolicy()), getAssets(context.getPersonaPolicy()));
    }

    private boolean isDataPolicyTypeUpdate(PersonaContext context) {
        if (!isDataPolicy(context.getPersonaPolicy())) {
            return false;
        }

        String existingMask = getDataPolicyMaskType(context.getExistingPersonaPolicy());
        existingMask = existingMask == null ? "" : existingMask;

        String newMask = getDataPolicyMaskType(context.getPersonaPolicy());

        return !existingMask.equals(newMask) && (StringUtils.isEmpty(existingMask) || StringUtils.isEmpty(newMask));
    }

    public void deletePersonaPolicy(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePersonaPolicy");
        context.setDeletePersonaPolicy(true);

        AtlasEntity personaPolicy = context.getPersonaPolicy();
        AtlasEntityWithExtInfo personaExtInfo = context .getPersonaExtInfo();

        try {
            if(!POLICY_ENTITY_TYPE.equals(personaPolicy.getTypeName())) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + POLICY_ENTITY_TYPE);
            }

            if(!personaPolicy.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
                LOG.info("Persona policy with guid {} is already deleted/purged", personaPolicy.getGuid());
                return;
            }

            List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                    context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                    context.isDataMaskPolicy() ? RANGER_POLICY_TYPE_DATA_MASK : RANGER_POLICY_TYPE_ACCESS,
                    getPersonaPolicyLabel(personaPolicy.getGuid()));

            String role = getRoleName(personaExtInfo.getEntity());

            for (RangerPolicy rangerPolicy : rangerPolicies) {
                boolean needUpdate = false;

                if (context.isDataMaskPolicy()) {
                    List<RangerDataMaskPolicyItem> policyItems = rangerPolicy.getDataMaskPolicyItems();

                    for (RangerPolicyItem policyItem : new ArrayList<>(policyItems)) {
                        if (policyItem.getRoles().remove(role)) {
                            needUpdate = true;
                            if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
                                policyItems.remove(policyItem);
                            }
                        }
                    }

                    if (CollectionUtils.isEmpty(rangerPolicy.getDataMaskPolicyItems())) {
                        atlasRangerService.deleteRangerPolicy(rangerPolicy);
                        needUpdate = false;
                    }
                } else {
                    List<RangerPolicyItem> policyItems = getIsAllow(personaPolicy) ?
                            rangerPolicy.getPolicyItems() :
                            rangerPolicy.getDenyPolicyItems();

                    for (RangerPolicyItem policyItem : new ArrayList<>(policyItems)) {
                        if (policyItem.getRoles().remove(role)) {
                            needUpdate = true;
                            if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
                                policyItems.remove(policyItem);
                            }
                        }
                    }

                    if (CollectionUtils.isEmpty(rangerPolicy.getPolicyItems()) && CollectionUtils.isEmpty(rangerPolicy.getDenyPolicyItems())) {
                        atlasRangerService.deleteRangerPolicy(rangerPolicy);
                        needUpdate = false;
                    }
                }

                if (needUpdate) {
                    rangerPolicy.getPolicyLabels().remove(getPersonaPolicyLabel(personaPolicy.getGuid()));
                    rangerPolicy.getPolicyLabels().remove(getPersonaLabel(personaExtInfo.getEntity().getGuid()));

                    long policyLabelCount = rangerPolicy.getPolicyLabels().stream().filter(x -> x.startsWith(LABEL_PREFIX_PERSONA)).count();
                    if (policyLabelCount == 0) {
                        rangerPolicy.getPolicyLabels().remove(LABEL_TYPE_PERSONA);
                    }

                    atlasRangerService.updateRangerPolicy(rangerPolicy);
                }
            }

            List<String> actions = getActions(personaPolicy);
            if (actions.contains(ACCESS_ENTITY_READ)) {
                aliasStore.updateAlias(context);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void cleanRoleFromExistingPolicies(AtlasEntity persona, List<RangerPolicy> rangerPolicies,
                                               List<String> removePolicyGuids) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("cleanRoleFromExistingPolicies");
        LOG.info("clean role from existing {} policies", rangerPolicies.size());

        try {
            if (CollectionUtils.isNotEmpty(rangerPolicies)) {
                submitCallablesAndWaitToFinish("cleanRoleWorker",
                        rangerPolicies.stream()
                                .map(x -> new CleanRoleWorker(persona, x, removePolicyGuids, atlasRangerService))
                                .collect(Collectors.toList()));
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void filterRemovedActions(List<String> removedActions, List<String> updatedActions,
                                      List<String>... actionsSet) {

        for (List<String> actionSet : actionsSet) {
            if (removedActions.size() > 0) {
                Collection<String> removedActionItems = CollectionUtils.intersection(removedActions, actionSet);
                Collection<String> processedActionItems = CollectionUtils.intersection(updatedActions, actionSet);

                if (removedActionItems.size() > 0 && processedActionItems.size() > 0) {
                    removedActions.removeAll(actionSet);
                }
            }
        }
    }

    /*
     *
     * This method removes action that does not have any other action left of its type
     * e.g. consider entity action type -> entity-read,entity-create,entity-update,entity-delete
     *      There were only one entity action in policy say entity-read,
     *      removing entity-read while updating policy will call this method
     *
     *
     *    check if resource match is found in existingRangerPolicies
     *        if yes, remove access from policy Item
     *            check if no access remaining, remove item if true
     *                 check if no policy item remaining, delete Ranger policy if true
     *        if not, search by resources
     *            if found, remove access from policy Item
     *                check if no access remaining, remove item if true
     *                     check if no policy item remaining, delete Ranger policy if true
     *
     * */
    private void processActionsRemoval(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processActionsRemoval");
        List<String> existingActions = getActions(context.getExistingPersonaPolicy());
        List<String> updatedActions = getActions(context.getPersonaPolicy());

        List<String> removedActions = existingActions.stream()
                .filter(x -> !updatedActions.contains(x))
                .collect(Collectors.toList());

        try {
            filterRemovedActions(removedActions, updatedActions, ENTITY_ACTIONS, CLASSIFICATION_ACTIONS, TERM_ACTIONS, LABEL_ACTIONS);

            if (CollectionUtils.isNotEmpty(removedActions)) {
                List<RangerPolicy> provisionalPoliciesForDelete = PersonaServiceHelper.personaPolicyToRangerPolicies(context, removedActions);

                Map<RangerPolicy, RangerPolicy> provisionalToRangerPoliciesMap = mapPolicies(context, provisionalPoliciesForDelete, context.getExcessExistingRangerPolicies());

                List<RangerPolicy> provPoliciesForResourceSearch = new ArrayList<>();
                List<RangerPolicy> rangerPoliciesToClean = new ArrayList<>();

                provisionalToRangerPoliciesMap
                        .forEach((k, v) -> {
                            if (v == null) {
                                provPoliciesForResourceSearch.add(k);
                            } else {
                                rangerPoliciesToClean.add(v);
                            }
                        });

                for (RangerPolicy provPolicy : provPoliciesForResourceSearch) {
                    RangerPolicy rangerPolicy = AccessControlUtil.fetchRangerPolicyByResources(atlasRangerService,
                            context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                            context.isDataMaskPolicy() ? RANGER_POLICY_TYPE_DATA_MASK : RANGER_POLICY_TYPE_ACCESS,
                            provPolicy);

                    if (rangerPolicy != null) {
                        rangerPoliciesToClean.add(rangerPolicy);
                    }
                }

                List<String> allPolicyGuids = getLabelsForPersonaPolicy(context.getPersona().getGuid(), context.getPersonaPolicy().getGuid());
                cleanRoleFromExistingPolicies(context.getPersona(), rangerPoliciesToClean, allPolicyGuids);
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void processUpdatePolicies(PersonaContext context,
                                 Map<RangerPolicy, RangerPolicy> provisionalToRangerPoliciesMap) throws AtlasBaseException {
        if (MapUtils.isEmpty(provisionalToRangerPoliciesMap)) {
            throw new AtlasBaseException("Policies map is empty");
        }
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processUpdatePolicies");

        try {
            if (MapUtils.isNotEmpty(provisionalToRangerPoliciesMap)) {
                submitCallablesAndWaitToFinish("updateRangerPolicyWorker",
                        provisionalToRangerPoliciesMap.entrySet().stream()
                                .map(x -> new UpdateRangerPolicyWorker(context, x.getValue(), x.getKey(), atlasRangerService)).
                                collect(Collectors.toList()));
            } else {
                LOG.error("No provisional policy pair found to create on Ranger");
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    /*
    * @Param provisionalRangerPolicies -> Policies transformed from AtlasPersonaPolicy to Ranger policy
    * @Param existingRangerPolicies -> Policies found by label search
    * */
    private Map<RangerPolicy, RangerPolicy> mapPolicies(PersonaContext context,
                                                        List<RangerPolicy> provisionalRangerPolicies,
                                                        List<RangerPolicy> existingRangerPolicies) {

        Map<RangerPolicy, RangerPolicy> ret = new HashMap<>();

        for (RangerPolicy existingRangerPolicy: existingRangerPolicies) {
            boolean mapped = false;
            String existingRangerPolicySignature = new RangerPolicyResourceSignature(existingRangerPolicy).getSignature();
            int existingPolicyType = existingRangerPolicy.getPolicyType();

            for (RangerPolicy provisionalRangerPolicy: provisionalRangerPolicies) {
                String provisionalRangerPolicySignature = new RangerPolicyResourceSignature(provisionalRangerPolicy).getSignature();
                int provisionalPolicyType = provisionalRangerPolicy.getPolicyType();

                if (existingRangerPolicySignature.equals(provisionalRangerPolicySignature) && existingPolicyType == provisionalPolicyType) {
                    ret.put(provisionalRangerPolicy, existingRangerPolicy);
                    mapped = true;
                    break;
                }
            }

            if (!mapped) {
                //excess Ranger policy for persona policy
                context.addExcessExistingRangerPolicy(existingRangerPolicy);
            }
        }

        for (RangerPolicy provisionalRangerPolicy: provisionalRangerPolicies) {
            if (!ret.containsKey(provisionalRangerPolicy)) {
                ret.put(provisionalRangerPolicy, null);
            }
        }

        return ret;
    }
}
