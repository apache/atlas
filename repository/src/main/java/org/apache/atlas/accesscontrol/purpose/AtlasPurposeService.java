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
package org.apache.atlas.accesscontrol.purpose;


import org.apache.atlas.RequestContext;
import org.apache.atlas.accesscontrol.aliasstore.ESAliasStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.accesscontrol.AccessControlUtil.fetchRangerPoliciesByLabel;
import static org.apache.atlas.accesscontrol.AccessControlUtil.fetchRangerPolicyByResources;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getESAliasName;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getIsEnabled;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getName;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.ACCESS_ENTITY_READ;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.POLICY_QN_FORMAT;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.RESOURCE_TAG;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.fetchRangerPoliciesByResourcesForPurposeDelete;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getActions;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getIsAllow;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getPolicies;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getPurposeLabel;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getPurposePolicyLabel;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getQualifiedName;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getTags;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getTenantId;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getUUID;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.validateUniquenessByName;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.validateUniquenessByTags;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;


public class AtlasPurposeService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPurposeService.class);

    private final AtlasGraph graph;
    private final ESAliasStore aliasStore;
    private final EntityGraphRetriever entityRetriever;

    private AtlasRangerService atlasRangerService;

    public AtlasPurposeService(AtlasGraph graph,
                               EntityGraphRetriever entityRetriever) {
        this.atlasRangerService = new AtlasRangerService();
        this.graph = graph;
        this.entityRetriever = entityRetriever;

        this.aliasStore = new ESAliasStore(graph, entityRetriever);
    }

    public EntityMutationResponse createPurpose(PurposeContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPurpose");
        EntityMutationResponse ret = null;
        LOG.info("Creating Purpose");

        AtlasEntityWithExtInfo entityWithExtInfo = context.getPurposeExtInfo();

        context.setCreateNewPurpose(true);

        try {
            validateUniquenessByName(graph, getName(entityWithExtInfo.getEntity()), PURPOSE_ENTITY_TYPE);
            validateUniquenessByTags(graph, getTags(entityWithExtInfo.getEntity()), PURPOSE_ENTITY_TYPE);

            //unique qualifiedName for Purpose
            String tenantId = getTenantId(context.getPurpose());
            if (StringUtils.isEmpty(tenantId)) {
                tenantId = "tenant";
            }
            entityWithExtInfo.getEntity().setAttribute(QUALIFIED_NAME, String.format(POLICY_QN_FORMAT, tenantId, getUUID()));
            entityWithExtInfo.getEntity().setAttribute("enabled", true);

            aliasStore.createAlias(context);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        return ret;
    }

    public void updatePurpose(PurposeContext context, AtlasEntityWithExtInfo existingPurposeWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePurpose");
        LOG.info("Updating Purpose");
        boolean needPolicyUpdate = false;

        AtlasEntity purpose = context.getPurpose();
        AtlasEntity existingPurposeEntity = existingPurposeWithExtInfo.getEntity();

        purpose.setAttribute(QUALIFIED_NAME, getQualifiedName(existingPurposeEntity));

        try {
            if (!AtlasEntity.Status.ACTIVE.equals(existingPurposeEntity.getStatus())) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Purpose not Active");
            }

            if (getIsEnabled(existingPurposeEntity) != getIsEnabled(purpose)) {
                if (getIsEnabled(purpose)) {
                    enablePurpose(existingPurposeWithExtInfo);
                } else {
                    disablePurpose(existingPurposeWithExtInfo);
                }
            }

            //check name update
            // if yes: check naw name for uniqueness
            if (!getName(purpose).equals(getName(existingPurposeEntity))) {
                validateUniquenessByName(graph, getName(purpose), PURPOSE_ENTITY_TYPE);
            }

            //check tags update
            // if yes: check tags for uniqueness
            if (!CollectionUtils.isEqualCollection(getTags(purpose), getTags(existingPurposeEntity))) {
                needPolicyUpdate = true;
                validateUniquenessByTags(graph, getTags(purpose), PURPOSE_ENTITY_TYPE);
            }

            if (needPolicyUpdate && CollectionUtils.isNotEmpty(getPolicies(context.getPurposeExtInfo()))) {
                updatePurposePoliciesTag(context, existingPurposeEntity);
                aliasStore.updateAlias(context);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void disablePurpose(AtlasEntityWithExtInfo existingPurposeWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("disablePurpose");

        try {
            cleanRangerPolicies(existingPurposeWithExtInfo.getEntity());
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void enablePurpose(AtlasEntityWithExtInfo existingPurposeWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("enablePurpose");

        PurposeContext context = new PurposeContext();
        context.setPurposeExtInfo(existingPurposeWithExtInfo);

        try {
            List<RangerPolicy> provisionalRangerPolicies = PurposeServiceHelper.purposePolicyToRangerPolicy(context);
            updatePurposePolicies(context, provisionalRangerPolicies);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public void deletePurpose(AtlasEntityWithExtInfo personaExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePurpose");
        AtlasEntity purpose = personaExtInfo.getEntity();

        if(!purpose.getTypeName().equals(PURPOSE_ENTITY_TYPE)) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + PURPOSE_ENTITY_TYPE);
        }

        try {
            if(!purpose.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
                LOG.info("Purpose with guid {} is already deleted/purged", purpose.getGuid());
                return;
            }

            cleanRangerPolicies(purpose);

            aliasStore.deleteAlias(getESAliasName(purpose));
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void cleanRangerPolicies(AtlasEntity purpose) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                "tag",
                null,
                getPurposeLabel(purpose.getGuid()));

        if (CollectionUtils.isEmpty(rangerPolicies)) {
            rangerPolicies = fetchRangerPoliciesByResourcesForPurposeDelete(atlasRangerService, purpose);
        }

        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            for (RangerPolicy policy : rangerPolicies) {
                atlasRangerService.deleteRangerPolicy(policy);
            }
        }
    }

    public void deletePurposePolicy(PurposeContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePurposePolicy");
        context.setDeletePurposePolicy(true);

        AtlasEntity purposePolicy = context.getPurposePolicy();

        try {
            if(!POLICY_ENTITY_TYPE.equals(purposePolicy.getTypeName())) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + POLICY_ENTITY_TYPE);
            }

            if(!purposePolicy.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
                LOG.info("Purpose policy with guid {} is already deleted/purged", purposePolicy.getGuid());
                return;
            }

            List<RangerPolicy> provisionalRangerPolicies = PurposeServiceHelper.purposePolicyToRangerPolicy(context);
            updatePurposePolicies(context, provisionalRangerPolicies);

            processDeletePolicy(context, provisionalRangerPolicies);

            List<String> actions = getActions(purposePolicy);
            if (actions.contains(ACCESS_ENTITY_READ)) {
                aliasStore.updateAlias(context);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processDeletePolicy(PurposeContext context, List<RangerPolicy> provisionalRangerPolicies) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = null;

        if (CollectionUtils.isEmpty(provisionalRangerPolicies)) {
            rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                    "tag",
                    null,
                    getPurposeLabel(context.getPurpose().getGuid()));
        } else if (provisionalRangerPolicies.size() == 1) {
            rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                    "tag",
                    String.valueOf(provisionalRangerPolicies.get(0).getPolicyType() == 0 ? 1 : 0),
                    getPurposeLabel(context.getPurpose().getGuid()));
        }

        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            atlasRangerService.deleteRangerPolicy(rangerPolicies.get(0));
        }
    }

    public void createPurposePolicy(PurposeContext context) throws AtlasBaseException  {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPurposePolicy");

        AtlasEntity purposePolicy = context.getPurposePolicy();
        context.setCreateNewPurposePolicy(true);
        context.setAllowPolicy(getIsAllow(purposePolicy));
        context.setAllowPolicyUpdate();

        try {
            PurposeServiceHelper.validatePurposePolicy(context);

            purposePolicy.setAttribute(QUALIFIED_NAME, String.format(POLICY_QN_FORMAT, getQualifiedName(context.getPurpose()), getUUID()));

            List<RangerPolicy> provisionalRangerPolicies = PurposeServiceHelper.purposePolicyToRangerPolicy(context);

            updatePurposePolicies(context, provisionalRangerPolicies);

            aliasStore.updateAlias(context);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public void updatePurposePolicy(PurposeContext context) throws AtlasBaseException  {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePurposePolicy");

        AtlasEntity purposePolicy = context.getPurposePolicy();
        context.setCreateNewPurposePolicy(false);
        context.setAllowPolicy(getIsAllow(purposePolicy));
        context.setAllowPolicyUpdate();

        try {
            PurposeServiceHelper.validatePurposePolicy(context);

            List<RangerPolicy> provisionalRangerPolicies = PurposeServiceHelper.purposePolicyToRangerPolicy(context);
            updatePurposePolicies(context, provisionalRangerPolicies);

            aliasStore.updateAlias(context);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void updatePurposePolicies(PurposeContext context, List<RangerPolicy> provisionalPolicies) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePurposePolicies");
        RangerPolicy ret;

        try {
            if (context.getExistingPurposePolicy() != null && !AtlasEntity.Status.ACTIVE.equals(context.getExistingPurposePolicy().getStatus())) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Entity not Active");
            }

            for (RangerPolicy provisionalPolicy : provisionalPolicies) {
                List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                        "tag",
                        String.valueOf(provisionalPolicy.getPolicyType()),
                        getPurposeLabel(context.getPurpose().getGuid()));

                RangerPolicy rangerPolicy = null;
                if (CollectionUtils.isEmpty(rangerPolicies)) {
                    rangerPolicy = fetchRangerPolicyByResources(atlasRangerService,
                            "tag",
                            String.valueOf(provisionalPolicy.getPolicyType()),
                            provisionalPolicy);

                } else if (rangerPolicies.size() == 1) {
                    rangerPolicy = rangerPolicies.get(0);
                } else {
                    String resourcesSignature = new RangerPolicyResourceSignature(provisionalPolicy).getSignature();

                    for (RangerPolicy matchedRangerPolicy : rangerPolicies) {
                        String labelMatchedPolicyResourcesSignature = new RangerPolicyResourceSignature(matchedRangerPolicy).getSignature();

                        if (resourcesSignature.equals(labelMatchedPolicyResourcesSignature)) {
                            rangerPolicy = matchedRangerPolicy;
                        }
                    }
                }

                if (rangerPolicy == null) {
                    ret = atlasRangerService.createRangerPolicy(provisionalPolicy);
                } else {

                    rangerPolicy.setPolicyItems(provisionalPolicy.getPolicyItems());
                    rangerPolicy.setDenyPolicyItems(provisionalPolicy.getDenyPolicyItems());
                    rangerPolicy.setDataMaskPolicyItems(provisionalPolicy.getDataMaskPolicyItems());

                    if (context.isDeletePurposePolicy()) {
                        rangerPolicy.getPolicyLabels().remove(getPurposePolicyLabel(context.getPurposePolicy().getGuid()));
                    } else {
                        rangerPolicy.getPolicyLabels().addAll(provisionalPolicy.getPolicyLabels());
                    }

                    ret = atlasRangerService.updateRangerPolicy(rangerPolicy);

                    LOG.info("Updated Ranger Policy with ID {}", ret.getId());
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void updatePurposePoliciesTag(PurposeContext context, AtlasEntity existingPurposeEntity) throws AtlasBaseException {
        RangerPolicy accessPolicy = null;
        RangerPolicy maskingPolicy = null;

        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                "tag",
                null,
                getPurposeLabel(context.getPurpose().getGuid()));

        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            List<String> tags = getTags(existingPurposeEntity);

            for (RangerPolicy rangerPolicy : rangerPolicies) {
                List<String> rangerPolicyTags = rangerPolicy.getResources().get("tag").getValues();

                if (CollectionUtils.isEqualCollection(tags, rangerPolicyTags)) {

                    if (Integer.valueOf("0").equals(rangerPolicy.getPolicyType())) {
                        accessPolicy = rangerPolicy;
                    }

                    if (Integer.valueOf("1").equals(rangerPolicy.getPolicyType())) {
                        maskingPolicy = rangerPolicy;
                    }
                }
            }
        }

        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        resources.put(RESOURCE_TAG, new RangerPolicy.RangerPolicyResource(getTags(context.getPurpose()), false, false));

        if (maskingPolicy != null) {
            maskingPolicy.setResources(resources);
            atlasRangerService.updateRangerPolicy(maskingPolicy);
        }

        if (accessPolicy != null) {
            accessPolicy.setResources(resources);
            atlasRangerService.updateRangerPolicy(accessPolicy);
        }
    }
}
