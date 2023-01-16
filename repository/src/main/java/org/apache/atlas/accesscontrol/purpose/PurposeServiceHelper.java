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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.POLICY_ALREADY_EXISTS;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_ADD_REL;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_REMOVE_REL;
import static org.apache.atlas.accesscontrol.AccessControlUtil.ACCESS_UPDATE_REL;
import static org.apache.atlas.accesscontrol.AccessControlUtil.LINK_ASSET_ACTION;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getActions;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getDataPolicyMaskType;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getIsAllow;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getName;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getPolicies;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getPolicyGroups;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getPolicyUsers;
import static org.apache.atlas.accesscontrol.AccessControlUtil.isDataMaskPolicy;
import static org.apache.atlas.accesscontrol.AccessControlUtil.isDataPolicy;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.RESOURCE_TAG;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.formatAccessType;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.formatMaskType;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getIsAllUsers;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getPurposeLabel;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getPurposePolicyLabel;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.getTags;

public class PurposeServiceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PurposeServiceHelper.class);

    protected static void validatePurposePolicy(PurposeContext context) throws AtlasBaseException {
        validatePurposePolicyRequest(context);
        verifyUniqueNameForPurposePolicy(context);
    }

    private static void validatePurposePolicyRequest(PurposeContext context) throws AtlasBaseException {
        if (!AtlasEntity.Status.ACTIVE.equals(context.getPurpose().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Purpose not Active");
        }

        if (CollectionUtils.isEmpty(getActions(context.getPurposePolicy()))) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide actions for policy policy");
        }

        if (CollectionUtils.isEmpty(getPolicyUsers(context.getPurposePolicy())) &&
                CollectionUtils.isEmpty(getPolicyGroups(context.getPurposePolicy())) && !getIsAllUsers(context.getPurposePolicy())) {

            throw new AtlasBaseException(BAD_REQUEST, "Please provide users/groups OR select All users for policy policy");
        }
    }

    private static void verifyUniqueNameForPurposePolicy(PurposeContext context) throws AtlasBaseException {

        if (!context.isCreateNewPurposePolicy() && !getName(context.getExistingPurposePolicy()).equals(getName(context.getPurpose()))) {
            return;
        }
        List<String> policyNames = new ArrayList<>();
        String newPolicyName = getName(context.getPurposePolicy());

        List<AtlasEntity> policies = getPolicies(context.getPurposeExtInfo());

        if (CollectionUtils.isNotEmpty(policies)) {
            if (context.isCreateNewPurposePolicy()) {
                policies = policies.stream()
                        .filter(x -> !x.getGuid().equals(context.getPurposePolicy().getGuid()))
                        .collect(Collectors.toList());
            }
        }

        policies.forEach(x -> policyNames.add(getName(x)));

        if (policyNames.contains(newPolicyName)) {
            throw new AtlasBaseException(POLICY_ALREADY_EXISTS, newPolicyName);
        }
    }

    protected static List<RangerPolicy> purposePolicyToRangerPolicy(PurposeContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("purposePolicyToRangerPolicy");

        List<RangerPolicy> ret = new ArrayList<>();
        RangerPolicy rangerAccessPolicy, rangerMaskPolicy;

        AtlasEntity purpose = context.getPurpose();

        List<String> tags = getTags(purpose);
        try {
            if (CollectionUtils.isEmpty(tags)) {
                throw new AtlasBaseException("Tags list is empty");
            }

            rangerAccessPolicy = getRangerPolicy(context, 0);
            rangerMaskPolicy = getRangerPolicy(context, 1);

            Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
            resources.put(RESOURCE_TAG, new RangerPolicy.RangerPolicyResource(tags, false, false));

            rangerAccessPolicy.setResources(resources);
            rangerMaskPolicy.setResources(resources);

            policyToRangerPolicy(context, rangerAccessPolicy, rangerMaskPolicy);

            if (CollectionUtils.isNotEmpty(rangerAccessPolicy.getPolicyItems()) || CollectionUtils.isNotEmpty(rangerAccessPolicy.getDenyPolicyItems())) {
                ret.add(rangerAccessPolicy);
            }

            if (CollectionUtils.isNotEmpty(rangerMaskPolicy.getDataMaskPolicyItems())) {
                ret.add(rangerMaskPolicy);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
        return ret;
    }

    private static RangerPolicy getRangerPolicy(PurposeContext context, int policyType){
        RangerPolicy rangerPolicy = new RangerPolicy();
        AtlasEntity purpose = context.getPurpose();

        rangerPolicy.setPolicyLabels(Arrays.asList(getPurposeLabel(purpose.getGuid()), "type:purpose"));

        rangerPolicy.setPolicyType(policyType);
        rangerPolicy.setServiceType("tag"); //TODO: disable & check
        rangerPolicy.setService("default_atlan"); //TODO: read from property config

        return rangerPolicy;
    }

    private static void policyToRangerPolicy(PurposeContext context, RangerPolicy rangerAccessPolicy, RangerPolicy rangerMaskPolicy) {
        rangerAccessPolicy.setName("purpose-tag-" + UUID.randomUUID());

        for (AtlasEntity policy : getPolicies(context.getPurposeExtInfo())) {
            if (context.isDeletePurposePolicy() && policy.getGuid().equals(context.getPurposePolicy().getGuid())) {
                continue;
            }

            List<String> actions = getActions(policy);
            List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();

            if (isDataPolicy(policy)) {
                for (String action : actions) {
                    accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType("heka", action)));
                }

                if (isDataMaskPolicy(policy)) {
                    rangerMaskPolicy.setName("purpose-tag-mask-" + UUID.randomUUID());
                    String maskType = getDataPolicyMaskType(policy);
                    RangerPolicy.RangerPolicyItemDataMaskInfo maskInfo = new RangerPolicy.RangerPolicyItemDataMaskInfo(formatMaskType(maskType), null, null);
                    setPolicyItem(policy, rangerMaskPolicy, accesses, maskInfo);
                    rangerMaskPolicy.getPolicyLabels().add(getPurposePolicyLabel(policy.getGuid()));

                } else {
                    setPolicyItem(policy, rangerAccessPolicy, accesses, null);
                    rangerAccessPolicy.getPolicyLabels().add(getPurposePolicyLabel(policy.getGuid()));
                }
            } else {
                for (String action : actions) {
                    if (action.equals(LINK_ASSET_ACTION)) {
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType(ACCESS_ADD_REL)));
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType(ACCESS_UPDATE_REL)));
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType(ACCESS_REMOVE_REL)));
                    } else {
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType(action)));
                    }
                }
                setPolicyItem(policy, rangerAccessPolicy, accesses, null);
                rangerAccessPolicy.getPolicyLabels().add(getPurposePolicyLabel(policy.getGuid()));
            }
        }
    }

    private static void setPolicyItem(AtlasEntity purposePolicy, RangerPolicy rangerPolicy,
                               List<RangerPolicy.RangerPolicyItemAccess> accesses, RangerPolicy.RangerPolicyItemDataMaskInfo maskInfo) {

        List<String> users = getPolicyUsers(purposePolicy);
        List<String> groups = getPolicyGroups(purposePolicy);
        if (getIsAllUsers(purposePolicy)) {
            users = null;
            groups = Collections.singletonList("public");
        }

        if (maskInfo != null) {
            RangerPolicy.RangerDataMaskPolicyItem policyItem = new RangerPolicy.RangerDataMaskPolicyItem(accesses, maskInfo, users, groups, null, null, false);
            rangerPolicy.getDataMaskPolicyItems().add(policyItem);

        } else {
            RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, users, groups, null, null, false);

            if (getIsAllow(purposePolicy)) {
                rangerPolicy.getPolicyItems().addAll(Collections.singletonList(policyItem));
            } else {
                rangerPolicy.getDenyPolicyItems().addAll(Collections.singletonList(policyItem));
            }
        }
    }
}
