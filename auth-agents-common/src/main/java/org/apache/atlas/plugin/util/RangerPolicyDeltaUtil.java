/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerPolicyDelta;
import org.apache.atlas.plugin.store.EmbeddedServiceDefsUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RangerPolicyDeltaUtil {

    private static final Log LOG = LogFactory.getLog(RangerPolicyDeltaUtil.class);

    private static final Log PERF_POLICY_DELTA_LOG = RangerPerfTracer.getPerfLogger("policy.delta");

    public static List<RangerPolicy> deletePoliciesByDelta(List<RangerPolicy> policies, Map<String, RangerPolicyDelta> deltas) {
        if (MapUtils.isNotEmpty(deltas)) {
            List<RangerPolicy> ret = new ArrayList<>(policies);
            ret.removeIf(policy -> deltas.containsKey(policy.getAtlasGuid()));
            return ret;
        } else {
            return policies;
        }
    }

    public static List<RangerPolicy> applyDeltas(List<RangerPolicy> policies, List<RangerPolicyDelta> deltas, String serviceType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> applyDeltas(serviceType=" + serviceType + ")");
        }

        if (CollectionUtils.isEmpty(deltas)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("applyDeltas called with empty deltas. Returning policies without changes.");
            }
            return policies;
        }

        boolean hasExpectedServiceType = deltas.stream().anyMatch(delta -> serviceType.equals(delta.getServiceType()));

        if (!hasExpectedServiceType) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No deltas match the expected serviceType: " + serviceType);
            }
            return policies;
        }

        List<RangerPolicy> updatedPolicies = new ArrayList<>(policies);

        for (RangerPolicyDelta delta : deltas) {
            if (!serviceType.equals(delta.getServiceType())) {
                continue;
            }

            switch (delta.getChangeType()) {
                case RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE:
                case RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE:
                    updatedPolicies.add(delta.getPolicy());
                    break;
                default:
                    LOG.warn("Unexpected changeType in policyDelta: [" + delta + "]. Ignoring delta.");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== applyDeltas(serviceType=" + serviceType + "): " + updatedPolicies);
        }

        return updatedPolicies;
    }

    public static boolean isValidDeltas(List<RangerPolicyDelta> deltas, String componentServiceType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isValidDeltas(deltas=" + Arrays.toString(deltas.toArray()) + ", componentServiceType=" + componentServiceType +")");
        }
        boolean isValid = true;

        for (RangerPolicyDelta delta : deltas) {
            final Integer changeType = delta.getChangeType();
            final String policyGuid = delta.getPolicyGuid();

            if (changeType == null) {
                isValid = false;
                break;
            }

            if (changeType != RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE
                    && changeType != RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE
                    && changeType != RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE) {
                isValid = false;
            } else if (policyGuid == null) {
                isValid = false;
            } else {
                final String  serviceType = delta.getServiceType();
                final String  policyType  = delta.getPolicyType();

                if (serviceType == null || (!serviceType.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME) &&
                        !serviceType.equals(componentServiceType))) {
                    isValid = false;
                } else if (StringUtils.isEmpty(policyType) || (!RangerPolicy.POLICY_TYPE_ACCESS.equals(policyType)
                        && !RangerPolicy.POLICY_TYPE_DATAMASK.equals(policyType)
                        && !RangerPolicy.POLICY_TYPE_ROWFILTER.equals(policyType))) {
                    isValid = false;
                }
            }

            if (!isValid) {
                break;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isValidDeltas(deltas=" + Arrays.toString(deltas.toArray()) + ", componentServiceType=" + componentServiceType +"): " + isValid);
        }
        return isValid;
    }

    public static Boolean hasPolicyDeltas(final ServicePolicies servicePolicies) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> hasPolicyDeltas(servicePolicies:[" + servicePolicies + "]");
        }
        final Boolean ret;

        if (servicePolicies == null) {
            LOG.error("ServicePolicies are null!");
            ret = null;
        } else {
            boolean isPoliciesExistInSecurityZones     = false;
            boolean isPolicyDeltasExistInSecurityZones = false;

            if (MapUtils.isNotEmpty(servicePolicies.getSecurityZones())) {
                for (ServicePolicies.SecurityZoneInfo element : servicePolicies.getSecurityZones().values()) {
                    if (CollectionUtils.isNotEmpty(element.getPolicies()) && CollectionUtils.isEmpty(element.getPolicyDeltas())) {
                        isPoliciesExistInSecurityZones = true;
                    }
                    if (CollectionUtils.isEmpty(element.getPolicies()) && CollectionUtils.isNotEmpty(element.getPolicyDeltas())) {
                        isPolicyDeltasExistInSecurityZones = true;
                    }
                }
            }

            boolean isPoliciesExist     = CollectionUtils.isNotEmpty(servicePolicies.getPolicies()) || (servicePolicies.getTagPolicies() != null && CollectionUtils.isNotEmpty(servicePolicies.getTagPolicies().getPolicies())) || isPoliciesExistInSecurityZones;
            boolean isPolicyDeltasExist = CollectionUtils.isNotEmpty(servicePolicies.getPolicyDeltas()) || isPolicyDeltasExistInSecurityZones;

            if (isPoliciesExist && isPolicyDeltasExist) {
                LOG.warn("ServicePolicies contain both policies and policy-deltas!! Cannot build policy-engine from these servicePolicies. Please check server-side code!");
                LOG.warn("Downloaded ServicePolicies for [" + servicePolicies.getServiceName() + "]");
                ret = null;
            } else if (!isPoliciesExist && !isPolicyDeltasExist) {
                LOG.warn("ServicePolicies do not contain any policies or policy-deltas!! There are no material changes in the policies.");
                LOG.warn("Downloaded ServicePolicies for [" + servicePolicies.getServiceName() + "]");
                ret = null;
            } else {
                ret = isPolicyDeltasExist;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== hasPolicyDeltas(servicePolicies:[" + servicePolicies + "], ret:[" + ret + "]");
        }
        return ret;
    }
}
