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

package org.apache.atlas.plugin.model.validation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.atlas.plugin.policyresourcematcher.RangerPolicyResourceEvaluator;
import org.apache.atlas.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.atlas.plugin.resourcematcher.RangerResourceMatcher;
import org.apache.atlas.plugin.util.ServiceDefUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RangerZoneResourceMatcher implements RangerPolicyResourceEvaluator {
    private static final Log LOG = LogFactory.getLog(RangerZoneResourceMatcher.class);

    private final String                                         securityZoneName;
    private final Map<String, RangerPolicy.RangerPolicyResource> policyResource;
    private final RangerPolicyResourceMatcher                    policyResourceMatcher;
    private RangerServiceDef.RangerResourceDef                   leafResourceDef;

    public RangerZoneResourceMatcher(final String securityZoneName, final Map<String, RangerPolicy.RangerPolicyResource> policyResource, final RangerServiceDef serviceDef) {

        RangerServiceDefHelper             serviceDefHelper = new RangerServiceDefHelper(serviceDef);
        final Collection<String>           resourceKeys     = policyResource.keySet();

        RangerDefaultPolicyResourceMatcher matcher          = new RangerDefaultPolicyResourceMatcher();

        matcher.setServiceDef(serviceDef);
        matcher.setServiceDefHelper(serviceDefHelper);

        boolean found = false;

        for (String policyType : RangerPolicy.POLICY_TYPES) {
            for (List<RangerServiceDef.RangerResourceDef> hierarchy : serviceDefHelper.getResourceHierarchies(policyType)) {
                if (serviceDefHelper.hierarchyHasAllResources(hierarchy, resourceKeys)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found hierarchy for resource-keys:[" + resourceKeys + "], policy-type:[" + policyType + "]");
                    }
                    matcher.setPolicyResources(policyResource, policyType);
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        if (found) {
            matcher.init();
        } else {
            LOG.error("Cannot initialize matcher for RangerZoneResourceMatcher");
        }

        this.securityZoneName      = securityZoneName;
        this.policyResourceMatcher = matcher;
        this.policyResource        = policyResource;
        this.leafResourceDef   = ServiceDefUtil.getLeafResourceDef(serviceDef, getPolicyResource());
    }

    public String getSecurityZoneName() { return securityZoneName; }

    @Override
    public long getId() {
        return securityZoneName.hashCode();
    }

    @Override
    public String getGuid() {
        return null;
    }

    @Override
    public RangerPolicyResourceMatcher getPolicyResourceMatcher() { return policyResourceMatcher; }

    @Override
    public Map<String, RangerPolicy.RangerPolicyResource> getPolicyResource() {
        return policyResource;
    }

    @Override
    public RangerResourceMatcher getResourceMatcher(String resourceName) {
        return policyResourceMatcher != null ? policyResourceMatcher.getResourceMatcher(resourceName) : null;
    }

    @Override
    public boolean isAncestorOf(RangerServiceDef.RangerResourceDef resourceDef) {
        return ServiceDefUtil.isAncestorOf(policyResourceMatcher.getServiceDef(), leafResourceDef, resourceDef);
    }

    @Override
    public String toString() {
        return "{security-zone-name:[" + securityZoneName + "], policyResource=[" + policyResource +"]}";
    }
}
