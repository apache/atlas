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

package org.apache.atlas.plugin.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.atlas.plugin.contextenricher.RangerContextEnricher;
import org.apache.atlas.plugin.policyengine.PolicyEngine;
import org.apache.atlas.plugin.policyengine.RangerAccessRequest;
import org.apache.atlas.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.atlas.plugin.policyengine.RangerAccessRequestProcessor;
import org.apache.atlas.plugin.policyengine.RangerAccessResource;
import org.apache.atlas.plugin.policyengine.RangerMutableResource;
import org.apache.atlas.plugin.util.RangerAccessRequestUtil;

import java.util.List;
import java.util.Set;

public class RangerDefaultRequestProcessor implements RangerAccessRequestProcessor {

    protected final PolicyEngine policyEngine;

    public RangerDefaultRequestProcessor(PolicyEngine policyEngine) {
        this.policyEngine = policyEngine;
    }

    @Override
    public void preProcess(RangerAccessRequest request) {

        setResourceServiceDef(request);
        if (request instanceof RangerAccessRequestImpl) {
            RangerAccessRequestImpl reqImpl = (RangerAccessRequestImpl) request;

            if (reqImpl.getClientIPAddress() == null) {
                reqImpl.extractAndSetClientIPAddress(policyEngine.getUseForwardedIPAddress(), policyEngine.getTrustedProxyAddresses());
            }

            if(policyEngine.getPluginContext() != null) {
                if (reqImpl.getClusterName() == null) {
                    reqImpl.setClusterName(policyEngine.getPluginContext().getClusterName());
                }

                if (reqImpl.getClusterType() == null) {
                    reqImpl.setClusterType(policyEngine.getPluginContext().getClusterType());
                }
            }
        }

        RangerAccessRequestUtil.setCurrentUserInContext(request.getContext(), request.getUser());

        String owner = request.getResource() != null ? request.getResource().getOwnerUser() : null;

        if (StringUtils.isNotEmpty(owner)) {
            RangerAccessRequestUtil.setOwnerInContext(request.getContext(), owner);
        }

        Set<String> roles = request.getUserRoles();
        if (CollectionUtils.isEmpty(roles)) {
            roles = policyEngine.getPluginContext().getAuthContext().getRolesForUserAndGroups(request.getUser(), request.getUserGroups());
        }

        if (CollectionUtils.isNotEmpty(roles)) {
            RangerAccessRequestUtil.setCurrentUserRolesInContext(request.getContext(), roles);
        }

        enrich(request);
    }

    @Override
    public void enrich(RangerAccessRequest request) {
        List<RangerContextEnricher> enrichers = policyEngine.getAllContextEnrichers();

        if (!CollectionUtils.isEmpty(enrichers)) {
            for(RangerContextEnricher enricher : enrichers) {
                enricher.enrich(request);
            }
        }
    }

    private void setResourceServiceDef(RangerAccessRequest request) {
        RangerAccessResource resource = request.getResource();

        if (resource.getServiceDef() == null) {
            if (resource instanceof RangerMutableResource) {
                RangerMutableResource mutable = (RangerMutableResource) resource;
                mutable.setServiceDef(policyEngine.getServiceDef());
            }
        }
    }

}
