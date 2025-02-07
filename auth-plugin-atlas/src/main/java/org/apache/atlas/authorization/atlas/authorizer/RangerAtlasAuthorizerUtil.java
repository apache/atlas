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

package org.apache.atlas.authorization.atlas.authorizer;

import org.apache.atlas.authorize.AtlasAccessorResponse;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.atlas.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.atlas.plugin.policyengine.RangerAccessResult;
import org.apache.atlas.plugin.policyevaluator.RangerPolicyItemEvaluator;

import java.util.Set;

import static org.apache.atlas.authorization.atlas.authorizer.RangerAtlasAuthorizer.CLASSIFICATION_PRIVILEGES;
import static org.apache.atlas.authorization.utils.RangerAtlasConstants.RESOURCE_CLASSIFICATION;
import static org.apache.atlas.authorization.utils.RangerAtlasConstants.RESOURCE_ENTITY_BUSINESS_METADATA;
import static org.apache.atlas.authorization.utils.RangerAtlasConstants.RESOURCE_ENTITY_ID;
import static org.apache.atlas.authorization.utils.RangerAtlasConstants.RESOURCE_ENTITY_LABEL;
import static org.apache.atlas.authorization.utils.RangerAtlasConstants.RESOURCE_ENTITY_OWNER;
import static org.apache.atlas.authorization.utils.RangerAtlasConstants.RESOURCE_ENTITY_TYPE;


public class RangerAtlasAuthorizerUtil {

    static void toRangerRequest(AtlasEntityAccessRequest request, RangerAccessRequestImpl rangerRequest, RangerAccessResourceImpl rangerResource){

        final String                   action         = request.getAction() != null ? request.getAction().getType() : null;
        final Set<String> entityTypes                 = request.getEntityTypeAndAllSuperTypes();
        final String                   entityId       = request.getEntityId();
        final String                   classification = request.getClassification() != null ? request.getClassification().getTypeName() : null;
        final String                   ownerUser      = request.getEntity() != null ? (String) request.getEntity().getAttribute(RESOURCE_ENTITY_OWNER) : null;

        rangerResource.setValue(RESOURCE_ENTITY_TYPE, entityTypes);
        rangerResource.setValue(RESOURCE_ENTITY_ID, entityId);
        rangerResource.setOwnerUser(ownerUser);
        rangerRequest.setAccessType(action);
        rangerRequest.setAction(action);
        rangerRequest.setUser(request.getUser());
        rangerRequest.setUserGroups(request.getUserGroups());
        rangerRequest.setClientIPAddress(request.getClientIPAddress());
        rangerRequest.setAccessTime(request.getAccessTime());
        rangerRequest.setResource(rangerResource);
        rangerRequest.setForwardedAddresses(request.getForwardedAddresses());
        rangerRequest.setRemoteIPAddress(request.getRemoteIPAddress());

        if (AtlasPrivilege.ENTITY_ADD_LABEL.equals(request.getAction()) || AtlasPrivilege.ENTITY_REMOVE_LABEL.equals(request.getAction())) {
            rangerResource.setValue(RESOURCE_ENTITY_LABEL, request.getLabel());
        } else if (AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA.equals(request.getAction())) {
            rangerResource.setValue(RESOURCE_ENTITY_BUSINESS_METADATA, request.getBusinessMetadata());
        } else if (StringUtils.isNotEmpty(classification) && CLASSIFICATION_PRIVILEGES.contains(request.getAction())) {
            rangerResource.setValue(RESOURCE_CLASSIFICATION, request.getClassificationTypeAndAllSuperTypes(classification));
        }
    }

    static void collectAccessors(RangerAccessResult result, AtlasAccessorResponse response) {
        if (result != null && CollectionUtils.isNotEmpty(result.getMatchedItemEvaluators())) {

            result.getMatchedItemEvaluators().forEach(x -> {
                collectSubjects(response, x);
            });
        }
    }

    static private void collectSubjects(AtlasAccessorResponse response, RangerPolicyItemEvaluator evaluator) {

        RangerPolicy.RangerPolicyItem policyItem = evaluator.getPolicyItem();

        if (evaluator.getPolicyItemType() == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW) {
            response.getUsers().addAll(policyItem.getUsers());
            response.getRoles().addAll(policyItem.getRoles());
            response.getGroups().addAll(policyItem.getGroups());

        } else if (evaluator.getPolicyItemType() == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY) {
            response.getDenyUsers().addAll(policyItem.getUsers());
            response.getDenyRoles().addAll(policyItem.getRoles());
            response.getDenyGroups().addAll(policyItem.getGroups());
        }
    }

    static void collectAccessors(RangerAccessResult resultEnd1, RangerAccessResult resultEnd2, AtlasAccessorResponse accessorResponse) {

        if (resultEnd2 == null || CollectionUtils.isEmpty(resultEnd2.getMatchedItemEvaluators()))  {
            return;
        }

        final AtlasAccessorResponse accessorsEnd1 = new AtlasAccessorResponse();
        final AtlasAccessorResponse accessorsEnd2 = new AtlasAccessorResponse();

        // Collect lists of accessors for both results
        resultEnd1.getMatchedItemEvaluators().forEach(x -> {
            collectSubjects(accessorsEnd1, x);
        });

        resultEnd2.getMatchedItemEvaluators().forEach(x -> {
            collectSubjects(accessorsEnd2, x);
        });

        // Retain only common accessors
        accessorsEnd1.getUsers().retainAll(accessorsEnd2.getUsers());
        accessorsEnd1.getRoles().retainAll(accessorsEnd2.getRoles());
        accessorsEnd1.getGroups().retainAll(accessorsEnd2.getGroups());

        accessorsEnd1.getDenyUsers().addAll(accessorsEnd2.getDenyUsers());
        accessorsEnd1.getDenyRoles().addAll(accessorsEnd2.getDenyRoles());
        accessorsEnd1.getDenyGroups().addAll(accessorsEnd2.getDenyGroups());

        // add accessors to the response
        accessorResponse.getUsers().addAll(accessorsEnd1.getUsers());
        accessorResponse.getRoles().addAll(accessorsEnd1.getRoles());
        accessorResponse.getGroups().addAll(accessorsEnd1.getGroups());

        accessorResponse.getDenyUsers().addAll(accessorsEnd1.getDenyUsers());
        accessorResponse.getDenyRoles().addAll(accessorsEnd1.getDenyRoles());
        accessorResponse.getDenyGroups().addAll(accessorsEnd1.getDenyGroups());
    }

    static boolean hasAccessors(RangerAccessResult result) {
        if (result == null) {
            return false;
        }

        for (RangerPolicyItemEvaluator itemEvaluator : result.getMatchedItemEvaluators()) {
            RangerPolicy.RangerPolicyItem item = itemEvaluator.getPolicyItem();
            if (CollectionUtils.isNotEmpty(item.getUsers()) || CollectionUtils.isNotEmpty(item.getRoles()) && CollectionUtils.isNotEmpty(item.getGroups())) {
                return true;
            }
        }
        return false;
    }
}
