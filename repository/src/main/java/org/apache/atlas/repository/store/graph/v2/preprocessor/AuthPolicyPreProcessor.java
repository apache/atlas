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
package org.apache.atlas.repository.store.graph.v2.preprocessor;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.aliasstore.ESAliasStore;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.INSTANCE_GUID_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.UNAUTHORIZED_CONNECTION_ADMIN;
import static org.apache.atlas.authorize.AtlasAuthorizationUtils.getCurrentUserName;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ACTIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_GROUPS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ROLES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SERVICE_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SUB_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_USERS;
import static org.apache.atlas.repository.util.AccessControlUtils.CONN_NAME_PATTERN;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PERSONA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PURPOSE;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_ACCESS_CONTROL;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_POLICIES;
import static org.apache.atlas.repository.util.AccessControlUtils.getConnectionForPolicy;
import static org.apache.atlas.repository.util.AccessControlUtils.getEntityQualifiedName;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsAccessControlEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaRoleName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyActions;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyCategory;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyResourceCategory;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyServiceName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicySubCategory;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyType;
import static org.apache.atlas.repository.util.AccessControlUtils.getPurposeTags;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;

public class AuthPolicyPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AuthPolicyPreProcessor.class);

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private IndexAliasStore aliasStore;

    public AuthPolicyPreProcessor(AtlasGraph graph,
                                  AtlasTypeRegistry typeRegistry,
                                  EntityGraphRetriever entityRetriever) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;

        aliasStore = new ESAliasStore(graph, entityRetriever);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("AuthPolicyPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreatePolicy(entity);
                break;
            case UPDATE:
                processUpdatePolicy(entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreatePolicy(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreatePolicy");
        AtlasEntity policy = (AtlasEntity) entity;

        validatePolicyRequest(policy, null, CREATE);

        String policyCategory = getPolicyCategory(policy);

        if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
            AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
            AtlasEntity parentEntity = parent.getEntity();

            validatePersonaPolicyRequest(policy, null, parentEntity, CREATE);
            validateConnectionAdmin(policy);

            policy.setAttribute(QUALIFIED_NAME, String.format("%s/%s", getEntityQualifiedName(parentEntity), getUUID()));
            entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true);

            //extract role
            String roleName = getPersonaRoleName(parentEntity);
            List<String> roles = Arrays.asList(roleName);
            policy.setAttribute(ATTR_POLICY_ROLES, roles);

            policy.setAttribute(ATTR_POLICY_USERS, new ArrayList<>());
            policy.setAttribute(ATTR_POLICY_GROUPS, new ArrayList<>());


            //create ES alias
            aliasStore.updateAlias(parent, policy);

        } else if (POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {
            AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
            AtlasEntity parentEntity = parent.getEntity();

            policy.setAttribute(QUALIFIED_NAME, String.format("%s/%s", getEntityQualifiedName(parentEntity), getUUID()));
            entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true);

            validatePurposePolicyRequest(policy, null, parentEntity, CREATE);
            //extract tags
            List<String> purposeTags = getPurposeTags(parentEntity);

            List<String> policyResources = purposeTags.stream().map(x -> "tag:" + x).collect(Collectors.toList());

            policy.setAttribute(ATTR_POLICY_RESOURCES, policyResources);

            //create ES alias
            aliasStore.updateAlias(parent, policy);

        } else {
            if (CollectionUtils.isEmpty(getPolicyResources(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_RESOURCES);
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdatePolicy(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdatePolicy");
        AtlasEntity policy = (AtlasEntity) entity;
        AtlasEntity existingPolicy = entityRetriever.toAtlasEntityWithExtInfo(vertex).getEntity();

        validatePolicyRequest(policy, existingPolicy, UPDATE);

        String policyCategory = getPolicyCategory(policy);

        if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
            AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
            AtlasEntity parentEntity = parent.getEntity();

            validatePersonaPolicyRequest(policy, existingPolicy, parentEntity, UPDATE);
            validateConnectionAdmin(policy);

            String qName = getEntityQualifiedName(existingPolicy);
            policy.setAttribute(QUALIFIED_NAME, qName);

            //extract role
            String roleName = getPersonaRoleName(parentEntity);
            List<String> roles = Arrays.asList(roleName);

            policy.setAttribute(ATTR_POLICY_ROLES, roles);

            policy.setAttribute(ATTR_POLICY_USERS, new ArrayList<>());
            policy.setAttribute(ATTR_POLICY_GROUPS, new ArrayList<>());


            //create ES alias
            parent.addReferredEntity(policy);
            aliasStore.updateAlias(parent, null);

        } else if (POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {

            AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
            AtlasEntity parentEntity = parent.getEntity();

            validatePurposePolicyRequest(policy, existingPolicy, parentEntity, UPDATE);

            String qName = getEntityQualifiedName(existingPolicy);
            policy.setAttribute(QUALIFIED_NAME, qName);

            //extract tags
            List<String> purposeTags = getPurposeTags(parentEntity);

            List<String> policyResources = purposeTags.stream().map(x -> "tag:" + x).collect(Collectors.toList());

            policy.setAttribute(ATTR_POLICY_RESOURCES, policyResources);

            //create ES alias
            parent.addReferredEntity(policy);
            aliasStore.updateAlias(parent, null);


        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeletePolicy");

        try {
            AtlasEntity policy = entityRetriever.toAtlasEntity(vertex);

            if(!policy.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
                LOG.info("Policy with guid {} is already deleted/purged", policy.getGuid());
                return;
            }

            AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
            if (parent != null) {
                parent.getReferredEntity(policy.getGuid()).setStatus(AtlasEntity.Status.DELETED);
                aliasStore.updateAlias(parent, null);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void validateConnectionAdmin(AtlasEntity policy) throws AtlasBaseException {

        String subCategory = getPolicySubCategory(policy);
        if ("metadata".equals(subCategory) || "data".equals(subCategory)) {
            //connectionAdmins check

            List<String> resources = (List<String>) policy.getAttribute(ATTR_POLICY_RESOURCES);
            AtlasEntity connection = getConnectionForPolicy(entityRetriever, resources);
            String connectionRoleName = String.format(CONN_NAME_PATTERN, connection.getGuid());

            Set<String> userRoles = AtlasAuthorizationUtils.getRolesForCurrentUser();

            if (!userRoles.contains(connectionRoleName) || !userRoles.contains("$admin")) {
                throw new AtlasBaseException(UNAUTHORIZED_CONNECTION_ADMIN, getCurrentUserName(), connection.getGuid());
            }
        }
    }

    private AtlasEntityWithExtInfo getAccessControlEntity(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("AuthPolicyPreProcessor.getAccessControl");
        AtlasEntityWithExtInfo ret = null;

        AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
        if (objectId != null) {
            try {
                ret = entityRetriever.toAtlasEntityWithExtInfo(objectId);
            } catch (AtlasBaseException abe) {
                AtlasErrorCode code = abe.getAtlasErrorCode();

                if (INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND != code && INSTANCE_GUID_NOT_FOUND != code) {
                    throw abe;
                }
            }
        }

        if (ret != null) {
            List<AtlasObjectId> policies = (List<AtlasObjectId>) ret.getEntity().getRelationshipAttribute(REL_ATTR_POLICIES);

            for (AtlasObjectId policy : policies) {
                ret.addReferredEntity(entityRetriever.toAtlasEntity(policy));
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    private static void validatePolicyRequest(AtlasEntity policy, AtlasEntity existingPolicy, EntityOperation operation) throws AtlasBaseException {
        if (operation == CREATE) {

            if (StringUtils.isEmpty(getPolicyServiceName(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_SERVICE_NAME);
            }

            if (StringUtils.isEmpty(getPolicyCategory(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_CATEGORY);
            }

            if (StringUtils.isEmpty(getPolicyType(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_TYPE);
            }

            if (StringUtils.isEmpty(getPolicyResourceCategory(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_RESOURCES_CATEGORY);
            }

            if (CollectionUtils.isEmpty(getPolicyActions(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_ACTIONS);
            }
        } else {

            if (policy.hasAttribute(ATTR_POLICY_ACTIONS) && CollectionUtils.isEmpty(getPolicyActions(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_ACTIONS);
            }

            String newCategory = getPolicyCategory(policy);
            if (StringUtils.isNotEmpty(newCategory) && !newCategory.equals(getPolicyCategory(existingPolicy))) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, ATTR_POLICY_CATEGORY + " change not Allowed");
            }

            String newServiceName = getPolicyServiceName(policy);
            if (StringUtils.isNotEmpty(newServiceName) && !newServiceName.equals(getPolicyServiceName(existingPolicy))) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, ATTR_POLICY_SERVICE_NAME + " change not Allowed");
            }

            String newResourceCategory = getPolicyResourceCategory(policy);
            if (StringUtils.isNotEmpty(newResourceCategory) && !newResourceCategory.equals(getPolicyResourceCategory(existingPolicy))) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, ATTR_POLICY_RESOURCES_CATEGORY + " change not Allowed");
            }
        }
    }

    private static void validatePurposePolicyRequest(AtlasEntity policy, AtlasEntity existingPolicy, AtlasEntity purpose, EntityOperation operation) throws AtlasBaseException {

        boolean isParentEnabled = getIsAccessControlEnabled(purpose);
        if (!isParentEnabled) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy parent (accesscontrol) is disabled");
        }

        if (operation == CREATE) {
            if (!AtlasEntity.Status.ACTIVE.equals(purpose.getStatus())) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Purpose is not Active");
            }

            if (StringUtils.isEmpty(getPolicySubCategory(policy))) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Please provide attribute " + ATTR_POLICY_SUB_CATEGORY);
            }

            if (!PURPOSE_ENTITY_TYPE.equals(purpose.getTypeName())) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide Purpose as accesscontrol");
            }
        } else {
            if (!AtlasEntity.Status.ACTIVE.equals(existingPolicy.getStatus())) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy is not Active");
            }

            String newSubCategory = getPolicySubCategory(policy);
            if (StringUtils.isNotEmpty(newSubCategory) && !newSubCategory.equals(getPolicySubCategory(existingPolicy))) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy sub category change not Allowed");
            }

            validateParentUpdate(policy, existingPolicy);
        }
    }

    private static void validatePersonaPolicyRequest(AtlasEntity policy, AtlasEntity existingPolicy, AtlasEntity persona, EntityOperation operation) throws AtlasBaseException {
        boolean isParentEnabled = getIsAccessControlEnabled(persona);
        if (!isParentEnabled) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy parent (accesscontrol) is disabled");
        }

        if (operation == CREATE) {
            if (!AtlasEntity.Status.ACTIVE.equals(persona.getStatus())) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Persona is not Active");
            }

            if (StringUtils.isEmpty(getPolicySubCategory(policy))) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Please provide attribute " + ATTR_POLICY_SUB_CATEGORY);
            }

            if (!PERSONA_ENTITY_TYPE.equals(persona.getTypeName())) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide Persona as accesscontrol");
            }

            if (CollectionUtils.isEmpty(getPolicyResources(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_RESOURCES);
            }
        } else {

            if (policy.hasAttribute(ATTR_POLICY_RESOURCES) && CollectionUtils.isEmpty(getPolicyResources(policy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_RESOURCES);
            }

            if (!AtlasEntity.Status.ACTIVE.equals(existingPolicy.getStatus())) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy is not Active");
            }

            String newSubCategory = getPolicySubCategory(policy);
            if (StringUtils.isNotEmpty(newSubCategory) && !newSubCategory.equals(getPolicySubCategory(existingPolicy))) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy sub category change not Allowed");
            }

            validateParentUpdate(policy, existingPolicy);
        }
    }

    private static void validateParentUpdate(AtlasEntity policy, AtlasEntity existingPolicy) throws AtlasBaseException {

        Object object = policy.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
        if (object != null) {
            AtlasObjectId atlasObjectId = (AtlasObjectId) object;
            String newParentGuid = atlasObjectId.getGuid();

            object = existingPolicy.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
            if (object != null) {
                atlasObjectId = (AtlasObjectId) object;
                String existingParentGuid = atlasObjectId.getGuid();
                if (!newParentGuid.equals(existingParentGuid)) {
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy parent (accesscontrol) change is not Allowed");
                }
            }
        }
    }
}
