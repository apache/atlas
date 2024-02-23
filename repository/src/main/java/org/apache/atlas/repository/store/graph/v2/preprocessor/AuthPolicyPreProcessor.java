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
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.INSTANCE_GUID_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.RESOURCE_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.UNAUTHORIZED_CONNECTION_ADMIN;
import static org.apache.atlas.authorizer.AtlasAuthorizationUtils.getCurrentUserName;
import static org.apache.atlas.authorizer.AtlasAuthorizationUtils.verifyAccess;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.repository.Constants.ATTR_ADMIN_ROLES;
import static org.apache.atlas.repository.Constants.KEYCLOAK_ROLE_ADMIN;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STAKEHOLDER_ENTITY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.*;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicySubCategory;

public class AuthPolicyPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AuthPolicyPreProcessor.class);
    public static final String ENTITY_DEFAULT_DOMAIN_SUPER = "entity:default/domain/*/super";

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

        AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
        AtlasEntity parentEntity = null;
        if (parent != null) {
            parentEntity = parent.getEntity();
            verifyParentTypeName(parentEntity);
        }

        String policyCategory = getPolicyCategory(policy);
        if (StringUtils.isEmpty(policyCategory)) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_CATEGORY);
        }

        entity.setAttribute(ATTR_POLICY_IS_ENABLED, entity.getAttributes().getOrDefault(ATTR_POLICY_IS_ENABLED, true));

        AuthPolicyValidator validator = new AuthPolicyValidator(entityRetriever);
        if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
            String policySubCategory = getPolicySubCategory(policy);

            if (!POLICY_SUB_CATEGORY_DOMAIN.equals(policySubCategory)) {
                validator.validate(policy, null, parentEntity, CREATE);
                validateConnectionAdmin(policy);
            } else {
                validateAndReduce(policy);
            }

            policy.setAttribute(QUALIFIED_NAME, String.format("%s/%s", getEntityQualifiedName(parentEntity), getUUID()));

            //extract role
            String roleName = getPersonaRoleName(parentEntity);
            List<String> roles = Arrays.asList(roleName);
            policy.setAttribute(ATTR_POLICY_ROLES, roles);

            policy.setAttribute(ATTR_POLICY_USERS, new ArrayList<>());
            policy.setAttribute(ATTR_POLICY_GROUPS, new ArrayList<>());

            if(parentEntity != null) {
                policy.setAttribute(ATTR_POLICY_IS_ENABLED, getIsAccessControlEnabled(parentEntity));
            }

            //create ES alias
            aliasStore.updateAlias(parent, policy);

        } else if (POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {
            policy.setAttribute(QUALIFIED_NAME, String.format("%s/%s", getEntityQualifiedName(parentEntity), getUUID()));

            validator.validate(policy, null, parentEntity, CREATE);

            //extract tags
            List<String> purposeTags = getPurposeTags(parentEntity);

            List<String> policyResources = purposeTags.stream().map(x -> "tag:" + x).collect(Collectors.toList());

            policy.setAttribute(ATTR_POLICY_RESOURCES, policyResources);

            if(parentEntity != null) {
                policy.setAttribute(ATTR_POLICY_IS_ENABLED, getIsAccessControlEnabled(parentEntity));
            }

            //create ES alias
            aliasStore.updateAlias(parent, policy);

        } else {
            validator.validate(policy, null, null, CREATE);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }


    private void validateAndReduce(AtlasEntity policy) {
        List<String> resources = (List<String>) policy.getAttribute(ATTR_POLICY_RESOURCES);
        boolean hasAllDomainPattern = resources.stream().anyMatch(resource ->
                resource.equals("entity:*") ||
                        resource.equals("entity:*/super") ||
                        resource.equals(ENTITY_DEFAULT_DOMAIN_SUPER)
        );

        if (hasAllDomainPattern) {
            policy.setAttribute(ATTR_POLICY_RESOURCES, Collections.singletonList(ENTITY_DEFAULT_DOMAIN_SUPER));
        }
    }


    private void processUpdatePolicy(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdatePolicy");
        AtlasEntity policy = (AtlasEntity) entity;
        AtlasEntity existingPolicy = entityRetriever.toAtlasEntityWithExtInfo(vertex).getEntity();

        String policyCategory = policy.hasAttribute(ATTR_POLICY_CATEGORY) ? getPolicyCategory(policy) : getPolicyCategory(existingPolicy);

        AuthPolicyValidator validator = new AuthPolicyValidator(entityRetriever);
        if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
            AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
            AtlasEntity parentEntity = parent.getEntity();

            String policySubCategory = getPolicySubCategory(policy);

            if (!POLICY_SUB_CATEGORY_DOMAIN.equals(policySubCategory)) {
                validator.validate(policy, existingPolicy, parentEntity, UPDATE);
                validateConnectionAdmin(policy);
            } else {
                validateAndReduce(policy);
            }

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

            validator.validate(policy, existingPolicy, parentEntity, UPDATE);

            String qName = getEntityQualifiedName(existingPolicy);
            policy.setAttribute(QUALIFIED_NAME, qName);

            //extract tags
            List<String> purposeTags = getPurposeTags(parentEntity);

            List<String> policyResources = purposeTags.stream().map(x -> "tag:" + x).collect(Collectors.toList());

            policy.setAttribute(ATTR_POLICY_RESOURCES, policyResources);

            //create ES alias
            parent.addReferredEntity(policy);

        } else if (POLICY_CATEGORY_DATAMESH.equals(policyCategory)) {
            validator.validate(policy, existingPolicy, null, UPDATE);
        } else {
            validator.validate(policy, null, null, UPDATE);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeletePolicy");

        try {
            AtlasEntity policy = entityRetriever.toAtlasEntity(vertex);

            authorizeDeleteAuthPolicy(policy);

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

    private void authorizeDeleteAuthPolicy(AtlasEntity policy) throws AtlasBaseException {
        if (!RequestContext.get().isSkipAuthorizationCheck()) {
            AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, new AtlasEntityHeader(policy));
            verifyAccess(request, "delete entity: guid=" + policy.getGuid());
        }
        /* else,
        * skip auth check
        * */
    }

    private void validateConnectionAdmin(AtlasEntity policy) throws AtlasBaseException {
        String subCategory = getPolicySubCategory(policy);
        if (POLICY_SUB_CATEGORY_METADATA.equals(subCategory) || POLICY_SUB_CATEGORY_DATA.equals(subCategory)) {
            //connectionAdmins check

            String connQn = getPolicyConnectionQN(policy);
            AtlasEntity connection = getEntityByQualifiedName(entityRetriever, connQn);
            if (connection == null) {
                throw new AtlasBaseException(RESOURCE_NOT_FOUND, "Connection entity for policy");
            }
            String connectionRoleName = String.format(CONN_NAME_PATTERN, connection.getGuid());

            Set<String> userRoles = AtlasAuthorizationUtils.getRolesForCurrentUser();

            List<String> connRoles = new ArrayList<>(0);
            if (connection.hasAttribute(ATTR_ADMIN_ROLES)) {
                connRoles = (List<String>) connection.getAttribute(ATTR_ADMIN_ROLES);
            }

            if (userRoles.contains(connectionRoleName) || (userRoles.contains(KEYCLOAK_ROLE_ADMIN) && connRoles.contains(KEYCLOAK_ROLE_ADMIN))) {
                //valid connection admin
            } else if (ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
                // Argo service user Valid Service user for connection admin while access control is disabled
                //TODO: Remove this once we complete migration to Atlas AuthZ based access control
            } else {
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

    private void verifyParentTypeName(AtlasEntity parentEntity) throws AtlasBaseException {
        if (parentEntity.getTypeName().equals(STAKEHOLDER_ENTITY_TYPE)) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Updating policies for " + STAKEHOLDER_ENTITY_TYPE);
        }
    }
}
