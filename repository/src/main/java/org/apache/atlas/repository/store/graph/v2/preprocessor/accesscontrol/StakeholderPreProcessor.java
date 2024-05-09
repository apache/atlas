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
package org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol;


import org.apache.atlas.RequestContext;
import org.apache.atlas.auth.client.keycloak.AtlasKeycloakClient;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.keycloak.representations.idm.RoleRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_GROUPS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_ROLE_ID;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_USERS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_IS_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_POLICIES;
import static org.apache.atlas.repository.util.AccessControlUtils.getESAliasName;
import static org.apache.atlas.repository.util.AccessControlUtils.getEntityName;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsAccessControlEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaGroups;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaRoleId;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaRoleName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaUsers;
import static org.apache.atlas.repository.util.AccessControlUtils.getTenantId;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;
import static org.apache.atlas.repository.util.AccessControlUtils.validateNoPoliciesAttached;

public class StakeholderPreProcessor extends PersonaPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StakeholderPreProcessor.class);

    public StakeholderPreProcessor(AtlasGraph graph,
                                   AtlasTypeRegistry typeRegistry,
                                   EntityGraphRetriever entityRetriever,
                                   AtlasEntityStore entityStore) {
        super(graph, typeRegistry, entityRetriever, entityStore);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("StakeholderPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateStakeholder(entity);
                break;
            case UPDATE:
                processUpdateStakeholder(context, entity);
                break;
        }
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);
        AtlasEntity persona = entityWithExtInfo.getEntity();

        if(!persona.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Persona with guid {} is already deleted/purged", persona.getGuid());
            return;
        }

        //delete policies
        List<AtlasObjectId> policies = (List<AtlasObjectId>) persona.getRelationshipAttribute(REL_ATTR_POLICIES);
        if (CollectionUtils.isNotEmpty(policies)) {
            for (AtlasObjectId policyObjectId : policies) {
                //AtlasVertex policyVertex = entityRetriever.getEntityVertex(policyObjectId.getGuid());
                entityStore.deleteById(policyObjectId.getGuid());
            }
        }

        //remove role
        keycloakStore.removeRole(getPersonaRoleId(persona));

        //delete ES alias
        aliasStore.deleteAlias(getESAliasName(persona));
    }

    private void processCreateStakeholder(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateStakeholder");

        validateNoPoliciesAttached(entity);

        if (!entity.hasRelationshipAttribute("stakeholderTitle") || !entity.hasRelationshipAttribute("dataDomain")) {
            throw new AtlasBaseException(BAD_REQUEST, "Relations stakeholderTitle and dataDomain are mandatory");
        }

        String domainQualifiedName = getQualifiedNameFromRelationAttribute(entity, "dataDomain");
        entity.setAttribute("domainQualifiedName", domainQualifiedName);
        entity.setAttribute("stakeholderTitleGuid", getGuidFromRelationAttribute(entity, "stakeholderTitle"));

        String personaQualifiedName = String.format("default/%s/%s",
                getUUID(),
                domainQualifiedName);

        entity.setAttribute(QUALIFIED_NAME, personaQualifiedName);

        //TODO: validate Stakeholder & StakeholderTitle pair is unique



        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                "create Stakeholder: ", entity.getAttribute(NAME));

        entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, entity.getAttributes().getOrDefault(ATTR_ACCESS_CONTROL_ENABLED, true));

        //create keycloak role
        String roleId = createKeycloakRole(entity);

        entity.setAttribute(ATTR_PERSONA_ROLE_ID, roleId);

        //create ES alias
        aliasStore.createAlias(entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateStakeholder(EntityMutationContext context, AtlasEntity stakeholder) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateStakeholder");

        validateNoPoliciesAttached(stakeholder);

        validateNoPoliciesAttached(stakeholder);
        AtlasVertex vertex = context.getVertex(stakeholder.getGuid());

        AtlasEntity existingStakeholderEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex).getEntity();

        if (!AtlasEntity.Status.ACTIVE.equals(existingStakeholderEntity.getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Stakeholder not Active");
        }

        String domainGuid = (String) stakeholder.getAttribute("domainGuid");
        if (StringUtils.isNotEmpty(domainGuid)) {
            stakeholder.removeAttribute("domainGuid");
            stakeholder.removeAttribute("stakeholderTitleGuid");
            stakeholder.getRelationshipAttributes().remove("dataDomain");
            stakeholder.getRelationshipAttributes().remove("stakeholderTitle");
        }

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        stakeholder.setAttribute(QUALIFIED_NAME, vertexQName);
        stakeholder.setAttribute(ATTR_PERSONA_ROLE_ID, getPersonaRoleId(existingStakeholderEntity));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, new AtlasEntityHeader(stakeholder)),
                "update Stakeholder: ", stakeholder.getAttribute(NAME));

        updateKeycloakRole(stakeholder, existingStakeholderEntity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private String getGuidFromRelationAttribute(AtlasEntity entity, String relationshipAttributeName) throws AtlasBaseException {
        AtlasObjectId relationObjectId = (AtlasObjectId) entity.getRelationshipAttribute(relationshipAttributeName);

        String guid = relationObjectId.getGuid();
        if (StringUtils.isEmpty(guid)) {
            AtlasVertex vertex = entityRetriever.getEntityVertex(relationObjectId);
            guid = vertex.getProperty("__guid", String.class);
        }

        return guid;
    }

    private String getQualifiedNameFromRelationAttribute(AtlasEntity entity, String relationshipAttributeName) throws AtlasBaseException {
        AtlasObjectId relationObjectId = (AtlasObjectId) entity.getRelationshipAttribute(relationshipAttributeName);
        String qualifiedName = null;

        if (relationObjectId.getUniqueAttributes() != null) {
            qualifiedName = (String) relationObjectId.getUniqueAttributes().get(QUALIFIED_NAME);
        }

        if (StringUtils.isEmpty(qualifiedName)) {
            AtlasVertex vertex = entityRetriever.getEntityVertex(relationObjectId);
            qualifiedName = vertex.getProperty(QUALIFIED_NAME, String.class);
        }

        return qualifiedName;
    }
}
