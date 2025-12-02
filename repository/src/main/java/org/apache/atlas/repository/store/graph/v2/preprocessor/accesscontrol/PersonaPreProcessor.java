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
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.auth.client.keycloak.AtlasKeycloakClient;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.aliasstore.ESAliasStore;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.keycloak.representations.idm.RoleRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
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

public class PersonaPreProcessor extends AccessControlPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PersonaPreProcessor.class);

    protected final AtlasGraph graph;
    protected AtlasTypeRegistry typeRegistry;
    protected final EntityGraphRetriever entityRetriever;
    protected IndexAliasStore aliasStore;
    protected AtlasEntityStore entityStore;
    protected KeycloakStore keycloakStore;

    public PersonaPreProcessor(AtlasGraph graph,
                               AtlasTypeRegistry typeRegistry,
                               EntityGraphRetriever entityRetriever,
                               AtlasEntityStore entityStore) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;

        aliasStore = new ESAliasStore(graph, entityRetriever);
        keycloakStore = new KeycloakStore(true, true);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PersonaPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }
        super.processAttributes(entityStruct, context, operation);

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreatePersona(entity);
                break;
            case UPDATE:
                processUpdatePersona(context, entity);
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

    private void processCreatePersona(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreatePersona");

        validateNoPoliciesAttached((AtlasEntity) entity);

        String tenantId = getTenantId(entity);

        entity.setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
        entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, entity.getAttributes().getOrDefault(ATTR_ACCESS_CONTROL_ENABLED, true));

        //create keycloak role
        String roleId = createKeycloakRole((AtlasEntity) entity);

        entity.setAttribute(ATTR_PERSONA_ROLE_ID, roleId);

        //create ES alias
        aliasStore.createAlias((AtlasEntity) entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdatePersona(EntityMutationContext context, AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdatePersona");

        AtlasEntity persona = (AtlasEntity) entity;
        validateNoPoliciesAttached(persona);
        AtlasVertex vertex = context.getVertex(persona.getGuid());

        AtlasEntity existingPersonaEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex).getEntity();

        if (!AtlasEntity.Status.ACTIVE.equals(existingPersonaEntity.getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Persona not Active");
        }

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        entity.setAttribute(QUALIFIED_NAME, vertexQName);
        entity.setAttribute(ATTR_PERSONA_ROLE_ID, getPersonaRoleId(existingPersonaEntity));

        boolean isEnabled = getIsAccessControlEnabled(persona);
        if (getIsAccessControlEnabled(existingPersonaEntity) != isEnabled) {
            updatePoliciesIsEnabledAttr(context, existingPersonaEntity, isEnabled);
        }

        updateKeycloakRole(persona, existingPersonaEntity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void updatePoliciesIsEnabledAttr(EntityMutationContext context, AtlasEntity existingPersonaEntity,
                                             boolean enable) throws AtlasBaseException {

        List<AtlasRelatedObjectId> policies = (List<AtlasRelatedObjectId>) existingPersonaEntity.getRelationshipAttribute(REL_ATTR_POLICIES);

        if (CollectionUtils.isNotEmpty(policies)) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(POLICY_ENTITY_TYPE);

            for (AtlasRelatedObjectId policy : policies) {
                if (policy.getRelationshipStatus() == AtlasRelationship.Status.ACTIVE) {
                    AtlasVertex policyVertex = entityRetriever.getEntityVertex(policy.getGuid());

                    if (AtlasGraphUtilsV2.getState(policyVertex) == AtlasEntity.Status.ACTIVE) {
                        AtlasEntity policyToBeUpdated = entityRetriever.toAtlasEntity(policyVertex);
                        policyToBeUpdated.setAttribute(ATTR_POLICY_IS_ENABLED, enable);

                        context.addUpdated(policyToBeUpdated.getGuid(), policyToBeUpdated, entityType, policyVertex);
                        RequestContext.get().cacheDifferentialEntity(policyToBeUpdated, policyVertex);
                    }
                }
            }
        }
    }

    protected String createKeycloakRole(AtlasEntity entity) throws AtlasBaseException {
        String roleName = getPersonaRoleName(entity);
        List<String> users = getPersonaUsers(entity);
        List<String> groups = getPersonaGroups(entity);

        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put("name", Collections.singletonList(roleName));
        attributes.put("type", Collections.singletonList("persona"));
        attributes.put("level", Collections.singletonList("workspace"));
        attributes.put("createdAt", Collections.singletonList(String.valueOf(System.currentTimeMillis())));
        attributes.put("createdBy", Collections.singletonList(RequestContext.get().getUser()));
        attributes.put("enabled", Collections.singletonList(String.valueOf(true)));
        attributes.put("displayName", Collections.singletonList(getEntityName(entity)));

        RoleRepresentation role = keycloakStore.createRole(roleName, users, groups, null);

        return role.getId();
    }

    protected void updateKeycloakRole(AtlasEntity newPersona, AtlasEntity existingPersona) throws AtlasBaseException {
        String roleId = getPersonaRoleId(existingPersona);
        String roleName = getPersonaRoleName(existingPersona);

        RoleRepresentation roleRepresentation = AtlasKeycloakClient.getKeycloakClient().getRoleById(roleId);

        boolean isUpdated = false;
        if (newPersona.hasAttribute(ATTR_PERSONA_USERS)) {
            List<String> newUsers       = getPersonaUsers(newPersona);
            List<String> existingUsers  = getPersonaUsers(existingPersona);

            keycloakStore.updateRoleUsers(roleName, existingUsers, newUsers, roleRepresentation);
            isUpdated = true;
        }

        if (newPersona.hasAttribute(ATTR_PERSONA_GROUPS)) {
            List<String> newGroups = getPersonaGroups(newPersona);
            List<String> existingGroups = getPersonaGroups(existingPersona);

            keycloakStore.updateRoleGroups(roleName, existingGroups, newGroups, roleRepresentation);
            isUpdated = true;
        }

        if (isUpdated) {
            Map<String, List<String>> attributes = new HashMap<>();
            attributes.put("updatedAt", Collections.singletonList(String.valueOf(System.currentTimeMillis())));
            attributes.put("updatedBy", Collections.singletonList(RequestContext.get().getUser()));
            attributes.put("enabled", Collections.singletonList(String.valueOf(true)));
            attributes.put("displayName", Collections.singletonList(getEntityName(newPersona)));

            roleRepresentation.setAttributes(attributes);
            AtlasKeycloakClient.getKeycloakClient().updateRole(roleId, roleRepresentation);
            LOG.info("Updated keycloak role with name {}", roleName);
        }
    }
}
