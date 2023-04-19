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


import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.keycloak.client.KeycloakClient;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.transformer.PreProcessorPoliciesTransformer;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.keycloak.admin.client.resource.RoleResource;
import org.keycloak.representations.idm.RoleRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.ATTR_ADMIN_GROUPS;
import static org.apache.atlas.repository.Constants.ATTR_ADMIN_ROLES;
import static org.apache.atlas.repository.Constants.ATTR_ADMIN_USERS;
import static org.apache.atlas.repository.Constants.CREATED_BY_KEY;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class ConnectionPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPreProcessor.class);

    public static final String CONN_NAME_PATTERN = "connection_admins_%s";

    private final AtlasGraph graph;
    private final EntityGraphRetriever entityRetriever;
    private AtlasEntityStore entityStore;
    private EntityDiscoveryService discovery;
    private PreProcessorPoliciesTransformer transformer;
    private KeycloakStore keycloakStore;

    public ConnectionPreProcessor(AtlasGraph graph,
                                  EntityDiscoveryService discovery,
                                  EntityGraphRetriever entityRetriever,
                                  AtlasEntityStore entityStore) {
        this.graph = graph;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;
        this.discovery = discovery;

        transformer = new PreProcessorPoliciesTransformer();
        keycloakStore = new KeycloakStore();
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PurposePreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateConnection(entity);
                break;
            case UPDATE:
                processUpdateConnection(context, entity);
                break;
        }
    }

    private void processCreateConnection(AtlasStruct struct) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateConnection");

        AtlasEntity connection = (AtlasEntity) struct;

        //create connection role
        String roleName = String.format(CONN_NAME_PATTERN, connection.getGuid());

        List<String> adminUsers = (List<String>) connection.getAttribute(ATTR_ADMIN_USERS);
        List<String> adminGroups = (List<String>) connection.getAttribute(ATTR_ADMIN_GROUPS);
        List<String> adminRoles = (List<String>) connection.getAttribute(ATTR_ADMIN_ROLES);

        if (adminUsers == null) {
            adminUsers = new ArrayList<>();
        }

        String creatorUser = RequestContext.get().getUser();
        if(StringUtils.isNotEmpty(creatorUser) && !adminUsers.contains(creatorUser)) {
            adminUsers.add(creatorUser);
        }
        connection.setAttribute(ATTR_ADMIN_USERS, adminUsers);

        RoleRepresentation role = keycloakStore.createRoleForConnection(roleName, true, adminUsers, adminGroups, adminRoles);

        //create connection bootstrap policies
        AtlasEntitiesWithExtInfo policies = transformer.transform(connection);

        try {
            RequestContext.get().setPoliciesBootstrappingInProgress(true);
            EntityStream entityStream = new AtlasEntityStream(policies);
            entityStore.createOrUpdate(entityStream, false);
            LOG.info("Created bootstrap policies for connection");
        } finally {
            RequestContext.get().setPoliciesBootstrappingInProgress(false);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateConnection(EntityMutationContext context,
                                      AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateConnection");

        AtlasEntity connection = (AtlasEntity) entity;

        AtlasVertex vertex = context.getVertex(connection.getGuid());
        AtlasEntity existingConnEntity = entityRetriever.toAtlasEntity(vertex);

        String roleName = String.format(CONN_NAME_PATTERN, connection.getGuid());

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        entity.setAttribute(QUALIFIED_NAME, vertexQName);

        RoleResource rolesResource = KeycloakClient.getKeycloakClient().getRealm().roles().get(roleName);
        RoleRepresentation representation = rolesResource.toRepresentation();
        String creatorUser = vertex.getProperty(CREATED_BY_KEY, String.class);

        if (connection.hasAttribute(ATTR_ADMIN_USERS)) {
            List<String> newAdminUsers = (List<String>) connection.getAttribute(ATTR_ADMIN_USERS);
            List<String> currentAdminUsers = (List<String>) existingConnEntity.getAttribute(ATTR_ADMIN_USERS);
            if (StringUtils.isNotEmpty(creatorUser) && !newAdminUsers.contains(creatorUser)) {
                newAdminUsers.add(creatorUser);
            }

            connection.setAttribute(ATTR_ADMIN_USERS, newAdminUsers);
            if (CollectionUtils.isNotEmpty(newAdminUsers) || CollectionUtils.isNotEmpty(currentAdminUsers)) {
                keycloakStore.updateRoleUsers(roleName, currentAdminUsers, newAdminUsers, representation);
            }
        }

        if (connection.hasAttribute(ATTR_ADMIN_GROUPS)) {
            List<String> newAdminGroups = (List<String>) connection.getAttribute(ATTR_ADMIN_GROUPS);
            List<String> currentAdminGroups =(List<String>)  existingConnEntity.getAttribute(ATTR_ADMIN_GROUPS);

            if (CollectionUtils.isNotEmpty(newAdminGroups) || CollectionUtils.isNotEmpty(currentAdminGroups)) {
                keycloakStore.updateRoleGroups(roleName, currentAdminGroups, newAdminGroups, representation);
            }
        }

        if (connection.hasAttribute(ATTR_ADMIN_ROLES)) {
            List<String> newAdminRoles = (List<String>) connection.getAttribute(ATTR_ADMIN_ROLES);
            List<String> currentAdminRoles = (List<String>) existingConnEntity.getAttribute(ATTR_ADMIN_ROLES);

            if (CollectionUtils.isNotEmpty(newAdminRoles) || CollectionUtils.isNotEmpty(currentAdminRoles)) {
                keycloakStore.updateRoleRoles(roleName, currentAdminRoles, newAdminRoles, rolesResource, representation);
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);
        AtlasEntity connection = entityWithExtInfo.getEntity();
        String roleName = String.format(CONN_NAME_PATTERN, connection.getGuid());

        if (!AtlasEntity.Status.ACTIVE.equals(connection.getStatus())) {
            throw new AtlasBaseException("Connection is already deleted/purged");
        }

        //delete connection policies
        List<AtlasEntityHeader> policies = getConnectionPolicies(connection.getGuid(), roleName);
        EntityMutationResponse response = entityStore.deleteByIds(policies.stream().map(x -> x.getGuid()).collect(Collectors.toList()));

        //delete connection role
        keycloakStore.removeRoleByName(roleName);
    }

    private List<AtlasEntityHeader> getConnectionPolicies(String guid, String roleName) throws AtlasBaseException {
        List<AtlasEntityHeader> ret = new ArrayList<>();
        
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = new HashMap<>();

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", POLICY_ENTITY_TYPE)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));


        mustClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, guid + "/*")));
        mustClauseList.add(mapOf("term", mapOf("policyRoles", roleName)));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);

        AtlasSearchResult result = discovery.directIndexSearch(indexSearchParams);
        if (result != null) {
            ret = result.getEntities();
        }

        return ret;
    }
}
