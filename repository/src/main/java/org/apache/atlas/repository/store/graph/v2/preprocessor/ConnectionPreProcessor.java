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

import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.featureflag.FeatureFlagStore;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v1.SoftDeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.transformer.PreProcessorPoliciesTransformer;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.keycloak.representations.idm.RoleRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.authorize.AtlasAuthorizerFactory.ATLAS_AUTHORIZER_IMPL;
import static org.apache.atlas.authorize.AtlasAuthorizerFactory.CURRENT_AUTHORIZER_IMPL;
import static org.apache.atlas.keycloak.client.AtlasKeycloakClient.getKeycloakClient;
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
    private FeatureFlagStore featureFlagStore;
    private KeycloakStore keycloakStore;
    private final DeleteHandlerDelegate deleteDelegate;

    public ConnectionPreProcessor(AtlasGraph graph,
                                  EntityDiscoveryService discovery,
                                  EntityGraphRetriever entityRetriever,
                                  FeatureFlagStore featureFlagStore,
                                  DeleteHandlerDelegate deleteDelegate,
                                  AtlasEntityStore entityStore) {
        this.graph = graph;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;
        this.featureFlagStore = featureFlagStore;
        this.discovery = discovery;
        this.deleteDelegate = deleteDelegate;
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
        if (ATLAS_AUTHORIZER_IMPL.equalsIgnoreCase(CURRENT_AUTHORIZER_IMPL)) {
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
            if (StringUtils.isNotEmpty(creatorUser) && !adminUsers.contains(creatorUser)) {
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
                LOG.info("Created bootstrap policies for connection {}", connection.getAttribute(QUALIFIED_NAME));
            } finally {
                RequestContext.get().setPoliciesBootstrappingInProgress(false);
            }

            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processUpdateConnection(EntityMutationContext context,
                                      AtlasStruct entity) throws AtlasBaseException {

        AtlasEntity connection = (AtlasEntity) entity;

        if (ATLAS_AUTHORIZER_IMPL.equalsIgnoreCase(CURRENT_AUTHORIZER_IMPL)) {
            AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateConnection");

            AtlasVertex vertex = context.getVertex(connection.getGuid());
            AtlasEntity existingConnEntity = entityRetriever.toAtlasEntity(vertex);

            String roleName = String.format(CONN_NAME_PATTERN, connection.getGuid());

            String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
            entity.setAttribute(QUALIFIED_NAME, vertexQName);

            RoleRepresentation representation = getKeycloakClient().getRoleByName(roleName);
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
                List<String> currentAdminGroups = (List<String>) existingConnEntity.getAttribute(ATTR_ADMIN_GROUPS);

                if (CollectionUtils.isNotEmpty(newAdminGroups) || CollectionUtils.isNotEmpty(currentAdminGroups)) {
                    keycloakStore.updateRoleGroups(roleName, currentAdminGroups, newAdminGroups, representation);
                }
            }

            if (connection.hasAttribute(ATTR_ADMIN_ROLES)) {
                List<String> newAdminRoles = (List<String>) connection.getAttribute(ATTR_ADMIN_ROLES);
                List<String> currentAdminRoles = (List<String>) existingConnEntity.getAttribute(ATTR_ADMIN_ROLES);

                if (CollectionUtils.isNotEmpty(newAdminRoles) || CollectionUtils.isNotEmpty(currentAdminRoles)) {
                    keycloakStore.updateRoleRoles(roleName, currentAdminRoles, newAdminRoles, representation);
                }
            }

            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        // Process Delete connection role and policies in case of hard delete or purge
        if (isDeleteTypeSoft()) {
            LOG.info("Skipping processDelete for connection as delete type is {}", RequestContext.get().getDeleteType());
            return;
        }

        if (ATLAS_AUTHORIZER_IMPL.equalsIgnoreCase(CURRENT_AUTHORIZER_IMPL)) {
            AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);
            AtlasEntity connection = entityWithExtInfo.getEntity();
            String roleName = String.format(CONN_NAME_PATTERN, connection.getGuid());

            boolean connectionIsDeleted = AtlasEntity.Status.DELETED.equals(connection.getStatus());

            if (connectionIsDeleted && DeleteType.HARD.equals(RequestContext.get().getDeleteType())) {
                LOG.warn("Connection {} is SOFT DELETED! Can't HARD DELETE connection role and policies", connection.getAttribute(QUALIFIED_NAME));
                return;
            }

            //delete connection policies
            List<AtlasEntityHeader> policies = getConnectionPolicies(connection.getGuid(), roleName);
            entityStore.deleteByIds(policies.stream().map(x -> x.getGuid()).collect(Collectors.toList()));

            keycloakStore.removeRoleByName(roleName);
        }
    }

    private boolean isDeleteTypeSoft() {
        return deleteDelegate.getHandler().getClass().equals(SoftDeleteHandlerV1.class);
    }

    private List<AtlasEntityHeader> getConnectionPolicies(String guid, String roleName) throws AtlasBaseException {
        List<AtlasEntityHeader> ret = new ArrayList<>();
        
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = new HashMap<>();

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", POLICY_ENTITY_TYPE)));
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
