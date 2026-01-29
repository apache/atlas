/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.AtlasErrorCode;
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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.atlas.auth.client.keycloak.AtlasKeycloakClient.getKeycloakClient;
import static org.apache.atlas.authorize.AtlasAuthorizerFactory.ATLAS_AUTHORIZER_IMPL;
import static org.apache.atlas.authorize.AtlasAuthorizerFactory.CURRENT_AUTHORIZER_IMPL;
import static org.apache.atlas.repository.Constants.*;
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
                RequestContext.get().setSkipAuthorizationCheck(true);
                EntityStream entityStream = new AtlasEntityStream(policies);
                entityStore.createOrUpdate(entityStream, false);
                LOG.info("Created bootstrap policies for connection {}", connection.getAttribute(QUALIFIED_NAME));
            } finally {
                RequestContext.get().setSkipAuthorizationCheck(false);
            }

            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processUpdateConnection(EntityMutationContext context, AtlasStruct entity) throws AtlasBaseException {
        AtlasEntity connection = (AtlasEntity) entity;
        if (ATLAS_AUTHORIZER_IMPL.equalsIgnoreCase(CURRENT_AUTHORIZER_IMPL)) {
            AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateConnection");
            AtlasVertex vertex = context.getVertex(connection.getGuid());
            AtlasEntity existingConnEntity = entityRetriever.toAtlasEntity(vertex);
            String roleName = String.format(CONN_NAME_PATTERN, connection.getGuid());
            String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
            entity.setAttribute(QUALIFIED_NAME, vertexQName);

            //optional is used here to distinguish if the admin related attributes are set in request body or not (else part)
            //if set, check for empty list so that appropriate error can be thrown
            List<String> newAdminUsers = getAttributeList(connection, ATTR_ADMIN_USERS).orElse(null);
            List<String> currentAdminUsers = getAttributeList(existingConnEntity, ATTR_ADMIN_USERS).orElseGet(ArrayList::new);

            List<String> newAdminGroups = getAttributeList(connection, ATTR_ADMIN_GROUPS).orElse(null);
            List<String> currentAdminGroups = getAttributeList(existingConnEntity, ATTR_ADMIN_GROUPS).orElseGet(ArrayList::new);

            List<String> newAdminRoles = getAttributeList(connection, ATTR_ADMIN_ROLES).orElse(null);
            List<String> currentAdminRoles = getAttributeList(existingConnEntity, ATTR_ADMIN_ROLES).orElseGet(ArrayList::new);

            // Check conditions and throw exceptions as necessary

            // If all new admin attributes are null, no action required as these are not meant to update in the request
            if (newAdminUsers == null && newAdminGroups == null && newAdminRoles == null) {
                RequestContext.get().endMetricRecord(metricRecorder);
                return;
            }

            // Throw exception if all new admin attributes are empty but not null
            boolean emptyName = newAdminUsers != null && newAdminUsers.isEmpty();
            boolean emptyGroup = newAdminGroups != null && newAdminGroups.isEmpty();
            boolean emptyRole = newAdminRoles != null && newAdminRoles.isEmpty();

            if (emptyName && emptyGroup && emptyRole) {
                throw new AtlasBaseException(AtlasErrorCode.ADMIN_LIST_SHOULD_NOT_BE_EMPTY, existingConnEntity.getTypeName());
            }
            // Update Keycloak roles
            RoleRepresentation representation = getKeycloakClient().getRoleByName(roleName);
            List<String> finalStateUsers = determineFinalState(newAdminUsers, currentAdminUsers);
            List<String> finalStateGroups = determineFinalState(newAdminGroups, currentAdminGroups);
            List<String> finalStateRoles = determineFinalState(newAdminRoles, currentAdminRoles);
            //this is the case where the final state after comparison with current and new value of all the attributes become empty
            if (allEmpty(finalStateUsers, finalStateGroups, finalStateRoles)) {
                throw new AtlasBaseException(AtlasErrorCode.ADMIN_LIST_SHOULD_NOT_BE_EMPTY, existingConnEntity.getTypeName());
            }

            keycloakStore.updateRoleUsers(roleName, currentAdminUsers, finalStateUsers, representation);
            keycloakStore.updateRoleGroups(roleName, currentAdminGroups, finalStateGroups, representation);
            keycloakStore.updateRoleRoles(roleName, currentAdminRoles, finalStateRoles, representation);


            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    // if the list is null -> we don't want to change
    // if the list is empty -> we want to remove all elements
    // if the list is non-empty -> we want to replace
    private List<String> determineFinalState(List<String> newAdmins, List<String> currentAdmins) {
        return newAdmins == null ? currentAdmins : newAdmins;
    }

    private boolean allEmpty(List<String>... lists) {
        if (lists == null || lists.length == 0) {
            return true;
        }
        return Stream.of(lists).allMatch(list -> list != null && list.isEmpty());
    }


    private Optional<List<String>> getAttributeList(AtlasEntity entity, String attributeName) {
        if (entity.hasAttribute(attributeName)) {
            if (Objects.isNull(entity.getAttribute(attributeName))) {
                return Optional.of(new ArrayList<>(0));
            }
            return Optional.of((List<String>) entity.getAttribute(attributeName));
        }
        return Optional.empty();
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
        mustClauseList.add(mapOf("prefix", mapOf(QUALIFIED_NAME, guid + "/")));
        mustClauseList.add(mapOf("term", mapOf("policyRoles", roleName)));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setSuppressLogs(true);

        AtlasSearchResult result = discovery.directIndexSearch(indexSearchParams);
        if (result != null && result.getEntities() != null) {
            ret = result.getEntities();
        }
        if (CollectionUtils.isEmpty(ret)) {
            throw new AtlasBaseException("No policies found for connection with guid: " + guid + " and role: " + roleName);
        }

        return ret;
    }
}
