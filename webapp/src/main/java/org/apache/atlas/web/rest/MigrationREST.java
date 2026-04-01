package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.migration.ValidateProductEdgesMigrationService;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.StakeholderQNAttributeMigrationService;
import org.apache.atlas.repository.store.graph.v2.*;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.transformer.PreProcessorPoliciesTransformer;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.apache.atlas.repository.migration.SoftDeletionProductMigrationService;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.auth.client.keycloak.AtlasKeycloakClient.getKeycloakClient;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

@Path("migration")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class MigrationREST {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.MigrationREST");

    private static final String COLL_ADMIN_ROLE_PATTERN = "collection_admins_%s";
    private static final String COLL_VIEWER_ROLE_PATTERN = "collection_viewer_%s";
    public static final String CONN_NAME_PATTERN = "connection_admins_%s";

    private final AtlasEntityStore entityStore;
    private final PreProcessorPoliciesTransformer transformer;
    private KeycloakStore keycloakStore;
    private AtlasGraph graph;

    private final EntityGraphRetriever entityRetriever;
    private final RedisService redisService;
    protected final AtlasTypeRegistry typeRegistry;
    private final EntityDiscoveryService discovery;

    private final TransactionInterceptHelper   transactionInterceptHelper;

    @Inject
    public MigrationREST(AtlasEntityStore entityStore, AtlasGraph graph, RedisService redisService, EntityDiscoveryService discovery,
                         EntityGraphRetriever entityRetriever, AtlasTypeRegistry typeRegistry, TransactionInterceptHelper transactionInterceptHelper) {
        this.entityStore = entityStore;
        this.graph = graph;
        this.transformer = new PreProcessorPoliciesTransformer();
        keycloakStore = new KeycloakStore();
        this.redisService = redisService;
        this.discovery = discovery;
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.transactionInterceptHelper = transactionInterceptHelper;
    }

    @POST
    @Path("submit")
    @Timed
    public Boolean submit (@QueryParam("migrationType") String migrationType, @QueryParam("forceMigration") boolean forceMigration) throws Exception {
        AtlasPerfTracer perf = null;
        MigrationService migrationService;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.submit(" + migrationType + ")");
            }

            migrationType = MIGRATION_TYPE_PREFIX + migrationType;

            isMigrationInProgress(migrationType);

            switch (migrationType) {
                case DATA_MESH_QN:
                    migrationService = new DataMeshQNMigrationService(entityStore, discovery, entityRetriever, typeRegistry, transactionInterceptHelper, redisService, forceMigration);
                    break;

                default:
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Type of migration is not valid: " + migrationType);
            }

            Thread migrationThread = new Thread(migrationService);
            migrationThread.start();

        } catch (Exception e) {
            LOG.error("Error while submitting migration", e);
            return Boolean.FALSE;
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return Boolean.TRUE;
    }

    private void isMigrationInProgress(String migrationType) throws AtlasBaseException {
        String status = redisService.getValue(migrationType);
        if (PreProcessorUtils.MigrationStatus.IN_PROGRESS.name().equals(status)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    String.format("Migration for %s is already in progress", migrationType));
        }
    }

    @GET
    @Path("status")
    @Timed
    public String getMigrationStatus(@QueryParam("migrationType") String migrationType) throws Exception {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.getMigrationStatus(" + migrationType + ")");
            }

            String value = redisService.getValue(MIGRATION_TYPE_PREFIX + migrationType);

            return Objects.nonNull(value) ? value : "No Migration Found with this key";
        } catch (Exception e) {
            LOG.error("Error while fetching status for migration", e);
            throw e;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("dataproduct/inputs-outputs")
    @Timed
    public Boolean migrateProductInternalAttr (@QueryParam("guid") String guid) throws Exception {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.migrateProductInternalAttr(" + guid + ")");
            }

            DataProductInputsOutputsMigrationService migrationService = new DataProductInputsOutputsMigrationService(entityRetriever, guid, transactionInterceptHelper);
            migrationService.migrateProduct();

        } catch (Exception e) {
            LOG.error("Error while migration inputs/outputs for Dataproduct: {}", guid, e);
            throw e;
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return Boolean.TRUE;
    }

    @POST
    @Path("bootstrap/connections")
    @Timed
    public EntityMutationResponse bootstrapConnections(AtlasEntity.AtlasEntitiesWithExtInfo entities) throws Exception {
        AtlasPerfTracer perf = null;
        EntityMutationResponse response = new EntityMutationResponse();
        try {

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.bootstrapConnections(entityCount=" +
                        (CollectionUtils.isEmpty(entities.getEntities()) ? 0 : entities.getEntities().size()) + ")");
            }

            for (AtlasEntity entity : entities.getEntities()) {
                if (entity.getTypeName().equalsIgnoreCase(CONNECTION_ENTITY_TYPE)) {
                    //create connection role
                    String roleName = String.format(CONN_NAME_PATTERN, entity.getGuid());

                    List<String> adminUsers = (List<String>) entity.getAttribute(ATTR_ADMIN_USERS);
                    List<String> adminGroups = (List<String>) entity.getAttribute(ATTR_ADMIN_GROUPS);
                    List<String> adminRoles = (List<String>) entity.getAttribute(ATTR_ADMIN_ROLES);
                    if (CollectionUtils.isEmpty(adminUsers)) {
                        adminUsers = new ArrayList<>();
                    }

                    if (StringUtils.isNotEmpty(entity.getCreatedBy())) {
                        adminUsers.add(entity.getCreatedBy());
                    }

                    if (CollectionUtils.isEmpty(adminGroups)) {
                        adminGroups = new ArrayList<>();
                    }

                    if (CollectionUtils.isEmpty(adminRoles)) {
                        adminRoles = new ArrayList<>();
                    }

                    entity.setAttribute(ATTR_ADMIN_USERS, adminUsers);

                    RoleRepresentation role = keycloakStore.getRole(roleName);
                    if (role == null) {
                        createCompositeRole(roleName, adminUsers, adminGroups, adminRoles);
                    } else {
                        updateCompositeRole(role, adminUsers, adminGroups, adminRoles);
                    }
                    AtlasEntity.AtlasEntitiesWithExtInfo policiesExtInfo = transformer.transform(entity);
                    try {
                        RequestContext.get().setSkipAuthorizationCheck(true);
                        EntityStream entityStream = new AtlasEntityStream(policiesExtInfo);
                        EntityMutationResponse policyResponse = entityStore.createOrUpdate(entityStream, false);
                        response.setMutatedEntities(policyResponse.getMutatedEntities());
                        LOG.info("Created bootstrap policies for connection");
                    } finally {
                        RequestContext.get().setSkipAuthorizationCheck(false);
                    }
                }
            }

            return response;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("bootstrap/collections")
    @Timed
    public EntityMutationResponse bootstrapCollections(AtlasEntity.AtlasEntitiesWithExtInfo entities) throws Exception {
        AtlasPerfTracer perf = null;
        EntityMutationResponse response = new EntityMutationResponse();
        try {

            for (AtlasEntity entity : entities.getEntities()) {
                if (entity.getTypeName().equalsIgnoreCase(QUERY_COLLECTION_ENTITY_TYPE)) {
                    createCollectionAdminRole(entity);
                    createCollectionViewerRole(entity);

                    //create bootstrap policies
                    AtlasEntity.AtlasEntitiesWithExtInfo policies = transformer.transform(entity);
                    try {
                        RequestContext.get().setSkipAuthorizationCheck(true);

                        EntityStream entityStream = new AtlasEntityStream(policies);
                        EntityMutationResponse policyResponse = entityStore.createOrUpdate(entityStream, false);
                        response.setMutatedEntities(policyResponse.getMutatedEntities());
                        LOG.info("Created bootstrap policies for connection");
                    } finally {
                        RequestContext.get().setSkipAuthorizationCheck(false);
                    }
                }
            }

            return response;
        } finally {
            AtlasPerfTracer.log(perf);
        }

    }

    @GET
    @Path("search/{typeName}")
    @Timed
    public List<AtlasEntity> searchForType(@PathParam("typeName") String typeName, @QueryParam("minExtInfo") @DefaultValue("false") boolean minExtInfo, @QueryParam("ignoreRelationships") @DefaultValue("false") boolean ignoreRelationships) throws Exception {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.searchUsingDslQuery(" + typeName + ")");
            }

            List<AtlasEntity> ret = new ArrayList<>();

            List<String> allowedTypeNames = Arrays.asList("Persona", "Purpose");
            if (!allowedTypeNames.contains(typeName)) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, typeName);
            }

            IndexSearchParams indexSearchParams = new IndexSearchParams();

            Map<String, Object> dsl = getMap("size", 0);

            List<Map<String, Object>> mustClauseList = new ArrayList<>();
            mustClauseList.add(getMap("term", getMap("__typeName.keyword", typeName)));
            mustClauseList.add(getMap("match", getMap("__state", Id.EntityState.ACTIVE)));

            dsl.put("query", getMap("bool", getMap("must", mustClauseList)));

            dsl.put("sort", Collections.singleton(getMap("__guid", getMap("order", "desc"))));

            indexSearchParams.setDsl(dsl);

            int from = 0;
            int size = 100;
            boolean found = true;

            do {
                dsl.put("from", from);
                dsl.put("size", size);
                indexSearchParams.setDsl(dsl);
                List<AtlasEntity> entities = getEntitiesByIndexSearch(indexSearchParams, minExtInfo, ignoreRelationships);

                if (CollectionUtils.isNotEmpty(entities)) {
                    ret.addAll(entities);
                } else {
                    found = false;
                }
                from += size;

            } while (found && ret.size() % size == 0);

            return ret;

        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @POST
    @Path("repair-unique-qualified-name")
    @Timed
    public Boolean updateUniqueQualifiedName(final Set<String> assetGuids) throws Exception {
        AtlasPerfTracer perf = null;
        try {
            if (CollectionUtils.isEmpty(assetGuids)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Asset GUIDs are required for which updating unique qualified name is required");
            }

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.updateUniqueQualifiedName(" + assetGuids + ")");
            }

            UniqueQNAttributeMigrationService migrationService = new UniqueQNAttributeMigrationService(entityRetriever, assetGuids, transactionInterceptHelper);
            migrationService.migrateQN();
        } catch (Exception e) {
            LOG.error("Error while updating unique qualified name for guids: {}", assetGuids, e);
            throw e;
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return Boolean.TRUE;
    }

    @POST
    @Path("product/remove-edges")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Boolean bulkProductsRedundantEdgeRemoval(Set<String> guids) throws Exception {
        AtlasPerfTracer perf = null;
        try {
            if (CollectionUtils.isEmpty(guids)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Product GUIDs are required for removing redundant edges");
            }

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.bulkProductsRedundantEdgeRemoval(" + guids + ")");
            }

            SoftDeletionProductMigrationService migrationService = new SoftDeletionProductMigrationService(graph, guids, new GraphHelper(graph), transactionInterceptHelper);
            migrationService.startEdgeMigration();

        } catch (Exception e) {
            LOG.error("Error while removing edges for guid: {}", guids, e);
            throw e;
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return Boolean.TRUE;
    }

    @POST
    @Path("product/validate-edges")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Boolean bulkValidateProductEdges(Set<String> guids) throws Exception {
        AtlasPerfTracer perf = null;

        boolean flag = false;
        try {
            if (CollectionUtils.isEmpty(guids)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Product GUIDs are required for validating edges");
            }

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.bulkValidateProductEdges(" + guids + ")");
            }

            ValidateProductEdgesMigrationService migrationService = new ValidateProductEdgesMigrationService(guids, new GraphHelper(graph));
            flag = migrationService.validateEdgeMigration();

        } catch (Exception e) {
            LOG.error("Error while validating edges for guid: {}", guids, e);
            throw e;
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return flag;
    }

    @POST
    @Path("repair-stakeholder-qualified-name")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Boolean updateStakeholderQualifiedName(final Set<String> guids) throws Exception {
        AtlasPerfTracer perf = null;
        try {
            if (CollectionUtils.isEmpty(guids)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Stakeholder guids are required for updating unique qualified name");
            }

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.updateStakeholderQualifiedName(" + guids + " entities)");
            }

            StakeholderQNAttributeMigrationService migrationService = new StakeholderQNAttributeMigrationService(entityRetriever, guids, transactionInterceptHelper);
            migrationService.migrateStakeholderQN();
        } catch (Exception e) {
            LOG.error("Error while updating unique qualified name for stakeholders: {}", guids, e);
            throw e;
        } finally {
            AtlasPerfTracer.log(perf);
        }
        return Boolean.TRUE;
    }

    private List<AtlasEntity> getEntitiesByIndexSearch(IndexSearchParams indexSearchParams, Boolean minExtInfo, boolean ignoreRelationships) throws AtlasBaseException {
        List<AtlasEntity> entities = new ArrayList<>();
        String indexName = VERTEX_INDEX_NAME;
        AtlasIndexQuery indexQuery = graph.elasticsearchQuery(indexName);
        DirectIndexQueryResult indexQueryResult = indexQuery.vertices(indexSearchParams);
        Iterator<AtlasIndexQuery.Result> iterator = indexQueryResult.getIterator();

        while (iterator.hasNext()) {
            AtlasIndexQuery.Result result = iterator.next();
            AtlasVertex vertex = result.getVertex();

            if (vertex == null) {
                LOG.warn("vertex is null");
                continue;
            }

            AtlasEntity entity = new AtlasEntity();
            entity.setGuid(GraphHelper.getGuid(vertex));
            entity.setTypeName(GraphHelper.getTypeName(vertex));

            // Use a method to extract attributes from vertex
            setVertexAttributes(vertex, entity);

            // Use a method to get policy entities
            List<AtlasEntity> policyEntities = getPolicyEntities(vertex);
            if (!policyEntities.isEmpty()) {
                entity.setAttribute("policies", policyEntities);
            }
            // Check if entity is not null before adding it to the list
            if (entity != null) {
                entities.add(entity);
            }
        }

        return entities;
    }

    private void setVertexAttributes(AtlasVertex vertex, AtlasEntity entity) {
        List<String> attributes = Arrays.asList("name", "qualifiedName", "roleId");
        for (String attribute : attributes) {
            entity.setAttribute(attribute, vertex.getProperty(attribute, String.class));
        }
        entity.setCustomAttributes(GraphHelper.getCustomAttributes(vertex));
    }

    private List<AtlasEntity> getPolicyEntities(AtlasVertex vertex) {
        List<AtlasEntity> policyEntities = new ArrayList<>();
        Iterator<AtlasVertex> vertices = vertex.query().direction(AtlasEdgeDirection.OUT)
                .label("__AccessControl.policies").vertices().iterator();

        while (vertices.hasNext()) {
            AtlasVertex policyVertex = vertices.next();
            if (policyVertex != null) {
                AtlasEntity policyEntity = new AtlasEntity();
                policyEntity.setGuid(GraphHelper.getGuid(policyVertex));
                policyEntity.setTypeName(GraphHelper.getTypeName(policyVertex));

                // Use a method to extract attributes from policy vertex
                setVertexAttributes(policyVertex, policyEntity);

                policyEntity.setCustomAttributes(GraphHelper.getCustomAttributes(policyVertex));
                policyEntities.add(policyEntity);
            }
        }
        return policyEntities;
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private RoleRepresentation createCollectionAdminRole(AtlasEntity collection) throws AtlasBaseException {
        //create Admin role
        List<String> adminUsers = (List<String>) collection.getAttribute(ATTR_ADMIN_USERS);
        List<String> adminGroups = (List<String>) collection.getAttribute(ATTR_ADMIN_GROUPS);
        List<String> adminRoles = (List<String>) collection.getAttribute(ATTR_ADMIN_ROLES);

        if (adminUsers == null) {
            adminUsers = new ArrayList<>();
        }

        if (StringUtils.isNotEmpty(collection.getCreatedBy())) {
            adminUsers.add(collection.getCreatedBy());
        }

        if (adminGroups == null) {
            adminGroups = new ArrayList<>();
        }

        if (adminRoles == null) {
            adminRoles = new ArrayList<>();
        }

        String adminRoleName = String.format(COLL_ADMIN_ROLE_PATTERN, collection.getGuid());
        RoleRepresentation role = keycloakStore.getRole(adminRoleName);
        if (role == null) {
            createCompositeRole(adminRoleName, adminUsers, adminGroups, adminRoles);
        } else {
            updateCompositeRole(role, adminUsers, adminGroups, adminRoles);
        }


        return role;
    }

    private RoleRepresentation createCollectionViewerRole(AtlasEntity collection) throws AtlasBaseException {
        //create viewers role
        String viewerRoleName = String.format(COLL_VIEWER_ROLE_PATTERN, collection.getGuid());
        List<String> viewerUsers = (List<String>) collection.getAttribute(ATTR_VIEWER_USERS);
        List<String> viewerGroups = (List<String>) collection.getAttribute(ATTR_VIEWER_GROUPS);

        if (viewerUsers == null) {
            viewerUsers = new ArrayList<>();
        }

        if (viewerGroups == null) {
            viewerGroups = new ArrayList<>();
        }

        RoleRepresentation role = keycloakStore.getRole(viewerRoleName);
        if (role == null) {
            createCompositeRole(viewerRoleName, viewerUsers, viewerGroups, new ArrayList<>());
        } else {
            updateCompositeRole(role, viewerUsers, viewerGroups, new ArrayList<>());
        }
        return role;
    }

    private void updateCompositeRole(RoleRepresentation role, List<String> users, List<String> groups, List<String> roles) throws AtlasBaseException {
        List<UserRepresentation> currentUsers = getKeycloakClient().getRoleUserMembers(role.getName()).stream().collect(Collectors.toList());
        List<GroupRepresentation> currentGroups = getKeycloakClient().getRoleGroupMembers(role.getName()).stream().collect(Collectors.toList());
        List<RoleRepresentation> currentRoles = getKeycloakClient().getRoleComposites(role.getName()).stream().collect(Collectors.toList());

        //Find users to add and remove from role, if the user is already in the role, don't add it again
        List<String> usersToAdd = users.stream().filter(user -> currentUsers.stream().noneMatch(userRep -> userRep.getUsername().equals(user))).collect(Collectors.toList());
        List<UserRepresentation> usersToRemove = currentUsers.stream().filter(user -> users.stream().noneMatch(userRep -> userRep.equals(user.getUsername()))).collect(Collectors.toList());

        //Find groups to add and remove from role, if the group is already in the role, don't add it again
        List<String> groupsToAdd = groups.stream().filter(group -> currentGroups.stream().noneMatch(groupRep -> groupRep.getName().equals(group))).collect(Collectors.toList());
        List<GroupRepresentation> groupsToRemove = currentGroups.stream().filter(group -> groups.stream().noneMatch(groupRep -> groupRep.equals(group.getName()))).collect(Collectors.toList());

        //Find roles to add and remove from role, if the role is already in the role, don't add it again
        List<String> rolesToAdd = roles.stream().filter(roleToAdd -> currentRoles.stream().noneMatch(roleRep -> roleRep.getId().equals(roleToAdd))).collect(Collectors.toList());
        List<RoleRepresentation> rolesToRemove = currentRoles.stream().filter(roleToRemove -> roles.stream().noneMatch(roleRep -> roleRep.equals(roleToRemove.getId()))).collect(Collectors.toList());

        //Add users to role
        for (String userName : usersToAdd) {
            List<UserRepresentation> matchedUsers = getKeycloakClient().searchUserByUserName(userName);
            Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();
            if (keyUserOptional.isPresent()) {
                UserRepresentation keyUser = keyUserOptional.get();
                getKeycloakClient().addRealmLevelRoleMappingsForUser(keyUser.getId(), Collections.singletonList(role));
            }
        }
        //Remove users from role
        for (UserRepresentation userToRemove : usersToRemove) {
            getKeycloakClient().deleteRealmLevelRoleMappingsForUser(userToRemove.getId(), Collections.singletonList(role));
        }

        //Add groups to role
        for (String groupName : groupsToAdd) {
            List<GroupRepresentation> matchedGroups = getKeycloakClient().searchGroupByName(groupName, 0, 1);
            Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();
            if (keyGroupOptional.isPresent()) {
                GroupRepresentation keyGroup = keyGroupOptional.get();
                getKeycloakClient().addRealmLevelRoleMappingsForGroup(keyGroup.getId(), Collections.singletonList(role));
            }
        }
        //Remove groups from role
        for (GroupRepresentation groupToRemove : groupsToRemove) {
            getKeycloakClient().deleteRealmLevelRoleMappingsForGroup(groupToRemove.getId(), Collections.singletonList(role));
        }

        //Add roles to role
        for (String roleId : rolesToAdd) {
            RoleRepresentation roleToAdd = getKeycloakClient().getRoleById(roleId);
            getKeycloakClient().addComposites(role.getName(), Collections.singletonList(roleToAdd));
        }

        //Remove roles from role
        for (RoleRepresentation roleToRemove : rolesToRemove) {
            getKeycloakClient().deleteComposites(role.getName(), Collections.singletonList(roleToRemove));
        }
    }

    private void createCompositeRole(String roleName, List<String> users, List<String> groups, List<String> roles) throws AtlasBaseException {
        List<UserRepresentation> roleUsers = new ArrayList<>();
        List<GroupRepresentation> roleGroups = new ArrayList<>();
        List<RoleRepresentation> roleRoles = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(users)) {
            roleUsers = users.stream().map(user -> {
                List<UserRepresentation> matchedUsers = null;
                try {
                    matchedUsers = getKeycloakClient().searchUserByUserName(user);
                    Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> user.equals(x.getUsername())).findFirst();
                    if (keyUserOptional.isPresent()) {
                        return keyUserOptional.get();
                    } else {
                        LOG.warn("User {} not found in keycloak", user);
                    }
                } catch (AtlasBaseException e) {
                    LOG.error("Failed to get user by name {}", user, e);
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }

        if (CollectionUtils.isNotEmpty(groups)) {
            roleGroups = new ArrayList<>();
            for(String group : groups) {
                List<GroupRepresentation> matchedGroups = getKeycloakClient().searchGroupByName(group, 0, 1);
                Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> group.equals(x.getName())).findFirst();
                if (keyGroupOptional.isPresent()) {
                    roleGroups.add(keyGroupOptional.get());
                } else {
                    LOG.warn("Group {} not found in keycloak", group);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(roles)) {
            roleRoles = new ArrayList<>();
            for(String role: roles) {
                List<RoleRepresentation> matchedRoles = getKeycloakClient().getAllRoles();
                Optional<RoleRepresentation> keyRoleOptional = matchedRoles.stream().filter(x -> role.equals(x.getId())).findFirst();
                if (keyRoleOptional.isPresent()) {
                    roleRoles.add(keyRoleOptional.get());
                } else {
                    LOG.warn("Role {} not found in keycloak", role);
                }
            }
        }

        RoleRepresentation role = new RoleRepresentation();
        role.setName(roleName);
        role.setComposite(true);

        RoleRepresentation createdRole = keycloakStore.createRole(role);
        if (createdRole == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Failed to create role " + roleName);
        }

        //Add realm role to users
        for (UserRepresentation user : roleUsers) {
            getKeycloakClient().addRealmLevelRoleMappingsForUser(user.getId(), Collections.singletonList(createdRole));
        }

        //Add realm role to groups
        for (GroupRepresentation group : roleGroups) {
            getKeycloakClient().addRealmLevelRoleMappingsForGroup(group.getId(), Collections.singletonList(createdRole));

        }

        //Add realm role to roles
        for (RoleRepresentation roleToAdd : roleRoles) {
            getKeycloakClient().addComposites(createdRole.getName(), Collections.singletonList(roleToAdd));
        }

    }
}