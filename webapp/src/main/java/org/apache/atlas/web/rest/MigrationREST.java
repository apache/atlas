package org.apache.atlas.web.rest;

import javax.ws.rs.Path;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.keycloak.client.KeycloakClient;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.transformer.PreProcessorPoliciesTransformer;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.keycloak.admin.client.resource.*;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.stream.Collectors;
import static org.apache.atlas.repository.Constants.*;

@Path("migration")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class MigrationREST {
    private static final Logger LOG      = LoggerFactory.getLogger(MigrationREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.MigrationREST");

    private static final String COLL_ADMIN_ROLE_PATTERN = "collection_admins_%s";
    private static final String COLL_VIEWER_ROLE_PATTERN = "collection_viewer_%s";
    public static final String CONN_NAME_PATTERN = "connection_admins_%s";

    private final AtlasEntityStore entityStore;
    private final PreProcessorPoliciesTransformer transformer;
    private KeycloakStore keycloakStore;
    private AtlasGraph graph;

    @Inject
    public MigrationREST(AtlasEntityStore entityStore, AtlasGraph graph) {
        this.entityStore = entityStore;
        this.graph = graph;
        this.transformer = new PreProcessorPoliciesTransformer();
        keycloakStore = new KeycloakStore();
    }

    @POST
    @Path("bootstrap/connections")
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
                    }else {
                        updateCompositeRole(role, adminUsers, adminGroups, adminRoles);
                    }
                    AtlasEntity.AtlasEntitiesWithExtInfo policiesExtInfo = transformer.transform(entity);
                    try {
                        RequestContext.get().setPoliciesBootstrappingInProgress(true);
                        EntityStream entityStream = new AtlasEntityStream(policiesExtInfo);
                        EntityMutationResponse policyResponse = entityStore.createOrUpdate(entityStream, false);
                        response.setMutatedEntities(policyResponse.getMutatedEntities());
                        LOG.info("Created bootstrap policies for connection");
                    } finally {
                        RequestContext.get().setPoliciesBootstrappingInProgress(false);
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
                        RequestContext.get().setPoliciesBootstrappingInProgress(true);

                        EntityStream entityStream = new AtlasEntityStream(policies);
                        EntityMutationResponse policyResponse = entityStore.createOrUpdate(entityStream, false);
                        response.setMutatedEntities(policyResponse.getMutatedEntities());
                        LOG.info("Created bootstrap policies for connection");
                    } finally {
                        RequestContext.get().setPoliciesBootstrappingInProgress(false);
                    }
                }
            }

            return response;
        }finally {
            AtlasPerfTracer.log(perf);
        }

    }

    @GET
    @Path("search/{typeName}")
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

    private List<AtlasEntity> getEntitiesByIndexSearch(IndexSearchParams indexSearchParams, Boolean minExtInfo, boolean ignoreRelationships) throws AtlasBaseException{
        List<AtlasEntity> entities = new ArrayList<>();
        String indexName = "janusgraph_vertex_index";
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
        UsersResource usersResource = KeycloakClient.getKeycloakClient().getRealm().users();
        GroupsResource groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();
        RolesResource rolesResource      = KeycloakClient.getKeycloakClient().getRealm().roles();
        RoleByIdResource rolesIdResource = KeycloakClient.getKeycloakClient().getRealm().rolesById();

        RoleResource connectionRoleResource = rolesResource.get(role.getName());
        List<UserRepresentation> currentUsers = connectionRoleResource.getRoleUserMembers().stream().collect(Collectors.toList());
        List<GroupRepresentation> currentGroups = connectionRoleResource.getRoleGroupMembers().stream().collect(Collectors.toList());
        List<RoleRepresentation> currentRoles = connectionRoleResource.getRoleComposites().stream().collect(Collectors.toList());

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
            List<UserRepresentation> matchedUsers = usersResource.search(userName);
            Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();
            if (keyUserOptional.isPresent()) {
                UserRepresentation keyUser = keyUserOptional.get();
                UserResource userResource = usersResource.get(keyUser.getId());
                userResource.roles().realmLevel().add(Collections.singletonList(role));
                userResource.update(keyUser);
            }
        }
        //Remove users from role
        for (UserRepresentation userToRemove : usersToRemove) {
            UserResource userResource = usersResource.get(userToRemove.getId());
            userResource.roles().realmLevel().remove(Collections.singletonList(role));
            userResource.update(userToRemove);
        }

        //Add groups to role
        for (String groupName : groupsToAdd) {
            List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 1);
            Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();
            if (keyGroupOptional.isPresent()) {
                GroupRepresentation keyGroup = keyGroupOptional.get();
                GroupResource groupResource = groupsResource.group(keyGroup.getId());
                groupResource.roles().realmLevel().add(Collections.singletonList(role));
                groupResource.update(keyGroup);
            }
        }
        //Remove groups from role
        for (GroupRepresentation groupToRemove : groupsToRemove) {
            GroupResource groupResource = groupsResource.group(groupToRemove.getId());
            groupResource.roles().realmLevel().remove(Collections.singletonList(role));
            groupResource.update(groupToRemove);
        }

        //Add roles to role
        for (String roleId : rolesToAdd) {
            RoleRepresentation roleToAdd = rolesIdResource.getRole(roleId);
            connectionRoleResource.addComposites(Collections.singletonList(roleToAdd));
        }

        //Remove roles from role
        for (RoleRepresentation roleToRemove : rolesToRemove) {
            connectionRoleResource.deleteComposites(Collections.singletonList(roleToRemove));
        }
    }

    private void createCompositeRole(String roleName, List<String> users, List<String> groups, List<String> roles) throws AtlasBaseException {
        List<UserRepresentation> roleUsers = new ArrayList<>();
        List<GroupRepresentation> roleGroups = new ArrayList<>();
        List<RoleRepresentation> roleRoles = new ArrayList<>();

        UsersResource usersResource = KeycloakClient.getKeycloakClient().getRealm().users();
        GroupsResource groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();
        RolesResource rolesResource      = KeycloakClient.getKeycloakClient().getRealm().roles();

        if(CollectionUtils.isNotEmpty(users)) {
            roleUsers = users.stream().map(user -> {
                List<UserRepresentation> matchedUsers = usersResource.search(user);
                Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> user.equals(x.getUsername())).findFirst();
                if (keyUserOptional.isPresent()) {
                    return keyUserOptional.get();
                } else {
                    LOG.warn("User {} not found in keycloak", user);
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }

        if(CollectionUtils.isNotEmpty(groups)) {
            roleGroups = groups.stream().map(group -> {
                List<GroupRepresentation> matchedGroups = groupsResource.groups(group, 0, 1);
                Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> group.equals(x.getName())).findFirst();
                if (keyGroupOptional.isPresent()) {
                    return keyGroupOptional.get();
                } else {
                    LOG.warn("Group {} not found in keycloak", group);
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }

        if(CollectionUtils.isNotEmpty(roles)) {
            roleRoles = roles.stream().map(role -> {
                List<RoleRepresentation> matchedRoles = rolesResource.list();
                Optional<RoleRepresentation> keyRoleOptional = matchedRoles.stream().filter(x -> role.equals(x.getId())).findFirst();
                if (keyRoleOptional.isPresent()) {
                    return keyRoleOptional.get();
                } else  {
                    LOG.warn("Role {} not found in keycloak", role);    
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }

        RoleRepresentation role = new RoleRepresentation();
        role.setName(roleName);
        role.setComposite(true);

        RoleRepresentation createdRole = keycloakStore.createRole(role);
        if (createdRole == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Failed to create role " + roleName);
        }
        RoleResource compositeRoleResource = rolesResource.get(createdRole.getName());

        //Add realm role to users
        for (UserRepresentation user : roleUsers) {
            UserResource userResource = usersResource.get(user.getId());
            userResource.roles().realmLevel().add(Collections.singletonList(createdRole));
            userResource.update(user);
        }

        //Add realm role to groups
        for (GroupRepresentation group : roleGroups) {
            GroupResource groupResource = groupsResource.group(group.getId());
            groupResource.roles().realmLevel().add(Collections.singletonList(createdRole));
            groupResource.update(group);
        }

        //Add realm role to roles
        for (RoleRepresentation roleToAdd : roleRoles) {
            compositeRoleResource.addComposites(Collections.singletonList(roleToAdd));
            compositeRoleResource.update(createdRole);
        }

    }
}