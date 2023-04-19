package org.apache.atlas.web.rest;

import javax.ws.rs.Path;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.transformer.PreProcessorPoliciesTransformer;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.keycloak.representations.idm.RoleRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.authorize.AtlasAuthorizerFactory.ATLAS_AUTHORIZER_IMPL;
import static org.apache.atlas.authorize.AtlasAuthorizerFactory.CURRENT_AUTHORIZER_IMPL;
import static org.apache.atlas.repository.Constants.*;

@Path("migration")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class MigrationREST {
    private static final Logger LOG      = LoggerFactory.getLogger(MigrationREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.MigrationREST");

    private static final String COLL_ADMIN_ROLE_PATTERN  = "collection_admins_%s";
    private static final String COLL_VIEWER_ROLE_PATTERN = "collection_viewer_%s";

    public static final String CONN_NAME_PATTERN = "connection_admins_%s";

    private final AtlasEntityStore entityStore;
    private final PreProcessorPoliciesTransformer transformer;
    private KeycloakStore keycloakStore;

    @Inject
    public MigrationREST(AtlasEntityStore entityStore) {
        this.entityStore = entityStore;
        this.transformer = new PreProcessorPoliciesTransformer();
        keycloakStore = new KeycloakStore();
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

                    entity.setAttribute(ATTR_ADMIN_USERS, adminUsers);

                    RoleRepresentation role = keycloakStore.getRole(roleName);
                    if (role == null) {
                        role = keycloakStore.createRoleForConnection(roleName, true, adminUsers, adminGroups, adminRoles);
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

        String adminRoleName = String.format(COLL_ADMIN_ROLE_PATTERN, collection.getGuid());
        RoleRepresentation role = keycloakStore.getRole(adminRoleName);
        if (role == null) {
            role = keycloakStore.createRoleForConnection(adminRoleName, true, adminUsers, adminGroups, adminRoles);
        }

        return role;
    }

    private RoleRepresentation createCollectionViewerRole(AtlasEntity collection) throws AtlasBaseException {
        //create viewers role
        String viewerRoleName = String.format(COLL_VIEWER_ROLE_PATTERN, collection.getGuid());
        List<String> viewerUsers = (List<String>) collection.getAttribute(ATTR_VIEWER_USERS);
        List<String> viewerGroups = (List<String>) collection.getAttribute(ATTR_VIEWER_GROUPS);
        RoleRepresentation role = keycloakStore.getRole(viewerRoleName);
        if (role == null) {
            role = keycloakStore.createRoleForConnection(viewerRoleName, true, viewerUsers, viewerGroups, null);
        }
        return role;
    }
}
