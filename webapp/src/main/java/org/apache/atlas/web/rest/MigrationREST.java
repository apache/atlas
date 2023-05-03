package org.apache.atlas.web.rest;

import javax.ws.rs.Path;

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
import org.keycloak.representations.idm.RoleRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.web.rest.EntityREST.validateAttributeLength;

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
    private AtlasGraph graph;
    private EntityDiscoveryService discoveryService;

    @Inject
    public MigrationREST(AtlasEntityStore entityStore, AtlasGraph graph) {
        this.entityStore = entityStore;
        this.graph = graph;
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
