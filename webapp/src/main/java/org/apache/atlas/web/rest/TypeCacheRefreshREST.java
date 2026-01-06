package org.apache.atlas.web.rest;

import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.store.graph.AtlasTypeDefGraphStore;
import org.apache.atlas.web.service.AtlasHealthStatus;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import static org.apache.atlas.AtlasErrorCode.FAILED_TO_REFRESH_TYPE_DEF_CACHE;

@Path("admin/types")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TypeCacheRefreshREST {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefreshREST.class);

    private final AtlasTypeDefGraphStore typeDefStore;
    private final ServiceState serviceState;
    private final AtlasHealthStatus atlasHealthStatus;

    @Inject
    public TypeCacheRefreshREST(AtlasTypeDefGraphStore typeDefStore, ServiceState serviceState, AtlasHealthStatus atlasHealthStatus) {
        this.typeDefStore = typeDefStore;
        this.serviceState = serviceState;
        this.atlasHealthStatus = atlasHealthStatus;
    }

    /**
     * API to refresh type-def cache.
     *
     * @throws AtlasBaseException
     * @HTTP 204 if type def cache is refreshed successfully
     * @HTTP 500 if there is an error refreshing type def cache
     */
    @POST
    @Path("/refresh")
    @Timed
    public void refreshCache(final AtlasTypesDef typesDef, @QueryParam("action") String action,  @QueryParam("traceId") String traceId) throws AtlasBaseException {
        try {
            if (serviceState.getState() != ServiceState.ServiceStateValue.ACTIVE) {
                LOG.warn("Node is in {} state. skipping refreshing type-def-cache :: traceId {}", serviceState.getState(), traceId);
                return;
            }
            refreshTypeDef(typesDef, action, traceId);
        } catch (Exception e) {
            LOG.error("Error during refreshing cache  :: traceId " + traceId + " " + e.getMessage(), e);
            serviceState.setState(ServiceState.ServiceStateValue.PASSIVE, true);
            atlasHealthStatus.markUnhealthy(AtlasHealthStatus.Component.TYPE_DEF_CACHE, "type-def-cache is not in sync");
            throw new AtlasBaseException(FAILED_TO_REFRESH_TYPE_DEF_CACHE);
        }
    }

    private synchronized void refreshTypeDef(AtlasTypesDef typesDef, String action, final String traceId) throws RepositoryException, InterruptedException, AtlasBaseException {
        LOG.info("Refreshing type-def cache with action {} :: traceId {}", action, traceId);

        // Handle null action
        if (action == null) {
            action = "INIT";
        }

        // Force INIT for null typesDef
        if (typesDef == null) {
            action = "INIT";
        }

        // Check if this is a seeder action
        if (isTypeDefSeederAction(typesDef)) {
            action = "INIT";
        }

        // Process actions
        switch (action.toUpperCase()) {
            case "CREATE":
            case "UPDATE":
                typeDefStore.addTypesDefInCache(typesDef);
                break;
            case "DELETE":
                typeDefStore.deleteTypesDefInCache(typesDef);
                break;
            case "INIT":
            default:
                typeDefStore.init();
        }
        LOG.info("Completed type-def cache refresh :: traceId {}", traceId);
    }

    private boolean isTypeDefSeederAction(AtlasTypesDef typesDef) {
        if (typesDef == null) {
            return false;
        }

        // Check if any of the non-UI typedefs are changing (indicating a seeder action)
        return CollectionUtils.isNotEmpty(typesDef.getEntityDefs()) ||
                CollectionUtils.isNotEmpty(typesDef.getRelationshipDefs()) ||
                CollectionUtils.isNotEmpty(typesDef.getStructDefs());
    }
}
