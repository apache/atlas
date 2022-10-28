package org.apache.atlas.web.rest;

import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.IAtlasGraphProvider;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.web.service.AtlasHealthStatus;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import static org.apache.atlas.AtlasErrorCode.FAILED_TO_REFRESH_TYPE_DEF_CACHE;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;


@Path("admin/types")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TypeCacheRefreshREST {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefreshREST.class);

    private final AtlasTypeDefStore typeDefStore;
    private final IAtlasGraphProvider provider;
    private final ServiceState serviceState;
    private final AtlasHealthStatus atlasHealthStatus;

    @Inject
    public TypeCacheRefreshREST(AtlasTypeDefStore typeDefStore, IAtlasGraphProvider provider, ServiceState serviceState, AtlasHealthStatus atlasHealthStatus) {
        this.typeDefStore = typeDefStore;
        this.provider = provider;
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
    public void refreshCache(@QueryParam("expectedFieldKeys") int expectedFieldKeys, @QueryParam("traceId") String traceId) throws AtlasBaseException {
        try {
            if (serviceState.getState() != ServiceState.ServiceStateValue.ACTIVE) {
                LOG.warn("Node is in {} state. skipping refreshing type-def-cache :: traceId {}", serviceState.getState(), traceId);
                return;
            }
            refreshTypeDef(expectedFieldKeys, traceId);
        } catch (Exception e) {
            LOG.error("Error during refreshing cache  :: traceId " + traceId + " " + e.getMessage(), e);
            serviceState.setState(ServiceState.ServiceStateValue.PASSIVE, true);
            atlasHealthStatus.markUnhealthy(AtlasHealthStatus.Component.TYPE_DEF_CACHE, "type-def-cache is not in sync");
            throw new AtlasBaseException(FAILED_TO_REFRESH_TYPE_DEF_CACHE);
        }
    }

    private void refreshTypeDef(int expectedFieldKeys,final String traceId) throws RepositoryException, InterruptedException, AtlasBaseException {
        LOG.info("Initiating type-def cache refresh with expectedFieldKeys = {} :: traceId {}", expectedFieldKeys,traceId);
        int currentSize = provider.get().getManagementSystem().getGraphIndex(VERTEX_INDEX).getFieldKeys().size();
        LOG.info("Size of field keys before refresh = {} :: traceId {}", currentSize,traceId);

        long totalWaitTimeInMillis = 15 * 1000;//15 seconds
        long sleepTimeInMillis = 500;
        long totalIterationsAllowed = Math.floorDiv(totalWaitTimeInMillis, sleepTimeInMillis);
        int counter = 0;

        while (currentSize != expectedFieldKeys && counter++ < totalIterationsAllowed) {
            currentSize = provider.get().getManagementSystem().getGraphIndex(VERTEX_INDEX).getFieldKeys().size();
            LOG.info("field keys size found = {} at iteration {} :: traceId {}", currentSize, counter, traceId);
            Thread.sleep(sleepTimeInMillis);
        }
        //This condition will hold true when expected fieldKeys did not appear even after waiting for totalWaitTimeInMillis
        if (counter > totalIterationsAllowed) {
            final String errorMessage = String.format("Could not find desired count of fieldKeys %d after %d ms of wait. Current size of field keys is %d :: traceId %s",
                    expectedFieldKeys, totalWaitTimeInMillis, currentSize, traceId);
            throw new AtlasBaseException(errorMessage);
        } else {
            LOG.info("Found desired size of fieldKeys in iteration {} :: traceId {}", counter, traceId);
        }
        //Reload in-memory cache of type-registry
        typeDefStore.init();

        LOG.info("Size of field keys after refresh = {}", provider.get().getManagementSystem().getGraphIndex(VERTEX_INDEX).getFieldKeys().size());
        LOG.info("Completed type-def cache refresh :: traceId {}", traceId);
    }
}
