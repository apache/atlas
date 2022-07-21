package org.apache.atlas.web.rest;

import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static org.apache.atlas.repository.Constants.*;


@Path("admin/types")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TypeCacheRefreshREST {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefreshREST.class);

    private final AtlasTypeDefStore typeDefStore;
    private final AtlasJanusGraph atlasJanusGraph;

    @Inject
    public TypeCacheRefreshREST(AtlasTypeDefStore typeDefStore, AtlasJanusGraph atlasJanusGraph) {
        this.typeDefStore = typeDefStore;
        this.atlasJanusGraph = atlasJanusGraph;
    }

    /**
     * API to refresh type-def cache.
     * @throws AtlasBaseException
     * @HTTP 204 if type def cache is refreshed successfully
     * @HTTP 500 if there is an error refreshing type def cache
     */
    @POST
    @Path("/refresh")
    @Timed
    public void refreshCache() throws AtlasBaseException {
        LOG.info("Initiating type-def cache refresh");
        //Reload in-memory cache of type-registry
        typeDefStore.init();
        typeDefStore.notifyLoadCompletion();
        LOG.info("Completed type-def cache refresh");
    }
}
