package org.apache.atlas.web.rest;

import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graph.GremlinShellHelper;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("gremlin")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class GremlinREST {

    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.GremlinREST");
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryREST.class);
    GremlinShellHelper gremlinShellHelper ;

    public GremlinREST(GremlinShellHelper graphHelper) {
        this.gremlinShellHelper = graphHelper;
    }

    @Path("top")
    @GET
    @Timed
    public Object getTopXSuperVertex(@QueryParam("limit") final int limit)  {
        AtlasPerfTracer perf = null;
        try {
            return gremlinShellHelper.getTopXSuperVertex(limit);
        } catch (AtlasBaseException e) {
            throw new RuntimeException(e);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @Path("/guid/{guid}/relationship")
    @GET
    @Timed
    public Object getRelationshipStats(@PathParam("guid") final String guid)  {
        AtlasPerfTracer perf = null;
        try {
            return gremlinShellHelper.getVertexRelationshipStats(guid);
        } catch (AtlasBaseException e) {
            throw new RuntimeException(e);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

}

