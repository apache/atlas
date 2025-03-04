package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.BusinessLineageRequest;
import org.apache.atlas.model.instance.LinkMeshEntityRequest;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.businesslineage.BusinessLineageService;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("business-lineage")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class BusinessLineageREST {

    private static final Logger LOG = LoggerFactory.getLogger(BusinessLineageREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.BusinessLineageREST");

    private final BusinessLineageService businessLineageService;

    @Inject
    public BusinessLineageREST(BusinessLineageService businessLineageService) {
        this.businessLineageService = businessLineageService;
    }

    /**
     * Links a product to entities.
     *
     * @param request    the request containing the GUIDs of the assets to link the product to
     * @throws AtlasBaseException if there is an error during the linking process
     */

    @POST
    @Path("/create-lineage")
    @Timed
    public void createLineage(final BusinessLineageRequest request) throws AtlasBaseException, RepositoryException {

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(true);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "business-lineage-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BusinessLineageREST.createLineage()");
            }

            // Create lineage
            businessLineageService.createLineage(request);

        }catch(AtlasBaseException | RepositoryException e) {
            LOG.error("An error occurred while creating lineage", e);
            throw e;
        }finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
        }
    }
}