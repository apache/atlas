package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.LinkMeshEntityRequest;
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

@Path("mesh-asset-link")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class MeshEntityAssetLinkREST {

    private static final Logger LOG = LoggerFactory.getLogger(MeshEntityAssetLinkREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ProductAssetLinkREST");

    private final AtlasEntityStore entitiesStore;

    @Inject
    public MeshEntityAssetLinkREST(AtlasEntityStore entitiesStore) {
        this.entitiesStore = entitiesStore;
    }

    /**
     * Links a product to entities.
     *
     * @param domainGuid the ID of the product to be linked
     * @param request    the request containing the GUIDs of the assets to link the product to
     * @throws AtlasBaseException if there is an error during the linking process
     */

    @POST
    @Path("/{domainId}/link-domain")
    @Timed
    public void linkDomainToAssets(@PathParam("domainGuid") final String domainGuid, final LinkMeshEntityRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("linkDomainToAssets");
        // Ensure the current user is authorized to link domain
//        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
//            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Domain linking");
//        }

        LOG.info("Linking Domain {} to Asset", domainGuid);

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "mesh-asset-link");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MeshEntityAssetLinkREST.linkMeshEntityToAssets(" + domainGuid + ")");
            }

            // Link the domain to the specified entities
            entitiesStore.linkMeshEntityToAssets(domainGuid, request.getLinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Unlinks a product from entities.
     *
     * @param domainGuid the ID of the policy to be unlinked
     * @param request    the request containing the GUIDs of the assets to unlink the policy from
     * @throws AtlasBaseException if there is an error during the unlinking process
     */
    @POST
    @Path("/{meshEntityId}/unlink-domain")
    @Timed
    public void unlinkDomainFromAssets(@PathParam("domainGuid") final String domainGuid, final LinkMeshEntityRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("unlinkDomainFromAssets");
        // Ensure the current user is authorized to unlink policies
//        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
//            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy unlinking");
//        }

        LOG.info("Unlinking Domain {} to Asset", domainGuid);

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "domain-asset-link");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MeshEntityAssetLinkREST.unlinkDomainFromAssets(" + domainGuid + ")");
            }

            // Unlink the domain from the specified entities
            entitiesStore.unlinkMeshEntityFromAssets(domainGuid, request.getUnlinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }
}