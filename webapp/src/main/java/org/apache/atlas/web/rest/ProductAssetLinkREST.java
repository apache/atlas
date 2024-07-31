package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.LinkDataProductRequest;
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

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

@Path("product-asset-link")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class ProductAssetLinkREST {

    private static final Logger LOG = LoggerFactory.getLogger(ProductAssetLinkREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ProductAssetLinkREST");

    private final AtlasEntityStore entitiesStore;

    @Inject
    public ProductAssetLinkREST(AtlasEntityStore entitiesStore) {
        this.entitiesStore = entitiesStore;
    }

    /**
     * Links a product to entities.
     *
     * @param productGuid the ID of the product to be linked
     * @param request    the request containing the GUIDs of the assets to link the product to
     * @throws AtlasBaseException if there is an error during the linking process
     */

    @POST
    @Path("/{productId}/link-product-to-asset")
    @Timed
    public void linkProductToAsset(@PathParam("productId") final String productGuid, final LinkDataProductRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("linkDataProductToAsset");
        // Ensure the current user is authorized to link policies
//        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
//            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy linking");
//        }

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "product-asset-link");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ProductAssetLinkREST.linkProductToAsset(" + productGuid + ")");
            }

            // Link the product to the specified entities
            entitiesStore.linkProductToAsset(productGuid, request.getLinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }

    @POST
    @Path("/{productId}/link-product-with-notification")
    @Timed
    public void linkProductWithNotification(@PathParam("productId") final String productGuid, final LinkDataProductRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("linkDataProductToAsset");
        // Ensure the current user is authorized to link policies
//        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
//            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy linking");
//        }

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "product-asset-link");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ProductAssetLinkREST.linkProductWithNotification(" + productGuid + ")");
            }

            // Link the product to the specified entities
            entitiesStore.linkProductWithNotification(productGuid, request.getLinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Unlinks a product from entities.
     *
     * @param productGuid the ID of the policy to be unlinked
     * @param request    the request containing the GUIDs of the assets to unlink the policy from
     * @throws AtlasBaseException if there is an error during the unlinking process
     */
    @POST
    @Path("/{policyId}/unlink-product-to-asset")
    @Timed
    public void unlinkProductFromAsset(@PathParam("policyId") final String productGuid, final LinkDataProductRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("unlinkProductFromAsset");
        // Ensure the current user is authorized to unlink policies
//        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
//            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy unlinking");
//        }

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "product-asset-link");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ProductAssetLinkREST.unlinkProductFromAsset(" + productGuid + ")");
            }

            // Unlink the business policy from the specified entities
            entitiesStore.unlinkProductFromAsset(productGuid, request.getUnlinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }
}