package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.LinkBusinessPolicyRequest;
import org.apache.atlas.model.instance.MoveBusinessPolicyRequest;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.Objects;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

@Path("business-policy")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class BusinessPolicyREST {

    private static final Logger LOG = LoggerFactory.getLogger(BusinessPolicyREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.BusinessPolicyREST");

    private final AtlasEntityStore entitiesStore;

    @Inject
    public BusinessPolicyREST(AtlasEntityStore entitiesStore) {
        this.entitiesStore = entitiesStore;
    }

    /**
     * Links a business policy to entities.
     *
     * @param policyGuid the ID of the policy to be linked
     * @param request    the request containing the GUIDs of the assets to link the policy to
     * @throws AtlasBaseException if there is an error during the linking process
     */
    @POST
    @Path("/{policyId}/link-business-policy")
    @Timed
    public void linkBusinessPolicy(final LinkBusinessPolicyRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("linkBusinessPolicy");

        if(Objects.isNull(request) || CollectionUtils.isEmpty(request.getBusinessPolicyGuids()) || CollectionUtils.isEmpty(request.getLinkGuids())){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, RequestContext.getCurrentUser(), "Policy linking");
        }

        // Ensure the current user is authorized to link policies
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy linking");
        }


        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("x-atlan-route", "business-policy-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BusinessPolicyREST.linkBusinessPolicy()");
            }

            // Link the business policy to the specified entities
            entitiesStore.linkBusinessPolicy(request.getBusinessPolicyGuids(), request.getLinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Unlinks a business policy from entities.
     *
     * @param policyGuid the ID of the policy to be unlinked
     * @param request    the request containing the GUIDs of the assets to unlink the policy from
     * @throws AtlasBaseException if there is an error during the unlinking process
     */
    @POST
    @Path("/{policyId}/unlink-business-policy")
    @Timed
    public void unlinkBusinessPolicy(@PathParam("policyId") final String policyGuid, final LinkBusinessPolicyRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("unlinkBusinessPolicy");
        // Ensure the current user is authorized to unlink policies
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy unlinking");
        }

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("x-atlan-route", "business-policy-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BusinessPolicyREST.unlinkBusinessPolicy(" + policyGuid + ")");
            }

            // Unlink the business policy from the specified entities
            entitiesStore.unlinkBusinessPolicy(policyGuid, request.getUnlinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }


    @POST
    @Path("/move/{assetId}")
    @Timed
    public void moveBusinessPolicies(
            @PathParam("assetId") String assetId,
            final MoveBusinessPolicyRequest request
    ) throws AtlasBaseException {
        if (isInvalidRequest(request, assetId)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Request is missing required parameters.");
        }
        // Start performance metric recording
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("moveBusinessPolicy");

        // Ensure the current user is authorized to move the policy
        String currentUser = RequestContext.getCurrentUser();
        if (!ARGO_SERVICE_USER_NAME.equals(currentUser)) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, currentUser, "moveBusinessPolicy");
        }

        // Set request context parameters for the current operation
        RequestContext requestContext = RequestContext.get();
        requestContext.setIncludeClassifications(false);
        requestContext.setIncludeMeanings(false);
        requestContext.getRequestContextHeaders().put("x-atlan-route", "business-policy-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BusinessPolicyREST.moveBusinessPolicy(" + assetId + ")");
            }

            // Move the business policy using the entitiesStore
            entitiesStore.moveBusinessPolicies(request.getPolicyIds(), assetId, request.getType());
        } finally {
            // Log performance trace and end metric recording
            AtlasPerfTracer.log(perf);
            requestContext.endMetricRecord(metric);
        }
    }

    private boolean isInvalidRequest(MoveBusinessPolicyRequest request, String assetId) {
        return Objects.isNull(request) ||
                CollectionUtils.isEmpty(request.getPolicyIds()) ||
                Objects.isNull(request.getType()) ||
                Objects.isNull(assetId);
    }

}

