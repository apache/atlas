package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.LinkBusinessPolicyRequest;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
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

@Path("alternate")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class AlternateREST {

    private static final Logger LOG = LoggerFactory.getLogger(AlternateREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.AlternateREST");

    private final AtlasEntityStore entitiesStore;

    @Inject
    public AlternateREST(AtlasEntityStore entitiesStore) {
        this.entitiesStore = entitiesStore;
    }

    /**
     * Links a business policy to entities.
     *
     * @param policyId the ID of the policy to be linked
     * @param request  the request containing the GUIDs of the assets to link the policy to
     * @throws AtlasBaseException if there is an error during the linking process
     */
    @POST
    @Path("/{policyId}/link-business-policy")
    @Timed
    public void linkBusinessPolicy(@PathParam("policyId") final String policyId, final LinkBusinessPolicyRequest request) throws AtlasBaseException {
        // Ensure the current user is authorized to link policies
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy linking");
        }

        // Set request context parameters
        RequestContext.get().setAlternatePath(true);
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AlternateREST.linkBusinessPolicy(" + policyId + ")");
            }

            // Link the business policy to the specified entities
            entitiesStore.linkBusinessPolicy(policyId, request.getLinkGuids());
        } catch (AtlasBaseException abe) {
            LOG.error("Error in policy linking: ", abe);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "During Policy linking");
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Unlinks a business policy from entities.
     *
     * @param policyId the ID of the policy to be unlinked
     * @param request  the request containing the GUIDs of the assets to unlink the policy from
     * @throws AtlasBaseException if there is an error during the unlinking process
     */
    @POST
    @Path("/{policyId}/unlink-business-policy")
    @Timed
    public void unlinkBusinessPolicy(@PathParam("policyId") final String policyId, final LinkBusinessPolicyRequest request) throws AtlasBaseException {
        // Ensure the current user is authorized to unlink policies
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Policy unlinking");
        }

        // Set request context parameters
        RequestContext.get().setAlternatePath(true);
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AlternateREST.unlinkBusinessPolicy(" + policyId + ")");
            }

            // Unlink the business policy from the specified entities
            entitiesStore.unlinkBusinessPolicy(policyId, request.getUnlinkGuids());
        } catch (AtlasBaseException abe) {
            LOG.error("Error in policy unlinking: ", abe);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "During Policy unlinking");
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
        }
    }
}
