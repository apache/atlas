package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.BusinessPolicyRequest;
import org.apache.atlas.model.instance.LinkBusinessPolicyRequest;
import org.apache.atlas.model.instance.UnlinkBusinessPolicyRequest;
import org.apache.atlas.model.instance.MoveBusinessPolicyRequest;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.type.AtlasTypeRegistry;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.BACKEND_SERVICE_USER_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.GOVERNANCE_WORKFLOWS_SERVICE_USER_NAME;

@Path("business-policy")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class BusinessPolicyREST {

    private static final Logger LOG = LoggerFactory.getLogger(BusinessPolicyREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.BusinessPolicyREST");

    private final AtlasEntityStore entitiesStore;
    private final AtlasTypeRegistry typeRegistry;

    @Inject
    public BusinessPolicyREST(AtlasEntityStore entitiesStore, AtlasTypeRegistry typeRegistry) {
        this.entitiesStore = entitiesStore;
        this.typeRegistry = typeRegistry;
    }

    /**
     * Links a business policy to entities.
     * @param request    the request containing the GUIDs of the assets to link the policy to
     * @throws AtlasBaseException if there is an error during the linking process
     */
    @POST
    @Path("/link-business-policy")
    @Timed
    public void linkBusinessPolicy(final BusinessPolicyRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("linkBusinessPolicy");

        if (request.getData() != null) {
            Set<String> assetGuids = request.getData().stream()
                    .map(BusinessPolicyRequest.AssetComplianceInfo::getAssetId)
                    .collect(Collectors.toSet());

            verifyAssetUpdatePermissions(assetGuids, "Policy linking");
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
            entitiesStore.linkBusinessPolicy(request.getData());

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

        verifyAssetUpdatePermissions(request.getUnlinkGuids(), "Policy unlinking");

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
    @Path("/unlink-business-policy/v2")
    @Timed
    public void unlinkBusinessPolicyV2(final UnlinkBusinessPolicyRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("unlinkBusinessPolicyV2");

        if(CollectionUtils.isEmpty(request.getAssetGuids()) || CollectionUtils.isEmpty(request.getUnlinkGuids())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Asset GUIDs or Unlink GUIDs cannot be empty");
        }

        if(request.getAssetGuids().size() > 50 || request.getUnlinkGuids().size() > 50) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Asset GUIDs and Unlink GUIDs should not exceed 50");
        }

        verifyAssetUpdatePermissions(request.getAssetGuids(), "Policy unlinking");

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("x-atlan-route", "business-policy-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "BusinessPolicyREST.unlinkBusinessPolicyV2()");
            }

            // Unlink the business policy from the specified entities
            entitiesStore.unlinkBusinessPolicyV2(request.getAssetGuids(), request.getUnlinkGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }



    private boolean isInvalidRequest(MoveBusinessPolicyRequest request, String assetId) {
        return Objects.isNull(request) ||
                CollectionUtils.isEmpty(request.getPolicyIds()) ||
                Objects.isNull(request.getType()) ||
                Objects.isNull(assetId);
    }

    /**
     * Verifies that the current user has ENTITY_UPDATE permission on the specified assets.
     * Service accounts (atlan-argo, atlan-backend, atlan-governance-workflows) bypass authorization checks.
     * For other users, performs batch authorization check on each asset.
     *
     * @param assetGuids Set of asset GUIDs to verify permissions for
     * @param operation  Description of the operation being performed (for error messages)
     * @throws AtlasBaseException if user lacks permission on any asset
     */
    private void verifyAssetUpdatePermissions(Set<String> assetGuids, String operation) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(assetGuids)) {
            return;
        }

        String currentUser = RequestContext.getCurrentUser();

        boolean isServiceAccount = ARGO_SERVICE_USER_NAME.equals(currentUser)
                || BACKEND_SERVICE_USER_NAME.equals(currentUser)
                || GOVERNANCE_WORKFLOWS_SERVICE_USER_NAME.equals(currentUser);

        if (isServiceAccount) {
            LOG.debug("Service account {} bypassing per-asset authorization for {}", currentUser, operation);
            return;
        }

        LOG.debug("Performing per-asset authorization for user {} on {} assets for {}",
                  currentUser, assetGuids.size(), operation);

        // Batch fetch entity headers for performance (without authorization - we'll check immediately after)
        Map<String, AtlasEntityHeader> headerMap = entitiesStore.getEntityHeadersByIdsWithoutAuthorization(
                new ArrayList<>(assetGuids), null);

        for (String assetGuid : assetGuids) {
            AtlasEntityHeader entityHeader = headerMap.get(assetGuid);

            if (entityHeader == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, assetGuid);
            }

            AtlasEntityAccessRequest accessRequest = new AtlasEntityAccessRequest(
                    typeRegistry,
                    AtlasPrivilege.ENTITY_UPDATE,
                    entityHeader
            );

            AtlasAuthorizationUtils.verifyAccess(
                    accessRequest,
                    operation + " on asset: " + entityHeader.getDisplayText()
            );
        }
    }

}

