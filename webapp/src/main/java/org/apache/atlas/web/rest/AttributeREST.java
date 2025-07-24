package org.apache.atlas.web.rest;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AttributeUpdateRequest;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.List;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

@Path("attribute")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})

public class AttributeREST {
    private static final Logger LOG = LoggerFactory.getLogger(AttributeREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.AttributeREST");
    private static final String X_ATLAN_CLIENT_ORIGIN = "x-atlan-client-origin";

    private final AtlasEntityStore entitiesStore;
    public AttributeREST(AtlasEntityStore entitiesStore) {
        this.entitiesStore = entitiesStore;
    }

    @POST
    @Path("/update")
    @Timed
    public void updateAttribute(@Context HttpHeaders headers, final AttributeUpdateRequest request) throws AtlasBaseException {
        // Validate the size of the request data
        if (request.getData() == null || request.getData().size() > 50) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Request data size exceeds the limit of 50 attributes");
        }

        // Ensure the current user is authorized to trigger this endpoint
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Attribute update");
        }

        // Validate required headers
        List<String> clientOrigin = headers.getRequestHeader(X_ATLAN_CLIENT_ORIGIN);
        if (CollectionUtils.isEmpty(clientOrigin)) {
            LOG.error("Required header {} is missing or empty", X_ATLAN_CLIENT_ORIGIN);
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Required header x-atlan-client-origin is missing or empty");
        }

        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("updateAttribute");

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(false);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("x-atlan-route", "attribute-update-rest");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AttributeREST.updateAttribute()");
            }
            LOG.debug("==> AttributeREST.updateAttribute(request={}, client-origin={})", request, clientOrigin);
            // Update attribute
            entitiesStore.attributeUpdate(request.getData());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }
}
