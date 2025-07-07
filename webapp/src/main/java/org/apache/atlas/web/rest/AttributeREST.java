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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

@Path("attribute")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})

public class AttributeREST {
    private static final Logger LOG = LoggerFactory.getLogger(BusinessPolicyREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.AttributeREST");

    private final AtlasEntityStore entitiesStore;
    public AttributeREST(AtlasEntityStore entitiesStore) {
        this.entitiesStore = entitiesStore;
    }

    @POST
    @Path("/update")
    @Timed
    public void updateAttribute(final AttributeUpdateRequest request) throws AtlasBaseException {
        // Validate the size of the request data
        if (request.getData() == null || request.getData().size() > 50) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Request data size exceeds the limit of 50 attributes");
        }

        // Ensure the current user is authorized to link policies
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Attribute update");
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
            // Update attribute
            entitiesStore.attributeUpdate(request.getData());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
            RequestContext.get().endMetricRecord(metric);
        }
    }


}
