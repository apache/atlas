package org.apache.atlas.web.rest;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.DirectSearchRequest;
import org.apache.atlas.model.instance.DirectSearchResponse;
import org.apache.atlas.repository.audit.ESBasedAuditRepository;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.web.util.Servlets;
import org.elasticsearch.action.search.*;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.elasticsearch.common.settings.Settings;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

@Path("direct")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class DirectSearch {
    private static final long DEFAULT_KEEPALIVE = 60000L; // 60 seconds
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.DirectSearch");
    private static final Logger LOG = LoggerFactory.getLogger(DirectSearch.class);
    private static final NamedXContentRegistry REGISTRY;

    static {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    private final ESBasedAuditRepository es;

    @Inject
    public DirectSearch(ESBasedAuditRepository esBasedAuditRepository) {
        this.es = esBasedAuditRepository;
    }

    @Path("/search")
    @POST
    @Timed
    public DirectSearchResponse directSearch(DirectSearchRequest request) throws AtlasBaseException, IOException {
        if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Direct search");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DirectSearch.directSearch()");
            }

            LOG.debug("==> DirectSearch.directSearch({})", request);
            validateRequest(request);

            DirectSearchResponse response = switch (request.getSearchType()) {
                case SIMPLE -> DirectSearchResponse.fromSearchResponse(handleSimpleSearch(request));
                case PIT_CREATE -> DirectSearchResponse.fromPitCreateResponse(handlePitCreate(request));
                case PIT_SEARCH -> DirectSearchResponse.fromSearchResponse(handlePitSearch(request));
                case PIT_DELETE -> DirectSearchResponse.fromPitDeleteResponse(handlePitDelete(request));
            };

            LOG.debug("<== DirectSearch.directSearch() - {}", response);
            return response;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void validateRequest(DirectSearchRequest request) throws AtlasBaseException {
        if (request == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Request cannot be null");
        }

        if (request.getSearchType() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Search type is required");
        }

        switch (request.getSearchType()) {
            case SIMPLE -> validateSimpleSearch(request);
            case PIT_CREATE -> validatePitCreate(request);
            case PIT_SEARCH -> validatePitSearch(request);
            case PIT_DELETE -> validatePitDelete(request);
        }
    }

    private void validateSimpleSearch(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getIndexName() == null || request.getIndexName().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Index name is required for simple search");
        }
        if (request.getQuery() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Query is required for simple search");
        }
    }

    private void validatePitCreate(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getIndexName() == null || request.getIndexName().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Index name is required for PIT creation");
        }
    }

    private void validatePitSearch(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getPitId() == null || request.getPitId().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "PIT ID is required for PIT search");
        }
        if (request.getQuery() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Query is required for PIT search");
        }
    }

    private void validatePitDelete(DirectSearchRequest request) throws AtlasBaseException {
        if (request.getPitId() == null || request.getPitId().trim().isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "PIT ID is required for PIT deletion");
        }
    }

    private SearchSourceBuilder buildSearchSource(DirectSearchRequest request) throws Exception {
        try {
            String queryJson = AtlasJson.toJson(request.getQuery());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(REGISTRY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            queryJson)) {

                SearchSourceBuilder sourceBuilder = SearchSourceBuilder.fromXContent(parser);

                if (request.getSearchAfter() != null) {
                    sourceBuilder.searchAfter(request.getSearchAfter().toArray());
                }

                return sourceBuilder;
            }
        } catch (Exception e) {
            LOG.error("Error building search source from query: {}", request.getQuery(), e);
            throw new Exception("Failed to parse search query: " + e.getMessage(), e);
        }
    }

    private SearchResponse handleSimpleSearch(DirectSearchRequest request) throws IOException, AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handleSimpleSearch(indexName={}, query={})",
                    request.getIndexName(), request.getQuery());
            if (request.getIndexName() == null) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Search index cannot be null");
            }

            SearchSourceBuilder sourceBuilder = buildSearchSource(request);
            SearchRequest searchRequest = new SearchRequest(request.getIndexName());
            searchRequest.source(sourceBuilder);

            SearchResponse response = es.search(searchRequest);
            LOG.debug("<== DirectSearch.handleSimpleSearch() - {}", response);
            return response;
        } catch (IOException e) {
            LOG.error("Error in simple search", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private OpenPointInTimeResponse handlePitCreate(DirectSearchRequest request) throws AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handlePitCreate(indexName={})", request.getIndexName());

            long keepAlive = request.getKeepAlive() != null ? request.getKeepAlive() : DEFAULT_KEEPALIVE;
            OpenPointInTimeRequest pitRequest = new OpenPointInTimeRequest(request.getIndexName())
                    .keepAlive(TimeValue.timeValueMillis(keepAlive));

            OpenPointInTimeResponse response = es.openPointInTime(pitRequest);
            LOG.debug("<== DirectSearch.handlePitCreate() - {}", response);
            return response;
        } catch (Exception e) {
            LOG.error("Error creating PIT", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private SearchResponse handlePitSearch(DirectSearchRequest request) throws AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handlePitSearch(pitId={}, query={})", request.getPitId(), request.getQuery());

            // Add PIT to source builder
            PointInTimeBuilder pitBuilder = new PointInTimeBuilder(request.getPitId());
            
            // Only set keepAlive if specified
            if (request.getKeepAlive() != null) {
                pitBuilder.setKeepAlive(String.valueOf(request.getKeepAlive()));
            }
            SearchSourceBuilder sourceBuilder = buildSearchSource(request);
            sourceBuilder.pointInTimeBuilder(pitBuilder);

            // For PIT search, do not specify any index
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(sourceBuilder);

            SearchResponse response = es.search(searchRequest);
            LOG.debug("<== DirectSearch.handlePitSearch() - {}", response);
            return response;
        } catch (IOException e) {
            LOG.error("Error in PIT search", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }

    private ClosePointInTimeResponse handlePitDelete(DirectSearchRequest request) throws AtlasBaseException {
        try {
            LOG.debug("==> DirectSearch.handlePitDelete(pitId={})", request.getPitId());

            ClosePointInTimeRequest closeRequest = new ClosePointInTimeRequest(request.getPitId());
            ClosePointInTimeResponse response = es.closePointInTime(closeRequest);

            LOG.debug("<== DirectSearch.handlePitDelete() - {}", response);
            return response;
        } catch (Exception e) {
            LOG.error("Error deleting PIT", e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED, e.getMessage());
        }
    }
}
