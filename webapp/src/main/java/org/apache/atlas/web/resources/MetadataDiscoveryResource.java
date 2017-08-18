/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.resources;

import com.google.common.base.Preconditions;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.classification.InterfaceAudience;
import org.apache.atlas.discovery.DiscoveryException;
import org.apache.atlas.discovery.DiscoveryService;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Jersey Resource for metadata operations.
 */
@Path("discovery")
@Singleton
@Service
@Deprecated
public class MetadataDiscoveryResource {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDiscoveryResource.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.MetadataDiscoveryResource");
    private static final String QUERY_TYPE_DSL = "dsl";
    private static final String QUERY_TYPE_GREMLIN = "gremlin";
    private static final String QUERY_TYPE_FULLTEXT = "full-text";
    private static final String LIMIT_OFFSET_DEFAULT = "-1";

    private final DiscoveryService discoveryService;

    private final  boolean       gremlinSearchEnabled;
    private static Configuration applicationProperties          = null;
    private static final String  ENABLE_GREMLIN_SEARCH_PROPERTY = "atlas.search.gremlin.enable";

    /**
     * Created by the Guice ServletModule and injected with the
     * configured DiscoveryService.
     *
     * @param discoveryService metadata service handle
     */
    @Inject
    public MetadataDiscoveryResource(DiscoveryService discoveryService, Configuration configuration) {
        this.discoveryService  = discoveryService;
        applicationProperties  = configuration;
        gremlinSearchEnabled   = applicationProperties != null && applicationProperties.getBoolean(ENABLE_GREMLIN_SEARCH_PROPERTY, false);
    }

    /**
     * Search using a given query.
     *
     * @param query search query in DSL format falling back to full text.
     * @param limit number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     * @return JSON representing the type and results.
     */
    @GET
    @Path("search")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response search(@QueryParam("query") String query,
                           @DefaultValue(LIMIT_OFFSET_DEFAULT) @QueryParam("limit") int limit,
                           @DefaultValue(LIMIT_OFFSET_DEFAULT) @QueryParam("offset") int offset) {
        boolean dslQueryFailed = false;
        Response response = null;
        try {
            response = searchUsingQueryDSL(query, limit, offset);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                dslQueryFailed = true;
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error while running DSL. Switching to fulltext for query {}", query, e);
            }

            dslQueryFailed = true;
        }

        if ( dslQueryFailed ) {
            response = searchUsingFullText(query, limit, offset);
        }

        return response;
    }

    /**
     * Search using query DSL format.
     *
     * @param dslQuery search query in DSL format.
     * @param limit number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     * Limit and offset in API are used in conjunction with limit and offset in DSL query
     * Final limit = min(API limit, max(query limit - API offset, 0))
     * Final offset = API offset + query offset
     *
     * @return JSON representing the type and results.
     */
    @GET
    @Path("search/dsl")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response searchUsingQueryDSL(@QueryParam("query") String dslQuery,
                                        @DefaultValue(LIMIT_OFFSET_DEFAULT) @QueryParam("limit") int limit,
                                        @DefaultValue(LIMIT_OFFSET_DEFAULT) @QueryParam("offset") int offset) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> MetadataDiscoveryResource.searchUsingQueryDSL({}, {}, {})", dslQuery, limit, offset);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MetadataDiscoveryResource.searchUsingQueryDSL(" + dslQuery + ", " + limit + ", " + offset + ")");
            }

            dslQuery = ParamChecker.notEmpty(dslQuery, "dslQuery cannot be null");
            QueryParams queryParams = validateQueryParams(limit, offset);
            final String jsonResultStr = discoveryService.searchByDSL(dslQuery, queryParams);

            JSONObject response = new DSLJSONResponseBuilder().results(jsonResultStr).query(dslQuery).build();

            return Response.ok(response).build();
        } catch (DiscoveryException | IllegalArgumentException e) {
            LOG.error("Unable to get entity list for dslQuery {}", dslQuery, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (WebApplicationException e) {
            LOG.error("Unable to get entity list for dslQuery {}", dslQuery, e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to get entity list for dslQuery {}", dslQuery, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== MetadataDiscoveryResource.searchUsingQueryDSL({}, {}, {})", dslQuery, limit, offset);
            }
        }
    }

    private QueryParams validateQueryParams(int limitParam, int offsetParam) {
        int maxLimit = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();
        int defaultLimit = AtlasConfiguration.SEARCH_DEFAULT_LIMIT.getInt();

        int limit = defaultLimit;
        boolean limitSet = (limitParam != Integer.valueOf(LIMIT_OFFSET_DEFAULT));
        if (limitSet) {
            ParamChecker.lessThan(limitParam, maxLimit, "limit");
            ParamChecker.greaterThan(limitParam, 0, "limit");
            limit = limitParam;
        }

        int offset = 0;
        boolean offsetSet = (offsetParam != Integer.valueOf(LIMIT_OFFSET_DEFAULT));
        if (offsetSet) {
            ParamChecker.greaterThan(offsetParam, -1, "offset");
            offset = offsetParam;
        }

        return new QueryParams(limit, offset);
    }

    /**
     * Search using raw gremlin query format.
     *
     * @param gremlinQuery search query in raw gremlin format.
     * @return JSON representing the type and results.
     */
    @GET
    @Path("search/gremlin")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    @InterfaceAudience.Private
    public Response searchUsingGremlinQuery(@QueryParam("query") String gremlinQuery) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> MetadataDiscoveryResource.searchUsingGremlinQuery({})", gremlinQuery);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MetadataDiscoveryResource.searchUsingGremlinQuery(" + gremlinQuery + ")");
            }

            if (!gremlinSearchEnabled) {
                throw new DiscoveryException("Gremlin search is not enabled.");
            }

            gremlinQuery = ParamChecker.notEmpty(gremlinQuery, "gremlinQuery cannot be null or empty");
            final List<Map<String, String>> results = discoveryService.searchByGremlin(gremlinQuery);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.QUERY, gremlinQuery);
            response.put(AtlasClient.QUERY_TYPE, QUERY_TYPE_GREMLIN);

            JSONArray list = new JSONArray();
            for (Map<String, String> result : results) {
                list.put(new JSONObject(result));
            }
            response.put(AtlasClient.RESULTS, list);
            response.put(AtlasClient.COUNT, list.length());

            return Response.ok(response).build();
        } catch (DiscoveryException | IllegalArgumentException e) {
            LOG.error("Unable to get entity list for gremlinQuery {}", gremlinQuery, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (WebApplicationException e) {
            LOG.error("Unable to get entity list for gremlinQuery {}", gremlinQuery, e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to get entity list for gremlinQuery {}", gremlinQuery, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== MetadataDiscoveryResource.searchUsingGremlinQuery({})", gremlinQuery);
            }
        }
    }

    /**
     * Search using full text search.
     *
     * @param query search query.
     * @param limit number of rows to be returned in the result, used for pagination. maxlimit > limit > 0. -1 maps to atlas.search.defaultlimit property value
     * @param offset offset to the results returned, used for pagination. offset >= 0. -1 maps to offset 0
     * @return JSON representing the type and results.
     */
    @GET
    @Path("search/fulltext")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response searchUsingFullText(@QueryParam("query") String query,
                                        @DefaultValue(LIMIT_OFFSET_DEFAULT) @QueryParam("limit") int limit,
                                        @DefaultValue(LIMIT_OFFSET_DEFAULT) @QueryParam("offset") int offset) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> MetadataDiscoveryResource.searchUsingFullText({}, {}, {})", query, limit, offset);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MetadataDiscoveryResource.searchUsingFullText(" + query + ", " + limit + ", " + offset + ")");
            }

            query = ParamChecker.notEmpty(query, "query cannot be null or empty");
            QueryParams queryParams = validateQueryParams(limit, offset);
            final String jsonResultStr = discoveryService.searchByFullText(query, queryParams);
            JSONArray rowsJsonArr = new JSONArray(jsonResultStr);

            JSONObject response = new FullTextJSonResponseBuilder().results(rowsJsonArr).query(query).build();
            return Response.ok(response).build();
        } catch (DiscoveryException | IllegalArgumentException e) {
            LOG.error("Unable to get entity list for query {}", query, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (WebApplicationException e) {
            LOG.error("Unable to get entity list for query {}", query, e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to get entity list for query {}", query, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== MetadataDiscoveryResource.searchUsingFullText({}, {}, {})", query, limit, offset);
            }
        }
    }

    private class JsonResponseBuilder {

        protected int count = 0;
        protected String query;
        protected String queryType;
        protected JSONObject response;

        JsonResponseBuilder() {
            this.response = new JSONObject();
        }

        protected JsonResponseBuilder count(int count) {
            this.count = count;
            return this;
        }

        public JsonResponseBuilder query(String query) {
            this.query = query;
            return this;
        }

        public JsonResponseBuilder queryType(String queryType) {
            this.queryType = queryType;
            return this;
        }

        protected JSONObject build() throws JSONException {

            Preconditions.checkNotNull(query, "Query cannot be null");
            Preconditions.checkNotNull(queryType, "Query Type must be specified");
            Preconditions.checkArgument(count >= 0, "Search Result count should be > 0");

            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.QUERY, query);
            response.put(AtlasClient.QUERY_TYPE, queryType);
            response.put(AtlasClient.COUNT, count);
            return response;
        }
    }

    private class DSLJSONResponseBuilder extends JsonResponseBuilder {

        DSLJSONResponseBuilder() {
            super();
        }

        private JSONObject dslResults;

        public DSLJSONResponseBuilder results(JSONObject dslResults) {
            this.dslResults = dslResults;
            return this;
        }

        public DSLJSONResponseBuilder results(String dslResults) throws JSONException {
            return results(new JSONObject(dslResults));
        }

        @Override
        public JSONObject build() throws JSONException {
            Preconditions.checkNotNull(dslResults);
            JSONArray rowsJsonArr = dslResults.getJSONArray(AtlasClient.ROWS);
            count(rowsJsonArr.length());
            queryType(QUERY_TYPE_DSL);
            JSONObject response = super.build();
            response.put(AtlasClient.RESULTS, rowsJsonArr);
            response.put(AtlasClient.DATATYPE, dslResults.get(AtlasClient.DATATYPE));
            return response;
        }

    }

    private class FullTextJSonResponseBuilder extends JsonResponseBuilder {

        private JSONArray fullTextResults;

        public FullTextJSonResponseBuilder results(JSONArray fullTextResults) {
            this.fullTextResults = fullTextResults;
            return this;
        }

        public FullTextJSonResponseBuilder results(String dslResults) throws JSONException {
            return results(new JSONArray(dslResults));
        }

        public FullTextJSonResponseBuilder() {
            super();
        }

        @Override
        public JSONObject build() throws JSONException {
            Preconditions.checkNotNull(fullTextResults);
            count(fullTextResults.length());
            queryType(QUERY_TYPE_FULLTEXT);

            JSONObject response = super.build();
            response.put(AtlasClient.RESULTS, fullTextResults);
            return response;
        }
    }
}
