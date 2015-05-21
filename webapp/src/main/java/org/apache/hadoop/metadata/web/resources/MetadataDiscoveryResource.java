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

package org.apache.hadoop.metadata.web.resources;

import com.google.common.base.Preconditions;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.discovery.DiscoveryException;
import org.apache.hadoop.metadata.discovery.DiscoveryService;
import org.apache.hadoop.metadata.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Jersey Resource for metadata operations.
 */
@Path("discovery")
@Singleton
public class MetadataDiscoveryResource {

    private static final Logger LOG = LoggerFactory.getLogger(EntityResource.class);
    private static final String QUERY_TYPE_DSL = "dsl";
    private static final String QUERY_TYPE_GREMLIN = "gremlin";
    private static final String QUERY_TYPE_FULLTEXT = "full-text";

    private final DiscoveryService discoveryService;

    /**
     * Created by the Guice ServletModule and injected with the
     * configured DiscoveryService.
     *
     * @param discoveryService metadata service handle
     */
    @Inject
    public MetadataDiscoveryResource(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    /**
     * Search using a given query.
     *
     * @param query search query in raw gremlin or DSL format falling back to full text.
     * @return JSON representing the type and results.
     */
    @GET
    @Path("search")
    @Produces(MediaType.APPLICATION_JSON)
    public Response search(@QueryParam("query") String query) {
        Preconditions.checkNotNull(query, "query cannot be null");

        if (query.startsWith("g.")) { // raw gremlin query
            return searchUsingGremlinQuery(query);
        }

        JSONObject response = null;

        try {   // fall back to dsl
            final String jsonResultStr = discoveryService.searchByDSL(query);
            response = new DSLJSONResponseBuilder().results(jsonResultStr)
                .query(query)
                .build();

        } catch (Throwable throwable) {
            LOG.error("Unable to get entity list for query {} using dsl", query, throwable);

            try {   //fall back to full-text
                final String jsonResultStr = discoveryService.searchByFullText(query);
                response = new FullTextJSonResponseBuilder().results(jsonResultStr)
                    .query(query)
                    .build();

            } catch (DiscoveryException e) {
                LOG.error("Unable to get entity list for query {}", query, e);
                throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
            } catch (JSONException e) {
                LOG.error("Unable to get entity list for query {}", query, e);
                throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
            }
        }

        return Response.ok(response)
            .build();

    }

    /**
     * Search using query DSL format.
     *
     * @param dslQuery search query in DSL format.
     * @return JSON representing the type and results.
     */
    @GET
    @Path("search/dsl")
    @Produces(MediaType.APPLICATION_JSON)
    public Response searchUsingQueryDSL(@QueryParam("query") String dslQuery) {
        Preconditions.checkNotNull(dslQuery, "dslQuery cannot be null");

        try {
            final String jsonResultStr = discoveryService.searchByDSL(dslQuery);

            JSONObject response = new DSLJSONResponseBuilder().results(jsonResultStr)
                .query(dslQuery)
                .build();

            return Response.ok(response)
                .build();
        } catch (DiscoveryException e) {
            LOG.error("Unable to get entity list for dslQuery {}", dslQuery, e);
            throw new WebApplicationException(
                Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to get entity list for dslQuery {}", dslQuery, e);
            throw new WebApplicationException(
                Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Search using raw gremlin query format.
     *
     * @param gremlinQuery search query in raw gremlin format.
     * @return JSON representing the type and results.
     */
    @GET
    @Path("search/gremlin")
    @Produces(MediaType.APPLICATION_JSON)
    public Response searchUsingGremlinQuery(@QueryParam("query") String gremlinQuery) {
        Preconditions.checkNotNull(gremlinQuery, "gremlinQuery cannot be null");

        try {
            final List<Map<String, String>> results = discoveryService
                .searchByGremlin(gremlinQuery);

            JSONObject response = new JSONObject();
            response.put(MetadataServiceClient.REQUEST_ID, Servlets.getRequestId());
            response.put(MetadataServiceClient.QUERY, gremlinQuery);
            response.put(MetadataServiceClient.QUERY_TYPE, QUERY_TYPE_GREMLIN);

            JSONArray list = new JSONArray();
            for (Map<String, String> result : results) {
                list.put(new JSONObject(result));
            }
            response.put(MetadataServiceClient.RESULTS, list);
            response.put(MetadataServiceClient.COUNT, list.length());

            return Response.ok(response)
                .build();
        } catch (DiscoveryException e) {
            LOG.error("Unable to get entity list for gremlinQuery {}", gremlinQuery, e);
            throw new WebApplicationException(
                Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to get entity list for gremlinQuery {}", gremlinQuery, e);
            throw new WebApplicationException(
                Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Search using full text search.
     *
     * @param query search query.
     * @return JSON representing the type and results.
     */
    @GET
    @Path("search/fulltext")
    @Produces(MediaType.APPLICATION_JSON)
    public Response searchUsingFullText(@QueryParam("query") String query) {
        Preconditions.checkNotNull(query, "query cannot be null");

        try {
            final String jsonResultStr = discoveryService.searchByFullText(query);
            JSONArray rowsJsonArr = new JSONArray(jsonResultStr);

            JSONObject response = new FullTextJSonResponseBuilder().results(rowsJsonArr)
                .query(query)
                .build();
            return Response.ok(response)
                .build();
        } catch (DiscoveryException e) {
            LOG.error("Unable to get entity list for query {}", query, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (JSONException e) {
            LOG.error("Unable to get entity list for query {}", query, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
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

            response.put(MetadataServiceClient.REQUEST_ID, Servlets.getRequestId());
            response.put(MetadataServiceClient.QUERY, query);
            response.put(MetadataServiceClient.QUERY_TYPE, queryType);
            response.put(MetadataServiceClient.COUNT, count);
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
            JSONArray rowsJsonArr = dslResults.getJSONArray(MetadataServiceClient.ROWS);
            count(rowsJsonArr.length());
            queryType(QUERY_TYPE_DSL);
            JSONObject response = super.build();
            response.put(MetadataServiceClient.RESULTS, dslResults);
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
            response.put(MetadataServiceClient.RESULTS, fullTextResults);
            return response;
        }
    }
}