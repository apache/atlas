/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.DirectIndexQueryResult;
import org.apache.atlas.repository.graphdb.janus.cassandra.ESConnector;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasMetricType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.janusgraph.util.encoding.LongEncoding;
import org.redisson.client.RedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.apache.atlas.AtlasErrorCode.INDEX_NOT_FOUND;
import static org.apache.atlas.repository.Constants.LEANGRAPH_MODE;
import static org.apache.atlas.repository.Constants.LEAN_GRAPH_ENABLED;


public class AtlasElasticsearchQuery implements AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasElasticsearchQuery.class);

    public static final String CLIENT_ORIGIN_PRODUCT = "product_webapp";
    public static final String CLIENT_ORIGIN_PLAYBOOK = "playbook";

    private AtlasJanusGraph graph;
    private RestHighLevelClient esClient;
    private RestClient lowLevelRestClient;

    private RestClient esUiClusterClient;
    private RestClient esNonUiClusterClient;
    private String index;
    private SearchSourceBuilder sourceBuilder;
    private SearchResponse searchResponse;
    private SearchParams searchParams;
    private long vertexTotals = -1;

    public AtlasElasticsearchQuery(AtlasJanusGraph graph, RestHighLevelClient esClient, String index, SearchSourceBuilder sourceBuilder) {
        this(graph, index);
        this.esClient = esClient;
        this.sourceBuilder = sourceBuilder;
    }

    public AtlasElasticsearchQuery(AtlasJanusGraph graph, RestClient restClient, String index, SearchParams searchParams, RestClient esUiClusterClient, RestClient esNonUiClusterClient) {
        this(graph, index);
        this.lowLevelRestClient = restClient;
        this.esUiClusterClient = esUiClusterClient;
        this.esNonUiClusterClient = esNonUiClusterClient;
        this.searchParams = searchParams;
    }

    private AtlasElasticsearchQuery(AtlasJanusGraph graph, String index) {
        this.index = index;
        this.graph = graph;
        searchResponse = null;
    }

    public AtlasElasticsearchQuery(AtlasJanusGraph graph, String index, RestClient restClient, RestClient esUiClusterClient, RestClient esNonUiClusterClient) {
        this(graph, restClient, index, null, esUiClusterClient, esNonUiClusterClient);
    }

    public AtlasElasticsearchQuery(String index, RestClient restClient) {
        this.lowLevelRestClient = restClient;
        this.index = index;
    }

    private SearchRequest getSearchRequest(String index, SearchSourceBuilder sourceBuilder) {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    /**
     * Returns the appropriate Elasticsearch RestClient based on client origin and configuration settings if isolation is enabled.
     *
     * @return RestClient configured for either UI or Non-UI cluster, falling back to low-level client
     */
    private RestClient getESClient() {
        if (!AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_REQUEST_ISOLATION.getBoolean()) {
            return lowLevelRestClient;
        }

        try {
            String clientOrigin = RequestContext.get().getClientOrigin();
            if (clientOrigin == null) {
                return lowLevelRestClient;
            }
            if (CLIENT_ORIGIN_PRODUCT.equals(clientOrigin)) {
                return Optional.ofNullable(esUiClusterClient)
                        .orElse(lowLevelRestClient);
            } else {
                return Optional.ofNullable(esNonUiClusterClient)
                        .orElse(lowLevelRestClient);
            }

        } catch (Exception e) {
            LOG.error("Error determining ES client, falling back to low-level client", e);
            return lowLevelRestClient;
        }
    }

    private Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> runQuery(SearchRequest searchRequest) {
        Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> result = null;

        try {
            RequestOptions requestOptions = RequestOptions.DEFAULT;
            RequestOptions.Builder builder = requestOptions.toBuilder();
            int bufferLimit = 2000 * 1024 * 1024;
            builder.setHttpAsyncResponseConsumerFactory(
                    new HttpAsyncResponseConsumerFactory
                            .HeapBufferedResponseConsumerFactory(bufferLimit));
            requestOptions = builder.build();
            searchResponse = esClient.search(searchRequest, requestOptions);
            Stream<Result<AtlasJanusVertex, AtlasJanusEdge>> resultStream = Arrays.stream(searchResponse.getHits().getHits())
                    .map(ResultImpl::new);
            result = resultStream.iterator();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private DirectIndexQueryResult runQueryWithLowLevelClient(SearchParams searchParams) throws AtlasBaseException {
        DirectIndexQueryResult result = null;

        try {
            if(searchParams.isCallAsync() || AtlasConfiguration.ENABLE_ASYNC_INDEXSEARCH.getBoolean()) {
                return performAsyncDirectIndexQuery(searchParams);
            } else {
                String responseString =  performDirectIndexQuery(searchParams.getQuery(), false);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("runQueryWithLowLevelClient.response : {}", responseString);
                }
                return getResultFromResponse(responseString);
            }
        } catch (IOException e) {
            LOG.error("Failed to execute direct query on ES {}", e.getMessage());

            handleNetworkErrors(e);

            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED, e.getMessage());
        }
    }

    private Map<String, Object> runQueryWithLowLevelClient(String query) throws AtlasBaseException {
        Map<String, Object> ret = new HashMap<>();
        try {
            String responseString = performDirectIndexQuery(query, true);

            Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(responseString, Map.class);
            Map<String, LinkedHashMap> hits_0 = AtlasType.fromJson(AtlasType.toJson(responseMap.get("hits")), Map.class);

            ret.put("total", hits_0.get("total").get("value"));

            List<LinkedHashMap> hits_1 = AtlasType.fromJson(AtlasType.toJson(hits_0.get("hits")), List.class);
            ret.put("data", hits_1);

            Map<String, Object> aggregationsMap = (Map<String, Object>) responseMap.get("aggregations");

            if (MapUtils.isNotEmpty(aggregationsMap)) {
                ret.put("aggregations", aggregationsMap);
            }

            return ret;

        } catch (IOException e) {
            LOG.error("Failed to execute direct query on ES {}", e.getMessage());

            handleNetworkErrors(e);

            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED, e.getMessage());
        }
    }

    private Map<String, Object> runRawQueryWithLowLevelClient(String query) throws AtlasBaseException {
        try {
            String responseString = performDirectIndexQuery(query, true);
            Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);
            return responseMap;
        } catch (IOException e) {
            LOG.error("Failed to execute raw direct query on ES {}", e.getMessage());

            handleNetworkErrors(e);

            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED, e.getMessage());
        }
    }

    private Long runCountWithLowLevelClient(String query) throws AtlasBaseException {
        try {
            String responseString = performDirectCountQuery(query);
            Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);
            Object countObj = responseMap.get("count");
            if (countObj == null) {
                return 0L;
            }
            long countVal;
            if (countObj instanceof Number) {
                countVal = ((Number) countObj).longValue();
            } else {
                countVal = Long.parseLong(String.valueOf(countObj));
            }
            return countVal;
        } catch (IOException e) {
            LOG.error("Failed to execute _count query on ES {}", e.getMessage());

            handleNetworkErrors(e);

            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED, e.getMessage());
        }
    }

    private Map<String, LinkedHashMap> runUpdateByQueryWithLowLevelClient(String query) throws AtlasBaseException {
        try {
            String responseString = performDirectUpdateByQuery(query);

            Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(responseString, Map.class);
            return responseMap;

        } catch (IOException e) {
            LOG.error("Failed to execute direct query on ES {}", e.getMessage());

            handleNetworkErrors(e);

            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED, e.getMessage());
        }
    }

    private DirectIndexQueryResult performAsyncDirectIndexQuery(SearchParams searchParams) throws AtlasBaseException, IOException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("performAsyncDirectIndexQuery");
        DirectIndexQueryResult result = null;
        boolean contextIdExists = StringUtils.isNotEmpty(searchParams.getSearchContextId()) && searchParams.getSearchContextSequenceNo() != null;
        try {
            if(contextIdExists) {
                // If the search context id and greater sequence no is present,
                // then we need to delete the previous search context async
                    processRequestWithSameSearchContextId(searchParams);
            }

            String KeepAliveTime = AtlasConfiguration.INDEXSEARCH_ASYNC_SEARCH_KEEP_ALIVE_TIME_IN_SECONDS.getLong() +"s";
            if (searchParams.getRequestTimeoutInSecs() !=  null) {
                KeepAliveTime = searchParams.getRequestTimeoutInSecs() +"s";
            }
            AsyncQueryResult response = submitAsyncSearch(searchParams, KeepAliveTime, false).get();
            if(response.isRunning()) {
                /*
                    * If the response is still running, then we need to wait for the response
                    * We need to check if the search context ID is present and update the cache
                    * We also need to check if the search ID exists and delete if necessary, if the sequence number is greater than the cache sequence number
                    *
                 */
                String esSearchId = response.getId();
                String searchContextId = searchParams.getSearchContextId();
                Integer searchContextSequenceNo = searchParams.getSearchContextSequenceNo();
                if (contextIdExists) {
                    CompletableFuture.runAsync(() -> SearchContextCache.put(searchContextId, searchContextSequenceNo, esSearchId));
                }
                response = getAsyncSearchResponse(searchParams, esSearchId).get();
                if (response == null) {
                    // If the response is null, that request was cancelled by user return HTTP 204
                    return null;
                }

                if (response.isTimedOut()) {
                    LOG.error("timeout exceeded for query {}:", searchParams.getQuery());
                    RequestContext.get().endMetricRecord(RequestContext.get().startMetricRecord("elasticQueryTimeout"));
                    throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED_DUE_TO_TIMEOUT, KeepAliveTime);
                }
                result = getResultFromResponse(response.getFullResponse(), true);
            } else {
                result = getResultFromResponse(response.getFullResponse(), true);
            }
        }catch (Exception e) {
            LOG.error("Failed to execute async query on ES {}", e.getMessage());

            if (e instanceof IOException) {
                handleNetworkErrors((IOException) e);
            }

            if (e instanceof ResponseException) {
                int statusCode = ((ResponseException) e).getResponse().getStatusLine().getStatusCode();
                throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED_WITH_RESPONSE_CODE, "Elasticsearch returned status code: " + statusCode + ", message: " + e.getMessage());
            }

            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED, e.getMessage());
        } finally {
            if (contextIdExists) {
                // If the search context id is present, then we need to remove the search context from the cache
                try {
                    CompletableFuture.runAsync(() -> SearchContextCache.remove(searchParams.getSearchContextId()));
                } catch (Exception e) {
                    LOG.error("Failed to remove the search context from the cache {}", e.getMessage());
                }
            }
            RequestContext.get().endMetricRecord(metric);
        }
        return result;
    }

    /*
     * Checks if the exception is a network-related issue and throws a SERVICE_UNAVAILABLE error.
     */
    private void handleNetworkErrors(Exception e) throws AtlasBaseException {
        if (e instanceof SocketTimeoutException ||
                e instanceof UnknownHostException ||
                containsMessage(e, "connection reset by peer", "network error", "connection refused")) {

            throw new AtlasBaseException(AtlasErrorCode.SERVICE_UNAVAILABLE,
                    "Service is unavailable or a network error occurred: Elasticsearch - " + e.getMessage());
        }

        if (containsMessage(e, "gateway timeout")) {
            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_GATEWAY_TIMEOUT, e.getMessage());
        }
    }

    private boolean containsMessage(Exception e, String... keywords) {
        if (e.getMessage() == null) return false;

        String message = e.getMessage().toLowerCase();
        return Arrays.stream(keywords).anyMatch(message::contains);
    }

    /*
        * Process the request with the same search context ID and sequence number
        * @param searchParams
        * @return void
        * Function to process the request with the same search context ID
        * If the sequence number is greater than the cache sequence number,
        * then we need to cancel the request and update the cache
        * We also need to check if the search ID exists and delete if necessary
     */
    private void processRequestWithSameSearchContextId(SearchParams searchParams) {
        AtlasPerfMetrics.MetricRecorder funcMetric = RequestContext.get().startMetricRecord("processRequestWithSameSearchContextId");
        try {
            // Extract search context ID and sequence number
            String currentSearchContextId = searchParams.getSearchContextId();
            Integer currentSequenceNumber = searchParams.getSearchContextSequenceNo();
            // Get the search ID from the cache if sequence number is greater than the current sequence number
            String previousESSearchId = SearchContextCache.getESAsyncSearchIdFromContextCache(currentSearchContextId, currentSequenceNumber);

            if (StringUtils.isNotEmpty(previousESSearchId)) {
                LOG.debug("Deleting the previous async search response with ID {}", previousESSearchId);
                // If the search ID exists, then we need to delete the search context
                deleteAsyncSearchResponse(previousESSearchId);
            }
        } catch (RedisException e) {
            AtlasPerfMetrics.Metric failureCounter = new AtlasPerfMetrics.Metric("async_request_redis_failure_counter");
            failureCounter.setMetricType(AtlasMetricType.COUNTER);
            failureCounter.incrementInvocations();
            LOG.error("Failed to process the request with the same search context ID {}", e.getMessage());
            RequestContext.get().addApplicationMetrics(failureCounter);
        }
        catch (Exception e) {
            LOG.error("Failed to process the request with the same search context ID {}", e.getMessage());
        }
        finally {
            RequestContext.get().endMetricRecord(funcMetric);
        }
    }

    /*
        * Get the async search response
        * @param searchParams
        * @param esSearchId
        * @return Future<AsyncQueryResult>
        * Function to get the async search response after we submit the async search request
     */
    private Future<AsyncQueryResult> getAsyncSearchResponse(SearchParams searchParams, String esSearchId)  {
        CompletableFuture<AsyncQueryResult> future = new CompletableFuture<>();
        String endPoint = "_async_search/" + esSearchId;
        Request request = new Request("GET", endPoint);
        long waitTime = AtlasConfiguration.INDEXSEARCH_ASYNC_SEARCH_KEEP_ALIVE_TIME_IN_SECONDS.getLong();
        if (searchParams.getRequestTimeoutInSecs()!= null) {
            waitTime = searchParams.getRequestTimeoutInSecs();
        }
        request.addParameter("wait_for_completion_timeout", waitTime + "s");
        ResponseListener responseListener = new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    String respString = EntityUtils.toString(response.getEntity());
                    Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(respString, Map.class);
                    Boolean isInComplete = AtlasType.fromJson(AtlasType.toJson(responseMap.get("is_partial")), Boolean.class);
                    String id = AtlasType.fromJson(AtlasType.toJson(responseMap.get("id")), String.class);
                    boolean isTimedOut = AtlasType.fromJson(AtlasType.toJson(responseMap.get("response").get("timed_out")), Boolean.class);

                    if (isInComplete != null && isInComplete) {
                        /*
                           * After the wait time, if the response is still incomplete, then we need to delete the search context
                           * and complete the future with null
                           * So that ES don't process the request later
                         */
                        deleteAsyncSearchResponse(id);
                        future.complete(null);
                    }
                    AsyncQueryResult result = new AsyncQueryResult(respString, false, isTimedOut);
                    future.complete(result);
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException){
                    int statusCode = ((ResponseException) exception).getResponse().getStatusLine().getStatusCode();
                    if (statusCode == 400 || statusCode == 404) {
                        /*
                            * If the response is not found or deleted, then we need to complete the future with null
                            * Else we need to complete the future with the exception
                            * Sending null, would return 204 to the user
                         */
                        LOG.debug("Async search response not found or deleted", exception);
                        future.complete(null);
                    } else {
                        future.completeExceptionally(exception);
                    }
                } else {
                    future.completeExceptionally(exception);
                }
            }
        };

        getESClient().performRequestAsync(request, responseListener);

        return future;
    }

    private void deleteAsyncSearchResponse(String searchContextId) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("deleteAsyncSearchResponse");
        try {
            String endPoint = "_async_search/" + searchContextId;
            Request request = new Request("DELETE", endPoint);
            ResponseListener responseListener = new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    LOG.debug("Deleted async search response");
                }
                @Override
                public void onFailure(Exception exception) {
                    if (exception instanceof ResponseException && ((ResponseException) exception).getResponse().getStatusLine().getStatusCode() == 404) {
                        LOG.debug("Async search response not found");
                    } else {
                        LOG.error("Failed to delete async search response {}", exception.getMessage());
                    }
                }
            };
            getESClient().performRequestAsync(request, responseListener);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private Future<AsyncQueryResult> submitAsyncSearch(SearchParams searchParams, String KeepAliveTime, boolean source) {
        CompletableFuture<AsyncQueryResult> future = new CompletableFuture<>();
        HttpEntity entity = new NStringEntity(searchParams.getQuery(), ContentType.APPLICATION_JSON);
        String endPoint;

        if (source) {
            endPoint = index + "/_async_search";
        } else {
            endPoint = index + "/_async_search?_source=false";
        }

        Request request = new Request("POST", endPoint);
        request.setEntity(entity);

        request.addParameter("wait_for_completion_timeout", "100ms");
        request.addParameter("keep_alive", KeepAliveTime);

        ResponseListener responseListener = new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    String respString = EntityUtils.toString(response.getEntity());
                    Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(respString, Map.class);
                    boolean isRunning = AtlasType.fromJson(AtlasType.toJson(responseMap.get("is_running")), Boolean.class);
                    String id = AtlasType.fromJson(AtlasType.toJson(responseMap.get("id")), String.class);
                    boolean isTimedOut = AtlasType.fromJson(AtlasType.toJson(responseMap.get("response").get("timed_out")), Boolean.class);
                    AsyncQueryResult result = new AsyncQueryResult(respString, isRunning, isTimedOut);
                    /*
                        * If the response is running, then we need to complete the future with the ID to retrieve this later
                        * Else we will complete the future with the response, if it completes within default timeout of 100ms
                     */
                    if (isRunning && StringUtils.isNotEmpty(id)) {
                        // response is still running, complete the future with the ID
                        // use the ID to retrieve the response later
                        result.setId(id);
                        future.complete(result);
                    } else {
                        future.complete(result);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public void onFailure(Exception exception) {
                future.completeExceptionally(exception);
            }
        };

        getESClient().performRequestAsync(request, responseListener);

        return future;
    }

    private String performDirectIndexQuery(String query, boolean source) throws AtlasBaseException, IOException {
        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        String endPoint;

        if (source) {
            endPoint = index + "/_search";
        } else {
            endPoint = index + "/_search?_source=false";
        }

        Request request = new Request("GET", endPoint);
        request.setEntity(entity);

        Response response;
        try {
            response = getESClient().performRequest(request);
        } catch (ResponseException rex) {
            if (rex.getResponse().getStatusLine().getStatusCode() == 404) {
                LOG.warn(String.format("ES index with name %s not found", index));
                throw new AtlasBaseException(INDEX_NOT_FOUND, index);
            } else {
                throw new AtlasBaseException(String.format("Error in executing elastic query: %s", EntityUtils.toString(entity)), rex);
            }
        }

        return EntityUtils.toString(response.getEntity());
    }

    private String performDirectCountQuery(String query) throws AtlasBaseException, IOException {
        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        String endPoint = index + "/_count";

        Request request = new Request("GET", endPoint);
        request.setEntity(entity);

        Response response;
        try {
            response = getESClient().performRequest(request);
        } catch (ResponseException rex) {
            if (rex.getResponse().getStatusLine().getStatusCode() == 404) {
                LOG.warn(String.format("ES index with name %s not found", index));
                throw new AtlasBaseException(INDEX_NOT_FOUND, index);
            } else {
                throw new AtlasBaseException(String.format("Error in executing elastic count query: %s", EntityUtils.toString(entity)), rex);
            }
        }

        return EntityUtils.toString(response.getEntity());
    }

    private String performDirectUpdateByQuery(String query) throws AtlasBaseException, IOException {
        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        String endPoint;

        endPoint = index + "/_update_by_query";

        Request request = new Request("POST", endPoint);
        request.setEntity(entity);

        Response response;
        try {
            response = lowLevelRestClient.performRequest(request);
        } catch (ResponseException rex) {
            if (rex.getResponse().getStatusLine().getStatusCode() == 404) {
                LOG.warn(String.format("ES index with name %s not found", index));
                throw new AtlasBaseException(INDEX_NOT_FOUND, index);
            } else {
                throw new AtlasBaseException(String.format("Error in executing elastic query: %s", EntityUtils.toString(entity)), rex);
            }
        }

        return EntityUtils.toString(response.getEntity());
    }

    private DirectIndexQueryResult getResultFromResponse(String responseString, boolean async) throws IOException {
        Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(responseString, Map.class);
        return getResultFromResponse(responseMap.get("response"));
    }

    private DirectIndexQueryResult getResultFromResponse(Map<String, LinkedHashMap> responseMap) throws IOException {
        DirectIndexQueryResult result = new DirectIndexQueryResult();
        Map<String, LinkedHashMap> hits_0 = AtlasType.fromJson(AtlasType.toJson(responseMap.get("hits")), Map.class);
        if (hits_0 == null) {
            return result;
        }
        LinkedHashMap approximateCount = hits_0.get("total");
        if (approximateCount != null) {
            this.vertexTotals = (Integer) approximateCount.get("value");
        }

        List<LinkedHashMap> hits_1 = AtlasType.fromJson(AtlasType.toJson(hits_0.get("hits")), List.class);

        Stream<Result<AtlasJanusVertex, AtlasJanusEdge>> resultStream = hits_1.stream().map(ResultImplDirect::new);
        result.setIterator(resultStream.iterator());

        Map<String, Object> aggregationsMap = (Map<String, Object>) responseMap.get("aggregations");

        if (MapUtils.isNotEmpty(aggregationsMap)) {
            result.setAggregationMap(aggregationsMap);
        }

        return result;

    }


    private DirectIndexQueryResult getResultFromResponse(String responseString) throws IOException {

        Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(responseString, Map.class);

        return getResultFromResponse(responseMap);
    }



    @Override
    public DirectIndexQueryResult<AtlasJanusVertex, AtlasJanusEdge> vertices(SearchParams searchParams) throws AtlasBaseException {
        return runQueryWithLowLevelClient(searchParams);
    }

    @Override
    public Map<String, Object> directIndexQuery(String query) throws AtlasBaseException {
        return runQueryWithLowLevelClient(query);
    }

    @Override
    public Map<String, Object> directEsIndexQuery(String query) throws AtlasBaseException {
        return runRawQueryWithLowLevelClient(query);
    }

    @Override
    public Long countIndexQuery(String query) throws AtlasBaseException {
        return runCountWithLowLevelClient(query);
    }

    public Map<String, LinkedHashMap> directUpdateByQuery(String query) throws AtlasBaseException {
        return runUpdateByQueryWithLowLevelClient(query);
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices() {
        SearchRequest searchRequest = getSearchRequest(index, sourceBuilder);
        return runQuery(searchRequest);
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices(int offset, int limit, String sortBy, Order sortOrder) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Result<AtlasJanusVertex, AtlasJanusEdge>> vertices(int offset, int limit) {
        sourceBuilder.from(offset);
        sourceBuilder.size(limit);
        SearchRequest searchRequest = getSearchRequest(index, sourceBuilder);
        return runQuery(searchRequest);
    }

    @Override
    public Long vertexTotals() {
        if (searchResponse != null) {
            return searchResponse.getHits().getTotalHits().value;
        }
        return vertexTotals;
    }

    public final class ResultImpl implements AtlasIndexQuery.Result<AtlasJanusVertex, AtlasJanusEdge> {
        private SearchHit hit;

        public ResultImpl(SearchHit hit) {
            this.hit = hit;
        }

        @Override
        public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getVertex() {
            String vertexId = getVertexId();
            return graph.getVertex(String.valueOf(vertexId));
        }

        @Override
        public String getVertexId() {
            String docId = String.valueOf(hit.getId());
            return docId.substring(1);
        }

        @Override
        public double getScore() {
            return hit.getScore();
        }

        @Override
        public Set<String> getCollapseKeys() {
            return null;
        }

        @Override
        public DirectIndexQueryResult<AtlasJanusVertex, AtlasJanusEdge> getCollapseVertices(String key) {
            return null;
        }

        @Override
        public Map<String, List<String>> getHighLights() {
            return new HashMap<>();
        }

        @Override
        public ArrayList<Object> getSort() {
            return new ArrayList<>();
        }
    }


    public final class ResultImplDirect implements AtlasIndexQuery.Result<AtlasJanusVertex, AtlasJanusEdge> {
        private LinkedHashMap<String, Object> hit;
        Map<String, LinkedHashMap> innerHitsMap;

        public ResultImplDirect(LinkedHashMap<String, Object> hit) {
            this.hit = hit;
            if (hit.get("inner_hits") != null) {
                innerHitsMap = AtlasType.fromJson(AtlasType.toJson(hit.get("inner_hits")), Map.class);
            }
        }

        @Override
        public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getVertex() {
            if (LEAN_GRAPH_ENABLED) {
                String vertexId = getVertexId();
                return graph.getVertex(vertexId);
            } else {
                long vertexId = LongEncoding.decode(String.valueOf(hit.get("_id")));
                return graph.getVertex(String.valueOf(vertexId));
            }
        }

        @Override
        public Set<String> getCollapseKeys() {
            Set<String> collapseKeys = new HashSet<>();
            if (innerHitsMap != null) {
                collapseKeys = innerHitsMap.keySet();
            }
            return collapseKeys;
        }

        @Override
        public DirectIndexQueryResult getCollapseVertices(String key) {
            DirectIndexQueryResult result = new DirectIndexQueryResult();
            if (innerHitsMap != null) {
                Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(AtlasType.toJson(innerHitsMap.get(key)), Map.class);
                Map<String, LinkedHashMap> hits_0 = AtlasType.fromJson(AtlasType.toJson(responseMap.get("hits")), Map.class);

                Integer approximateCount = (Integer) hits_0.get("total").get("value");
                result.setApproximateCount(approximateCount);

                List<LinkedHashMap> hits_1 = AtlasType.fromJson(AtlasType.toJson(hits_0.get("hits")), List.class);
                Stream<Result<AtlasJanusVertex, AtlasJanusEdge>> resultStream = hits_1.stream().map(ResultImplDirect::new);
                result.setIterator(resultStream.iterator());

                return result;
            }
            return null;
        }

        @Override
        public String getVertexId() {
            String docId = String.valueOf(hit.get("_id"));
            if (docId.startsWith(ESConnector.JG_ES_DOC_ID_PREFIX) ) {
                // Only checking startsWith "S" is enough as LongEncoding.BASE_SYMBOLS does not have S at all
                // We can safely assume that no migrated DOC id will contain "S" char

                // Discard prefix "S" from doc id
                return docId.substring(1);
            } else {
                return String.valueOf(LongEncoding.decode(docId));
            }
        }

        @Override
        public double getScore() {
            Object _score = hit.get("_score");
            if (_score == null){
                return -1;
            }
            return Double.parseDouble(String.valueOf(hit.get("_score")));
        }

        @Override
        public Map<String, List<String>> getHighLights() {
            Object highlight = this.hit.get("highlight");
            if(Objects.nonNull(highlight)) {
                return (Map<String, List<String>>) highlight;
            }
            return new HashMap<>();
        }

        @Override
        public ArrayList<Object> getSort() {
            Object sort = this.hit.get("sort");
            if (Objects.nonNull(sort) && sort instanceof List) {
                return (ArrayList<Object>) sort;
            }
            return new ArrayList<>();
        }
    }

    public class AsyncQueryResult {
        private boolean isRunning;
        private String id;
        private String fullResponse;
        private boolean timedOut;

        private boolean success;
        // Constructor for a running process
        public AsyncQueryResult(String id) {
            this.isRunning = true;
            this.id = id;
            this.fullResponse = null;
        }

        // Constructor for a completed process
        public AsyncQueryResult(String fullResponse, boolean isRunning, boolean timedOut) {
            this.isRunning = isRunning;
            this.id = null;
            this.fullResponse = fullResponse;
            this.timedOut = timedOut;
        }

        public void setRunning(boolean running) {
            this.isRunning = running;
        }

        // Getters
        public boolean isRunning() {
            return isRunning;
        }

        public void setTimedOut(boolean timedOut) {
            this.timedOut = timedOut;
        }

        // Getters
        public boolean isTimedOut() {
            return timedOut;
        }

        void setId(String id) {
            this.id = id;
        }
        public String getId() {
            return id;
        }

        public String getFullResponse() {
            return fullResponse;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public boolean isSuccess() {
            return success;
        }
    }

}