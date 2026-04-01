package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.*;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.atlas.repository.graphdb.elasticsearch.SearchContextCache;
import org.apache.atlas.type.AtlasType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
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

/**
 * Cassandra/ES implementation of AtlasIndexQuery.
 * Delegates directly to Elasticsearch for index queries using the same
 * ES REST clients as the JanusGraph version (via AtlasElasticsearchDatabase).
 */
public class CassandraIndexQuery implements AtlasIndexQuery<CassandraVertex, CassandraEdge> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraIndexQuery.class);

    private final CassandraGraph      graph;
    private final String              index;
    private final String              queryString;
    private final int                 offset;
    private final SearchSourceBuilder sourceBuilder;
    private final SearchParams        searchParams;
    private final GraphIndexQueryParameters queryParameters;

    private RestHighLevelClient esClient;
    private RestClient          lowLevelRestClient;
    private RestClient          esUiClusterClient;
    private RestClient          esNonUiClusterClient;
    private SearchResponse      searchResponse;
    private long                vertexTotalsValue = -1;

    public CassandraIndexQuery(CassandraGraph graph, String index, String queryString, int offset) {
        this.graph           = graph;
        this.index           = normalizeIndexName(index);
        this.queryString     = queryString;
        this.offset          = offset;
        this.sourceBuilder   = null;
        this.searchParams    = null;
        this.queryParameters = null;
        initClients();
    }

    public CassandraIndexQuery(CassandraGraph graph, String index, SearchSourceBuilder sourceBuilder) {
        this.graph           = graph;
        this.index           = normalizeIndexName(index);
        this.queryString     = null;
        this.offset          = 0;
        this.sourceBuilder   = sourceBuilder;
        this.searchParams    = null;
        this.queryParameters = null;
        initClients();
    }

    public CassandraIndexQuery(CassandraGraph graph, String index, SearchParams searchParams) {
        this.graph           = graph;
        this.index           = normalizeIndexName(index);
        this.queryString     = null;
        this.offset          = 0;
        this.sourceBuilder   = null;
        this.searchParams    = searchParams;
        this.queryParameters = null;
        initClients();
    }

    public CassandraIndexQuery(CassandraGraph graph, GraphIndexQueryParameters queryParameters) {
        this.graph           = graph;
        this.index           = normalizeIndexName(queryParameters.getIndexName());
        this.queryString     = queryParameters.getGraphQueryString();
        this.offset          = queryParameters.getOffset();
        this.sourceBuilder   = null;
        this.searchParams    = null;
        this.queryParameters = queryParameters;
        initClients();
    }

    /**
     * Normalizes ES index names by prepending Constants.INDEX_PREFIX if not already present.
     * Callers may pass raw names like "vertex_index" (the JanusGraph internal name),
     * but ES has the index as "atlas_graph_vertex_index".
     */
    private static String normalizeIndexName(String indexName) {
        if (indexName == null) {
            return null;
        }
        if (!indexName.startsWith(Constants.INDEX_PREFIX)) {
            String normalized = Constants.INDEX_PREFIX + indexName;
            LOG.debug("Normalized ES index name: '{}' -> '{}'", indexName, normalized);
            return normalized;
        }
        return indexName;
    }

    private void initClients() {
        try {
            this.esClient            = AtlasElasticsearchDatabase.getClient();
            this.lowLevelRestClient  = AtlasElasticsearchDatabase.getLowLevelClient();
            this.esUiClusterClient   = AtlasElasticsearchDatabase.getUiClusterClient();
            this.esNonUiClusterClient = AtlasElasticsearchDatabase.getNonUiClusterClient();
        } catch (Exception e) {
            LOG.warn("Failed to initialize ES clients for CassandraIndexQuery. ES queries will not work.", e);
        }
    }

    private RestClient getESClient() {
        if (!AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_REQUEST_ISOLATION.getBoolean()) {
            return lowLevelRestClient;
        }
        try {
            String clientOrigin = RequestContext.get().getClientOrigin();
            if (clientOrigin == null) {
                return lowLevelRestClient;
            }
            if ("product_webapp".equals(clientOrigin)) {
                return Optional.ofNullable(esUiClusterClient).orElse(lowLevelRestClient);
            } else {
                return Optional.ofNullable(esNonUiClusterClient).orElse(lowLevelRestClient);
            }
        } catch (Exception e) {
            LOG.error("Error determining ES client, falling back to low-level client", e);
            return lowLevelRestClient;
        }
    }

    // ---- AtlasIndexQuery interface methods ----

    @Override
    public DirectIndexQueryResult<CassandraVertex, CassandraEdge> vertices(SearchParams searchParams)
            throws AtlasBaseException {
        if (lowLevelRestClient == null) {
            LOG.warn("ES client not available, returning empty result");
            DirectIndexQueryResult<CassandraVertex, CassandraEdge> result = new DirectIndexQueryResult<>();
            result.setIterator(Collections.emptyIterator());
            return result;
        }
        return runQueryWithLowLevelClient(searchParams);
    }

    @Override
    public Map<String, Object> directIndexQuery(String query) throws AtlasBaseException {
        if (lowLevelRestClient == null) {
            return Collections.emptyMap();
        }
        return runQueryWithLowLevelClient(query);
    }

    @Override
    public Map<String, Object> directEsIndexQuery(String query) throws AtlasBaseException {
        if (lowLevelRestClient == null) {
            return Collections.emptyMap();
        }
        return runRawQueryWithLowLevelClient(query);
    }

    @Override
    public Long countIndexQuery(String query) throws AtlasBaseException {
        if (lowLevelRestClient == null) {
            return 0L;
        }
        return runCountWithLowLevelClient(query);
    }

    @Override
    public Iterator<Result<CassandraVertex, CassandraEdge>> vertices() {
        if (esClient == null || sourceBuilder == null) {
            return Collections.emptyIterator();
        }
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(sourceBuilder);
        return runQuery(searchRequest);
    }

    @Override
    public Iterator<Result<CassandraVertex, CassandraEdge>> vertices(int offset, int limit,
                                                                      String sortBy, Order sortOrder) {
        // Not used in production code (JanusGraph version also throws NotImplementedException)
        LOG.warn("vertices(offset, limit, sortBy, sortOrder) not implemented");
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Result<CassandraVertex, CassandraEdge>> vertices(int offset, int limit) {
        if (sourceBuilder != null && esClient != null) {
            sourceBuilder.from(offset);
            sourceBuilder.size(limit);
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.source(sourceBuilder);
            return runQuery(searchRequest);
        }

        // Fallback: queryString-based search via low-level REST client
        if (queryString != null && !queryString.isEmpty()) {
            return runQueryStringSearch(offset, limit);
        }

        return Collections.emptyIterator();
    }

    /**
     * Executes a Lucene query_string query via the low-level ES REST client.
     * Used by EntitySearchProcessor / ClassificationSearchProcessor which pass
     * a query string like "+__typeName:Table AND +__state:ACTIVE".
     */
    private Iterator<Result<CassandraVertex, CassandraEdge>> runQueryStringSearch(int offset, int limit) {
        try {
            // Strip JanusGraph's "$v$" field-name prefix and surrounding quotes.
            // JanusGraph query strings use $v$"__typeName" format; the Cassandra backend
            // indexes vertex properties without the prefix, so ES fields are e.g.
            // "__typeName" not "$v$__typeName". The regex turns $v$"field" → field.
            String cleanedQueryString = queryString.replaceAll("\\$v\\$\"([^\"]*)\"", "$1");

            String esQuery = String.format(
                "{\"query\":{\"query_string\":{\"query\":\"%s\"}},\"from\":%d,\"size\":%d,\"_source\":false}",
                cleanedQueryString.replace("\\", "\\\\").replace("\"", "\\\""), offset, limit);

            LOG.info("runQueryStringSearch: index='{}', queryString='{}', cleaned='{}', offset={}, limit={}",
                    index, queryString, cleanedQueryString, offset, limit);

            String responseString = performDirectIndexQuery(esQuery, false);
            LOG.info("runQueryStringSearch: response length={}, preview='{}'",
                    responseString != null ? responseString.length() : 0,
                    responseString != null ? responseString.substring(0, Math.min(500, responseString.length())) : "null");

            return parseSearchResponse(responseString);
        } catch (Exception e) {
            LOG.error("Failed to execute queryString search on index {}: {}", index, e.getMessage(), e);
            return Collections.emptyIterator();
        }
    }

    @SuppressWarnings("unchecked")
    private Iterator<Result<CassandraVertex, CassandraEdge>> parseSearchResponse(String responseString) {
        Map<String, Object> responseMap = AtlasType.fromJson(responseString, Map.class);
        if (responseMap == null) {
            return Collections.emptyIterator();
        }

        Map<String, Object> hitsWrapper = (Map<String, Object>) responseMap.get("hits");
        if (hitsWrapper == null) {
            return Collections.emptyIterator();
        }

        // Extract total count
        Object totalObj = hitsWrapper.get("total");
        if (totalObj instanceof Map) {
            Object value = ((Map<String, Object>) totalObj).get("value");
            if (value instanceof Number) {
                vertexTotalsValue = ((Number) value).longValue();
            }
        } else if (totalObj instanceof Number) {
            vertexTotalsValue = ((Number) totalObj).longValue();
        }

        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");
        if (hits == null || hits.isEmpty()) {
            return Collections.emptyIterator();
        }

        List<Result<CassandraVertex, CassandraEdge>> results = new ArrayList<>(hits.size());
        for (Map<String, Object> hit : hits) {
            String vertexId = (String) hit.get("_id");
            if (vertexId != null) {
                double score = hit.get("_score") instanceof Number ? ((Number) hit.get("_score")).doubleValue() : 0.0;
                CassandraVertex vertex = (CassandraVertex) graph.getVertex(vertexId);
                if (vertex != null) {
                    results.add(new ResultImpl(vertex, score));
                }
            }
        }
        return results.iterator();
    }

    @Override
    public Long vertexTotals() {
        if (searchResponse != null) {
            return searchResponse.getHits().getTotalHits().value;
        }
        return vertexTotalsValue;
    }

    // ---- High-level ES client query (for SearchSourceBuilder-based queries) ----

    private Iterator<Result<CassandraVertex, CassandraEdge>> runQuery(SearchRequest searchRequest) {
        try {
            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            int bufferLimit = 2000 * 1024 * 1024;
            builder.setHttpAsyncResponseConsumerFactory(
                    new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(bufferLimit));
            RequestOptions requestOptions = builder.build();
            searchResponse = esClient.search(searchRequest, requestOptions);
            Stream<Result<CassandraVertex, CassandraEdge>> resultStream = Arrays.stream(searchResponse.getHits().getHits())
                    .map(ResultImpl::new);
            return resultStream.iterator();
        } catch (IOException e) {
            LOG.error("Failed to execute ES query", e);
            return Collections.emptyIterator();
        }
    }

    // ---- Low-level REST client queries ----

    private DirectIndexQueryResult<CassandraVertex, CassandraEdge> runQueryWithLowLevelClient(SearchParams searchParams) throws AtlasBaseException {
        try {
            if (searchParams.isCallAsync() || AtlasConfiguration.ENABLE_ASYNC_INDEXSEARCH.getBoolean()) {
                return performAsyncDirectIndexQuery(searchParams);
            } else {
                String responseString = performDirectIndexQuery(searchParams.getQuery(), searchParams.isIncludeSourceInResults());
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
            return AtlasType.fromJson(responseString, Map.class);
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
            if (countObj instanceof Number) {
                return ((Number) countObj).longValue();
            }
            return Long.parseLong(String.valueOf(countObj));
        } catch (IOException e) {
            LOG.error("Failed to execute _count query on ES {}", e.getMessage());
            handleNetworkErrors(e);
            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED, e.getMessage());
        }
    }

    // ---- Direct ES REST calls ----

    private String performDirectIndexQuery(String query, boolean source) throws AtlasBaseException, IOException {
        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        String endPoint = source ? index + "/_search" : index + "/_search?_source=false";

        Request request = new Request("GET", endPoint);
        request.setEntity(entity);

        Response response;
        try {
            response = getESClient().performRequest(request);
        } catch (ResponseException rex) {
            int statusCode = rex.getResponse().getStatusLine().getStatusCode();
            String responseBody = EntityUtils.toString(rex.getResponse().getEntity());
            if (statusCode == 404) {
                LOG.warn("ES index with name {} not found", index);
                throw new AtlasBaseException(INDEX_NOT_FOUND, index);
            }
            // Handle "No mapping found for [field] in order to sort on" — this happens
            // on a fresh Cassandra backend where ES fields are auto-mapped on first
            // document. If no document with the sort field has been indexed yet, ES
            // returns 400. Return an empty result instead of propagating the error.
            if (statusCode == 400 && responseBody != null && responseBody.contains("No mapping found for")) {
                LOG.warn("ES query returned 400 due to unmapped sort field (index={}). " +
                         "Returning empty result. This is expected on a fresh backend. Response: {}",
                         index, responseBody.substring(0, Math.min(500, responseBody.length())));
                return "{\"hits\":{\"total\":{\"value\":0},\"hits\":[]}}";
            }
            LOG.error("ES query failed: status={}, index={}, response={}", statusCode, index, responseBody);
            throw new AtlasBaseException(String.format("Error in executing elastic query: %s", EntityUtils.toString(entity)), rex);
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
                LOG.warn("ES index with name {} not found", index);
                throw new AtlasBaseException(INDEX_NOT_FOUND, index);
            } else {
                throw new AtlasBaseException(String.format("Error in executing elastic count query: %s", EntityUtils.toString(entity)), rex);
            }
        }
        return EntityUtils.toString(response.getEntity());
    }

    // ---- Async search support ----

    private DirectIndexQueryResult<CassandraVertex, CassandraEdge> performAsyncDirectIndexQuery(SearchParams searchParams) throws AtlasBaseException, IOException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("performAsyncDirectIndexQuery");
        DirectIndexQueryResult<CassandraVertex, CassandraEdge> result = null;
        boolean contextIdExists = StringUtils.isNotEmpty(searchParams.getSearchContextId())
                && searchParams.getSearchContextSequenceNo() != null;
        try {
            if (contextIdExists) {
                processRequestWithSameSearchContextId(searchParams);
            }

            String keepAliveTime = AtlasConfiguration.INDEXSEARCH_ASYNC_SEARCH_KEEP_ALIVE_TIME_IN_SECONDS.getLong() + "s";
            if (searchParams.getRequestTimeoutInSecs() != null) {
                keepAliveTime = searchParams.getRequestTimeoutInSecs() + "s";
            }

            AsyncQueryResult response = submitAsyncSearch(searchParams, keepAliveTime).get();
            if (response.isRunning()) {
                String esSearchId = response.getId();
                if (contextIdExists) {
                    String searchContextId = searchParams.getSearchContextId();
                    Integer searchContextSequenceNo = searchParams.getSearchContextSequenceNo();
                    CompletableFuture.runAsync(() -> SearchContextCache.put(searchContextId, searchContextSequenceNo, esSearchId));
                }
                response = getAsyncSearchResponse(searchParams, esSearchId).get();
                if (response == null) {
                    return null;
                }
                if (response.isTimedOut()) {
                    LOG.error("timeout exceeded for query {}:", searchParams.getQuery());
                    throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED_DUE_TO_TIMEOUT, keepAliveTime);
                }
                result = getResultFromResponse(response.getFullResponse(), true);
            } else {
                result = getResultFromResponse(response.getFullResponse(), true);
            }
        } catch (Exception e) {
            LOG.error("Failed to execute async query on ES {}", e.getMessage());
            if (e instanceof IOException) {
                handleNetworkErrors((IOException) e);
            }
            if (e instanceof ResponseException) {
                int statusCode = ((ResponseException) e).getResponse().getStatusLine().getStatusCode();
                throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED_WITH_RESPONSE_CODE,
                        "Elasticsearch returned status code: " + statusCode + ", message: " + e.getMessage());
            }
            throw new AtlasBaseException(AtlasErrorCode.INDEX_SEARCH_FAILED, e.getMessage());
        } finally {
            if (contextIdExists) {
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

    private void processRequestWithSameSearchContextId(SearchParams searchParams) {
        try {
            String currentSearchContextId = searchParams.getSearchContextId();
            Integer currentSequenceNumber = searchParams.getSearchContextSequenceNo();
            String previousESSearchId = SearchContextCache.getESAsyncSearchIdFromContextCache(currentSearchContextId, currentSequenceNumber);
            if (StringUtils.isNotEmpty(previousESSearchId)) {
                deleteAsyncSearchResponse(previousESSearchId);
            }
        } catch (Exception e) {
            LOG.error("Failed to process the request with the same search context ID {}", e.getMessage());
        }
    }

    private Future<AsyncQueryResult> getAsyncSearchResponse(SearchParams searchParams, String esSearchId) {
        CompletableFuture<AsyncQueryResult> future = new CompletableFuture<>();
        String endPoint = "_async_search/" + esSearchId;
        Request request = new Request("GET", endPoint);
        long waitTime = AtlasConfiguration.INDEXSEARCH_ASYNC_SEARCH_KEEP_ALIVE_TIME_IN_SECONDS.getLong();
        if (searchParams.getRequestTimeoutInSecs() != null) {
            waitTime = searchParams.getRequestTimeoutInSecs();
        }
        request.addParameter("wait_for_completion_timeout", waitTime + "s");

        getESClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    String respString = EntityUtils.toString(response.getEntity());
                    Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(respString, Map.class);
                    Boolean isInComplete = AtlasType.fromJson(AtlasType.toJson(responseMap.get("is_partial")), Boolean.class);
                    String id = AtlasType.fromJson(AtlasType.toJson(responseMap.get("id")), String.class);
                    boolean isTimedOut = AtlasType.fromJson(AtlasType.toJson(responseMap.get("response").get("timed_out")), Boolean.class);
                    if (isInComplete != null && isInComplete) {
                        deleteAsyncSearchResponse(id);
                        future.complete(null);
                    }
                    future.complete(new AsyncQueryResult(respString, false, isTimedOut));
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException) {
                    int statusCode = ((ResponseException) exception).getResponse().getStatusLine().getStatusCode();
                    if (statusCode == 400 || statusCode == 404) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(exception);
                    }
                } else {
                    future.completeExceptionally(exception);
                }
            }
        });
        return future;
    }

    private void deleteAsyncSearchResponse(String searchContextId) {
        try {
            String endPoint = "_async_search/" + searchContextId;
            Request request = new Request("DELETE", endPoint);
            getESClient().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    LOG.debug("Deleted async search response");
                }
                @Override
                public void onFailure(Exception exception) {
                    if (exception instanceof ResponseException
                            && ((ResponseException) exception).getResponse().getStatusLine().getStatusCode() == 404) {
                        LOG.debug("Async search response not found");
                    } else {
                        LOG.error("Failed to delete async search response {}", exception.getMessage());
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("Error deleting async search response", e);
        }
    }

    private Future<AsyncQueryResult> submitAsyncSearch(SearchParams searchParams, String keepAliveTime) {
        CompletableFuture<AsyncQueryResult> future = new CompletableFuture<>();
        HttpEntity entity = new NStringEntity(searchParams.getQuery(), ContentType.APPLICATION_JSON);
        String endPoint = searchParams.isIncludeSourceInResults()
                ? index + "/_async_search"
                : index + "/_async_search?_source=false";

        Request request = new Request("POST", endPoint);
        request.setEntity(entity);
        request.addParameter("wait_for_completion_timeout", "100ms");
        request.addParameter("keep_alive", keepAliveTime);

        getESClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    String respString = EntityUtils.toString(response.getEntity());
                    Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(respString, Map.class);
                    boolean isRunning = AtlasType.fromJson(AtlasType.toJson(responseMap.get("is_running")), Boolean.class);
                    String id = AtlasType.fromJson(AtlasType.toJson(responseMap.get("id")), String.class);
                    boolean isTimedOut = AtlasType.fromJson(AtlasType.toJson(responseMap.get("response").get("timed_out")), Boolean.class);
                    AsyncQueryResult result = new AsyncQueryResult(respString, isRunning, isTimedOut);
                    if (isRunning && StringUtils.isNotEmpty(id)) {
                        result.setId(id);
                    }
                    future.complete(result);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public void onFailure(Exception exception) {
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    // ---- Response parsing ----

    private DirectIndexQueryResult<CassandraVertex, CassandraEdge> getResultFromResponse(String responseString, boolean async) throws IOException {
        Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(responseString, Map.class);
        return getResultFromResponse(responseMap.get("response"));
    }

    private DirectIndexQueryResult<CassandraVertex, CassandraEdge> getResultFromResponse(String responseString) throws IOException {
        Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(responseString, Map.class);
        return getResultFromResponse(responseMap);
    }

    @SuppressWarnings("unchecked")
    private DirectIndexQueryResult<CassandraVertex, CassandraEdge> getResultFromResponse(Map<String, LinkedHashMap> responseMap) throws IOException {
        DirectIndexQueryResult<CassandraVertex, CassandraEdge> result = new DirectIndexQueryResult<>();
        Map<String, LinkedHashMap> hits_0 = AtlasType.fromJson(AtlasType.toJson(responseMap.get("hits")), Map.class);
        if (hits_0 == null) {
            result.setIterator(Collections.emptyIterator());
            return result;
        }
        LinkedHashMap approximateCount = hits_0.get("total");
        if (approximateCount != null) {
            this.vertexTotalsValue = ((Number) approximateCount.get("value")).longValue();
        }
        List<LinkedHashMap> hits_1 = AtlasType.fromJson(AtlasType.toJson(hits_0.get("hits")), List.class);
        Stream<Result<CassandraVertex, CassandraEdge>> resultStream = hits_1.stream().map(ResultImplDirect::new);
        result.setIterator(resultStream.iterator());
        @SuppressWarnings("unchecked")
        Map<String, Aggregation> aggregationsMap = (Map) responseMap.get("aggregations");
        if (MapUtils.isNotEmpty(aggregationsMap)) {
            result.setAggregationMap(aggregationsMap);
        }
        return result;
    }

    // ---- Error handling ----

    private void handleNetworkErrors(Exception e) throws AtlasBaseException {
        if (e instanceof SocketTimeoutException || e instanceof UnknownHostException
                || containsMessage(e, "connection reset by peer", "network error", "connection refused")) {
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

    // ---- ES doc ID decoding ----

    /**
     * JanusGraph's LongEncoding uses base-36: digits 0-9 then lowercase a-z.
     * This matches the actual JanusGraph source (janusgraph-driver LongEncoding.java).
     */
    private static final String BASE_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyz";
    private static final int    BASE         = BASE_SYMBOLS.length(); // 36

    /**
     * Decode an ES document ID to a Cassandra vertex ID.
     *
     * JanusGraph-era ES documents use LongEncoding (base-36) for the _id field.
     * For example, vertex 36 is stored as ES _id "10" (1*36 + 0 = 36).
     * After migration, vertex_id in Cassandra is String.valueOf(longId) = "36".
     *
     * For Cassandra-native vertices using UUID IDs (contain hyphens), no decoding is needed.
     * For IDs containing uppercase letters or special characters, no decoding is attempted
     * since JanusGraph base-36 only uses digits and lowercase letters.
     *
     * @param docId the ES document _id
     * @return the vertex_id to use for Cassandra lookup
     */
    static String decodeDocId(String docId) {
        if (docId == null || docId.isEmpty()) return docId;
        // UUID strings contain hyphens — no decoding needed
        if (docId.contains("-")) return docId;

        // Try base-36 decode (JanusGraph LongEncoding format: 0-9, a-z)
        try {
            long value = 0;
            for (int i = 0; i < docId.length(); i++) {
                int digit = BASE_SYMBOLS.indexOf(docId.charAt(i));
                if (digit < 0) {
                    // Character not in base-36 set (e.g., uppercase letter) — return as-is
                    return docId;
                }
                value = value * BASE + digit;
            }
            return String.valueOf(value);
        } catch (Exception e) {
            return docId;
        }
    }

    // ---- Result wrapper for SearchHit (high-level client) ----

    public final class ResultImpl implements Result<CassandraVertex, CassandraEdge> {
        private final SearchHit hit;
        private final AtlasVertex<CassandraVertex, CassandraEdge> resolvedVertex;
        private final double resolvedScore;

        public ResultImpl(SearchHit hit) {
            this.hit            = hit;
            this.resolvedVertex = null;
            this.resolvedScore  = Double.NaN;
        }

        public ResultImpl(AtlasVertex<CassandraVertex, CassandraEdge> vertex, double score) {
            this.hit            = null;
            this.resolvedVertex = vertex;
            this.resolvedScore  = score;
        }

        @Override
        public AtlasVertex<CassandraVertex, CassandraEdge> getVertex() {
            if (resolvedVertex != null) {
                return resolvedVertex;
            }
            String docId = hit.getId();

            // Try raw doc ID first — works for Cassandra-native ES indices (atlas_graph_*)
            // where _id is the numeric vertex ID directly.
            AtlasVertex<CassandraVertex, CassandraEdge> v = graph.getVertex(docId);

            if (v == null) {
                // Raw doc ID not found; try base-36 decode for JanusGraph-era ES indices
                // (janusgraph_*) where _id is LongEncoding-encoded.
                String decodedId = decodeDocId(docId);
                if (!decodedId.equals(docId)) {
                    v = graph.getVertex(decodedId);
                }
            }

            if (v == null) {
                // Vertex not found in Cassandra — try constructing from ES _source data.
                Map<String, Object> source = hit.getSourceAsMap();
                if (source != null && !source.isEmpty()) {
                    v = new CassandraVertex(docId, new LinkedHashMap<>(source), graph);
                    LOG.info("ResultImpl.getVertex: constructed vertex from ES _source. ES docId='{}'", docId);
                } else {
                    LOG.warn("ResultImpl.getVertex: vertex not found and no _source available. ES docId='{}', index='{}'",
                            docId, index);
                }
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("ResultImpl.getVertex: found vertex. ES docId='{}' → vertexId='{}'", docId, v.getId());
            }
            return v;
        }

        @Override
        public double getScore() {
            if (hit != null) {
                return hit.getScore();
            }
            return resolvedScore;
        }

        @Override
        public Set<String> getCollapseKeys() {
            return null;
        }

        @Override
        public DirectIndexQueryResult<CassandraVertex, CassandraEdge> getCollapseVertices(String key) {
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

    // ---- Result wrapper for direct/low-level REST responses ----

    @SuppressWarnings("unchecked")
    public final class ResultImplDirect implements Result<CassandraVertex, CassandraEdge> {
        private final LinkedHashMap<String, Object> hit;
        private Map<String, LinkedHashMap> innerHitsMap;

        public ResultImplDirect(LinkedHashMap<String, Object> hit) {
            this.hit = hit;
            if (hit.get("inner_hits") != null) {
                innerHitsMap = AtlasType.fromJson(AtlasType.toJson(hit.get("inner_hits")), Map.class);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public AtlasVertex<CassandraVertex, CassandraEdge> getVertex() {
            String docId = String.valueOf(hit.get("_id"));

            // Try raw doc ID first — works for Cassandra-native ES indices (atlas_graph_*)
            // where _id is the numeric vertex ID directly.
            AtlasVertex<CassandraVertex, CassandraEdge> v = graph.getVertex(docId);

            if (v == null) {
                // Raw doc ID not found; try base-36 decode for JanusGraph-era ES indices
                // (janusgraph_*) where _id is LongEncoding-encoded.
                String decodedId = decodeDocId(docId);
                if (!decodedId.equals(docId)) {
                    v = graph.getVertex(decodedId);
                }
            }

            if (v == null) {
                // Vertex not found in Cassandra — try constructing from ES _source data.
                // This handles JanusGraph-era ES documents when sharing ES between backends:
                // their vertex IDs reference JanusGraph storage, not Cassandra.
                Map<String, Object> source = (Map<String, Object>) hit.get("_source");
                if (source != null && !source.isEmpty()) {
                    v = new CassandraVertex(docId, new LinkedHashMap<>(source), graph);
                    LOG.info("ResultImplDirect.getVertex: constructed vertex from ES _source. ES docId='{}'", docId);
                } else {
                    LOG.warn("ResultImplDirect.getVertex: vertex not found and no _source available. ES docId='{}', index='{}'",
                            docId, index);
                }
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("ResultImplDirect.getVertex: found vertex. ES docId='{}' → vertexId='{}'", docId, v.getId());
            }
            return v;
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
        public DirectIndexQueryResult<CassandraVertex, CassandraEdge> getCollapseVertices(String key) {
            DirectIndexQueryResult<CassandraVertex, CassandraEdge> result = new DirectIndexQueryResult<>();
            if (innerHitsMap != null) {
                Map<String, LinkedHashMap> responseMap = AtlasType.fromJson(AtlasType.toJson(innerHitsMap.get(key)), Map.class);
                Map<String, LinkedHashMap> hits_0 = AtlasType.fromJson(AtlasType.toJson(responseMap.get("hits")), Map.class);
                Integer approximateCount = (Integer) hits_0.get("total").get("value");
                result.setApproximateCount(approximateCount);
                List<LinkedHashMap> hits_1 = AtlasType.fromJson(AtlasType.toJson(hits_0.get("hits")), List.class);
                Stream<Result<CassandraVertex, CassandraEdge>> resultStream = hits_1.stream().map(ResultImplDirect::new);
                result.setIterator(resultStream.iterator());
                return result;
            }
            return null;
        }

        @Override
        public double getScore() {
            Object _score = hit.get("_score");
            if (_score == null) {
                return -1;
            }
            return Double.parseDouble(String.valueOf(_score));
        }

        @Override
        public Map<String, List<String>> getHighLights() {
            Object highlight = this.hit.get("highlight");
            if (highlight != null) {
                return (Map<String, List<String>>) highlight;
            }
            return new HashMap<>();
        }

        @Override
        public ArrayList<Object> getSort() {
            Object sort = this.hit.get("sort");
            if (sort instanceof List) {
                return (ArrayList<Object>) sort;
            }
            return new ArrayList<>();
        }
    }

    // ---- Async query result holder ----

    private static class AsyncQueryResult {
        private boolean isRunning;
        private String id;
        private final String fullResponse;
        private final boolean timedOut;

        public AsyncQueryResult(String fullResponse, boolean isRunning, boolean timedOut) {
            this.isRunning = isRunning;
            this.fullResponse = fullResponse;
            this.timedOut = timedOut;
        }

        public boolean isRunning() { return isRunning; }
        public boolean isTimedOut() { return timedOut; }
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getFullResponse() { return fullResponse; }
    }
}
