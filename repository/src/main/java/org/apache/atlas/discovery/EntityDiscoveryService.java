/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.discovery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.*;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.authorize.AtlasSearchResultScrubRequest;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.*;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.searchlog.SearchLogSearchParams;
import org.apache.atlas.model.searchlog.SearchLogSearchResult;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.query.executors.DSLQueryExecutor;
import org.apache.atlas.query.executors.ScriptEngineBasedExecutor;
import org.apache.atlas.query.executors.TraversalBasedExecutor;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.VertexEdgePropertiesCache;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery.Result;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.userprofile.UserProfileService;
import org.apache.atlas.repository.util.AccessControlUtils;
import org.apache.atlas.searchlog.ESSearchLogger;
import org.apache.atlas.stats.StatsClient;
import org.apache.atlas.type.*;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.SearchTracker;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery.CLIENT_ORIGIN_PLAYBOOK;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery.CLIENT_ORIGIN_PRODUCT;

@Component
public class EntityDiscoveryService implements AtlasDiscoveryService {
    private static final Logger LOG = LoggerFactory.getLogger(EntityDiscoveryService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("discovery.EntityDiscoveryService");
    private static final String DEFAULT_SORT_ATTRIBUTE_NAME = "name";

    private final AtlasGraph                      graph;
    private final AtlasGremlinQueryProvider       gremlinQueryProvider;
    private final AtlasTypeRegistry               typeRegistry;
    private final GraphBackedSearchIndexer        indexer;
    private final SearchTracker                   searchTracker;
    private final int                             maxResultSetSize;
    private final int                             maxTypesLengthInIdxQuery;
    private final int                             maxTagsLengthInIdxQuery;
    private final String                          indexSearchPrefix;
    private final UserProfileService              userProfileService;
    private final SuggestionsProvider             suggestionsProvider;
    private final DSLQueryExecutor                dslQueryExecutor;
    private final StatsClient                     statsClient;
    private final ElasticsearchDslOptimizer dslOptimizer;

    private EntityGraphRetriever            entityRetriever;

    @Inject
    public EntityDiscoveryService(AtlasTypeRegistry typeRegistry,
                                  AtlasGraph graph,
                                  GraphBackedSearchIndexer indexer,
                                  SearchTracker searchTracker,
                                  UserProfileService userProfileService,
                                  StatsClient statsClient,
                                  EntityGraphRetriever entityRetriever) throws AtlasException {
        this(typeRegistry, graph, indexer, searchTracker, userProfileService, statsClient);
        this.entityRetriever          = entityRetriever;
    }

    public EntityDiscoveryService(AtlasTypeRegistry typeRegistry,
                           AtlasGraph graph,
                           GraphBackedSearchIndexer indexer,
                           SearchTracker searchTracker,
                           UserProfileService userProfileService,
                           StatsClient statsClient) throws AtlasException {
        this.graph                    = graph;
        this.indexer                  = indexer;
        this.searchTracker            = searchTracker;
        this.gremlinQueryProvider     = AtlasGremlinQueryProvider.INSTANCE;
        this.typeRegistry             = typeRegistry;
        this.maxResultSetSize         = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_MAX_RESULT_SET_SIZE, 150);
        this.maxTypesLengthInIdxQuery = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_TYPES_MAX_QUERY_STR_LENGTH, 512);
        this.maxTagsLengthInIdxQuery  = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_TAGS_MAX_QUERY_STR_LENGTH, 512);
        this.indexSearchPrefix        = AtlasGraphUtilsV2.getIndexSearchPrefix();
        this.userProfileService       = userProfileService;
        this.suggestionsProvider      = new SuggestionsProviderImpl(graph, typeRegistry);
        this.statsClient              = statsClient;
        this.dslQueryExecutor         = AtlasConfiguration.DSL_EXECUTOR_TRAVERSAL.getBoolean()
                                            ? new TraversalBasedExecutor(typeRegistry, graph, entityRetriever)
                                            : new ScriptEngineBasedExecutor(typeRegistry, graph, entityRetriever);
        this.dslOptimizer             = ElasticsearchDslOptimizer.getInstance();
    }

    /**
     * Package-private constructor for testing purposes only.
     * Skips initialization of dependencies that require application configuration.
     * 
     * WARNING: This constructor should ONLY be used in unit tests where the method
     * under test doesn't access the uninitialized fields.
     */
    EntityDiscoveryService(boolean forTesting) {
        // Minimal initialization for testing
        this.graph                    = null;
        this.indexer                  = null;
        this.searchTracker            = null;
        this.gremlinQueryProvider     = null;  // Skip AtlasGremlinQueryProvider.INSTANCE initialization
        this.typeRegistry             = null;
        this.maxResultSetSize         = 150;
        this.maxTypesLengthInIdxQuery = 512;
        this.maxTagsLengthInIdxQuery  = 512;
        this.indexSearchPrefix        = null;
        this.userProfileService       = null;
        this.suggestionsProvider      = null;
        this.statsClient              = null;
        this.dslQueryExecutor         = null;
        this.dslOptimizer             = ElasticsearchDslOptimizer.getInstance();  // Safe to initialize
        this.entityRetriever          = null;
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchWithParameters(SearchParameters searchParameters) throws AtlasBaseException {
        return searchWithSearchContext(new SearchContext(searchParameters, typeRegistry, graph, indexer.getVertexIndexKeys(), statsClient));
    }

    private AtlasSearchResult searchWithSearchContext(SearchContext searchContext) throws AtlasBaseException {
        SearchParameters  searchParameters = searchContext.getSearchParameters();
        AtlasSearchResult ret              = new AtlasSearchResult(searchParameters);
        final QueryParams params           = QueryParams.getNormalizedParams(searchParameters.getLimit(),searchParameters.getOffset());
        String            searchID         = searchTracker.add(searchContext); // For future cancellations

        searchParameters.setLimit(params.limit());
        searchParameters.setOffset(params.offset());

        try {
            List<AtlasVertex> resultList = searchContext.getSearchProcessor().execute();

            ret.setApproximateCount(searchContext.getSearchProcessor().getResultCount());

            String nextMarker = searchContext.getSearchProcessor().getNextMarker();
            if (StringUtils.isNotEmpty(nextMarker)) {
                ret.setNextMarker(nextMarker);
            }

            // By default any attribute that shows up in the search parameter should be sent back in the response
            // If additional values are requested then the entityAttributes will be a superset of the all search attributes
            // and the explicitly requested attribute(s)
            Set<String> resultAttributes = new HashSet<>();
            Set<String> entityAttributes = new HashSet<>();

            if (CollectionUtils.isNotEmpty(searchParameters.getAttributes())) {
                resultAttributes.addAll(searchParameters.getAttributes());
            }

            if (CollectionUtils.isNotEmpty(searchContext.getEntityAttributes())) {
                resultAttributes.addAll(searchContext.getEntityAttributes());
            }

            if (CollectionUtils.isNotEmpty(searchContext.getEntityTypes())) {

                AtlasEntityType entityType = searchContext.getEntityTypes().iterator().next();

               for (String resultAttribute : resultAttributes) {
                    AtlasStructType.AtlasAttribute attribute  = entityType.getAttribute(resultAttribute);

                    if (attribute == null) {
                        attribute = entityType.getRelationshipAttribute(resultAttribute, null);
                    }

                    if (attribute != null) {
                        AtlasType attributeType = attribute.getAttributeType();

                        if (attributeType instanceof AtlasArrayType) {
                            attributeType = ((AtlasArrayType) attributeType).getElementType();
                        }

                        if (attributeType instanceof AtlasEntityType || attributeType instanceof AtlasBuiltInTypes.AtlasObjectIdType) {
                            entityAttributes.add(resultAttribute);
                        }
                    }
                }
            }

            for (AtlasVertex atlasVertex : resultList) {
                AtlasEntityHeader entity = entityRetriever.toAtlasEntityHeader(atlasVertex, resultAttributes);

                if(searchParameters.getIncludeClassificationAttributes()) {
                    entity.setClassifications(entityRetriever.handleGetAllClassifications(atlasVertex));
                }

                ret.addEntity(entity);

                // populate ret.referredEntities
                for (String entityAttribute : entityAttributes) {
                    Object attrValue = entity.getAttribute(entityAttribute);

                    if (attrValue instanceof AtlasObjectId) {
                        AtlasObjectId objId = (AtlasObjectId) attrValue;

                        if (ret.getReferredEntities() == null) {
                            ret.setReferredEntities(new HashMap<>());
                        }

                        if (!ret.getReferredEntities().containsKey(objId.getGuid())) {
                            ret.getReferredEntities().put(objId.getGuid(), entityRetriever.toAtlasEntityHeader(objId.getGuid()));
                        }
                    } else if (attrValue instanceof Collection) {
                        Collection objIds = (Collection) attrValue;

                        for (Object obj : objIds) {
                            if (obj instanceof AtlasObjectId) {
                                AtlasObjectId objId = (AtlasObjectId) obj;

                                if (ret.getReferredEntities() == null) {
                                    ret.setReferredEntities(new HashMap<>());
                                }

                                if (!ret.getReferredEntities().containsKey(objId.getGuid())) {
                                    ret.getReferredEntities().put(objId.getGuid(), entityRetriever.toAtlasEntityHeader(objId.getGuid()));
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            searchTracker.remove(searchID);
        }

        scrubSearchResults(ret);

        return ret;
    }

    private void scrubSearchResults(AtlasSearchResult result) throws AtlasBaseException {
        scrubSearchResults(result, false);
    }

    private void scrubSearchResults(AtlasSearchResult result, boolean suppressLogs) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder scrubSearchResultsMetrics = RequestContext.get().startMetricRecord("scrubSearchResults");
        AtlasAuthorizationUtils.scrubSearchResults(new AtlasSearchResultScrubRequest(typeRegistry, result), suppressLogs);
        RequestContext.get().endMetricRecord(scrubSearchResultsMetrics);
    }

    private Set<String> getAggregationFields() {
        Set<String> ret = new HashSet<>(); // for non-modeled attributes.

        ret.add(Constants.ENTITY_TYPE_PROPERTY_KEY);
        ret.add(Constants.STATE_PROPERTY_KEY);

        return ret;
    }

    private Set<AtlasStructType.AtlasAttribute> getAggregationAtlasAttributes() {
        Set<AtlasStructType.AtlasAttribute> ret = new HashSet<>(); // for modeled attributes, like Asset.owner

        ret.add(getAtlasAttributeForAssetOwner());

        return ret;
    }

    private AtlasStructType.AtlasAttribute getAtlasAttributeForAssetOwner() {
        AtlasEntityType typeAsset = typeRegistry.getEntityTypeByName(ASSET_ENTITY_TYPE);
        AtlasStructType.AtlasAttribute atttOwner = typeAsset != null ? typeAsset.getAttribute(OWNER_ATTRIBUTE) : null;

        if(atttOwner == null) {
            String msg = String.format("Unable to resolve the attribute %s.%s", ASSET_ENTITY_TYPE, OWNER_ATTRIBUTE);

            LOG.error(msg);

            throw new RuntimeException(msg);
        }

        return atttOwner;
    }

    @Override
    public AtlasSearchResult directIndexSearch(SearchParams searchParams) throws AtlasBaseException {
        return directIndexSearch(searchParams, false);
    }

    @Override
    public AtlasSearchResult directIndexSearch(SearchParams searchParams, boolean useVertexEdgeBulkFetching) throws AtlasBaseException {
        IndexSearchParams params = (IndexSearchParams) searchParams;
        RequestContext.get().setRelationAttrsForSearch(params.getRelationAttributes());
        RequestContext.get().setAllowDeletedRelationsIndexsearch(params.isAllowDeletedRelations());
        RequestContext.get().setIncludeRelationshipAttributes(params.isIncludeRelationshipAttributes());
        String clientOrigin = RequestContext.get().getClientOrigin();

        RequestContext.get().setIncludeMeanings(!searchParams.isExcludeMeanings());
        RequestContext.get().setIncludeClassifications(!searchParams.isExcludeClassifications());
        RequestContext.get().setIncludeClassificationNames(searchParams.isIncludeClassificationNames());

        AtlasSearchResult ret = new AtlasSearchResult();
        AtlasIndexQuery indexQuery;

        ret.setSearchParameters(searchParams);
        ret.setQueryType(AtlasQueryType.INDEX);

        Set<String> resultAttributes = new HashSet<>();
        if (CollectionUtils.isNotEmpty(searchParams.getAttributes())) {
            resultAttributes.addAll(searchParams.getAttributes());
        }
        AtlasPerfTracer perf = null;
        try {
            if(LOG.isDebugEnabled()){
                LOG.debug("Performing ES search for the params ({})", searchParams);
            }

            String indexName = getIndexName(params);

            indexQuery = graph.elasticsearchQuery(indexName);

            if (searchParams.getEnableFullRestriction()) {
                addPreFiltersToSearchQuery(searchParams);
            }

            AtlasPerfMetrics.MetricRecorder elasticSearchQueryMetric = RequestContext.get().startMetricRecord("elasticSearchQuery");
            optimizeQueryIfApplicable(searchParams, clientOrigin);
            if (CLIENT_ORIGIN_PRODUCT.equals(clientOrigin) || CLIENT_ORIGIN_PLAYBOOK.equals(clientOrigin)) {
                if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                    perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityDiscoveryService.directIndexSearch(" + searchParams.getQuery() + ")");
                }
            }

            DirectIndexQueryResult indexQueryResult = indexQuery.vertices(searchParams);
            if (indexQueryResult == null) {
                return null;
            }
            RequestContext.get().endMetricRecord(elasticSearchQueryMetric);
            prepareSearchResult(ret, indexQueryResult, resultAttributes, true, useVertexEdgeBulkFetching);

            ret.setAggregations(indexQueryResult.getAggregationMap());
            ret.setApproximateCount(indexQuery.vertexTotals());
        } catch (Exception e) {
            LOG.error("Error while performing direct search for the params ({}), {}", searchParams, e.getMessage());
            throw e;
        } finally {
            if (perf != null) {
                AtlasPerfTracer.log(perf);
            }
        }
        return ret;
    }

    @Override
    public Map<String, Object> directEsIndexSearch(SearchParams searchParams) throws AtlasBaseException {
        IndexSearchParams params = (IndexSearchParams) searchParams;
        String clientOrigin = RequestContext.get().getClientOrigin();

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Performing raw ES search for the params ({})", searchParams);
            }

            String indexName = getIndexName(params);
            AtlasIndexQuery indexQuery = graph.elasticsearchQuery(indexName);

            if (searchParams.getEnableFullRestriction()) {
                addPreFiltersToSearchQuery(searchParams);
            }

            optimizeQueryIfApplicable(searchParams, clientOrigin);

            return indexQuery.directEsIndexQuery(searchParams.getQuery());
        } catch (Exception e) {
            LOG.error("Error while performing raw index search for the params ({}), {}", searchParams, e.getMessage());
            throw e;
        }
    }

    @Override
    public Long directCountIndexSearch(SearchParams searchParams) throws AtlasBaseException {
        IndexSearchParams params = (IndexSearchParams) searchParams;
        String clientOrigin = RequestContext.get().getClientOrigin();

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Performing ES count for the params ({})", searchParams);
            }

            String indexName = getIndexName(params);
            AtlasIndexQuery indexQuery = graph.elasticsearchQuery(indexName);

            if (searchParams.getEnableFullRestriction()) {
                addPreFiltersToSearchQuery(searchParams);
            }

            optimizeQueryIfApplicable(searchParams, clientOrigin);

            return indexQuery.countIndexQuery(searchParams.getQuery());
        } catch (Exception e) {
            LOG.error("Error while performing count index search for the params ({}), {}", searchParams, e.getMessage());
            throw e;
        }
    }

    @VisibleForTesting
    protected void optimizeQueryIfApplicable(SearchParams searchParams, String clientOrigin) {
        try {
            // no optimisation when ABAC full restriction is ON. The filters are encoded in the query by addPreFiltersToSearchQuery method
            if (searchParams.getEnableFullRestriction()) {
                return;
            }
            if (CLIENT_ORIGIN_PRODUCT.equals(clientOrigin) || CLIENT_ORIGIN_PLAYBOOK.equals(clientOrigin)) {
                ElasticsearchDslOptimizer.OptimizationResult result = dslOptimizer.optimizeQueryWithValidation(searchParams.getQuery());
                String dslOptimised = result.getOptimizedQuery();
                searchParams.setQuery(dslOptimised);

                if (!result.isValidationPassed()) {
                    LOG.warn("DSL optimization validation failed: {} - falling back to original query",
                            result.getValidationFailureReason());
                }
            }
        } catch (Exception ex) {
            // Do not fail the request on optimization errors; log and proceed with original query
            LOG.error("DSL optimization errored; proceeding with original query: {}", ex.getMessage());
        }
    }

    public List<AtlasVertex> directVerticesIndexSearch(SearchParams searchParams) throws AtlasBaseException {
        IndexSearchParams params = (IndexSearchParams) searchParams;
        RequestContext.get().setRelationAttrsForSearch(params.getRelationAttributes());
        RequestContext.get().setAllowDeletedRelationsIndexsearch(params.isAllowDeletedRelations());
        RequestContext.get().setIncludeRelationshipAttributes(params.isIncludeRelationshipAttributes());

        List<AtlasVertex> ret = new ArrayList<>();
        AtlasIndexQuery indexQuery;

        try {
            if(LOG.isDebugEnabled()){
                LOG.debug("Performing ES search for the params ({})", searchParams);
            }

            String indexName = getIndexName(params);

            indexQuery = graph.elasticsearchQuery(indexName);

            if (searchParams.getEnableFullRestriction()) {
                addPreFiltersToSearchQuery(searchParams);
            }

            AtlasPerfMetrics.MetricRecorder elasticSearchQueryMetric = RequestContext.get().startMetricRecord("elasticSearchQuery");
            DirectIndexQueryResult indexQueryResult = indexQuery.vertices(searchParams);
            if (indexQueryResult == null) {
                return null;
            }
            RequestContext.get().endMetricRecord(elasticSearchQueryMetric);

            Iterator<Result> iterator = indexQueryResult.getIterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                AtlasVertex vertex = result.getVertex();
                ret.add(vertex);
            }
        } catch (Exception e) {
            LOG.error("Error while performing direct search for the params ({}), {}", searchParams, e.getMessage());
            throw e;
        }
        return ret;
    }

    @Override
    public AtlasSearchResult directRelationshipIndexSearch(SearchParams searchParams) throws AtlasBaseException {
        AtlasSearchResult ret = new AtlasSearchResult();
        AtlasIndexQuery indexQuery;

        ret.setSearchParameters(searchParams);
        ret.setQueryType(AtlasQueryType.INDEX);

        try {
            if(LOG.isDebugEnabled()){
                LOG.debug("Performing ES relationship search for the params ({})", searchParams);
            }

            indexQuery = graph.elasticsearchQuery(EDGE_INDEX_NAME);
            AtlasPerfMetrics.MetricRecorder elasticSearchQueryMetric = RequestContext.get().startMetricRecord("elasticSearchQueryEdge");
            DirectIndexQueryResult indexQueryResult = indexQuery.vertices(searchParams);
            if (indexQueryResult == null) {
                return null;
            }
            RequestContext.get().endMetricRecord(elasticSearchQueryMetric);

            //Note: AtlasSearchResult.entities are not supported yet

            ret.setAggregations(indexQueryResult.getAggregationMap());
            ret.setApproximateCount(indexQuery.vertexTotals());
        } catch (Exception e) {
            LOG.error("Error while performing direct relationship search for the params ({}), {}", searchParams, e.getMessage());
            throw e;
        }
        return ret;
    }

    @Override
    public SearchLogSearchResult searchLogs(SearchLogSearchParams searchParams) throws AtlasBaseException {
        SearchLogSearchResult ret = new SearchLogSearchResult();
        ret.setSearchParameters(searchParams);
        AtlasIndexQuery indexQuery = null;

        try {
            indexQuery = graph.elasticsearchQuery(ESSearchLogger.INDEX_NAME);
            Map<String, Object> result = indexQuery.directIndexQuery(searchParams.getQueryString());

            if (result.get("total") != null)
                ret.setApproximateCount( ((Integer) result.get("total")).longValue());

            List<LinkedHashMap> hits = (List<LinkedHashMap>) result.get("data");

            List<Map<String, Object>> logs = hits.stream().map(x -> (HashMap<String, Object>) x.get("_source")).collect(Collectors.toList());

            ret.setLogs(logs);
            ret.setAggregations((Map<String, Object>) result.get("aggregations"));

            return ret;
        } catch (AtlasBaseException be) {
            throw be;
        }
    }

    private void prepareSearchResult(AtlasSearchResult ret, DirectIndexQueryResult indexQueryResult, Set<String> resultAttributes, boolean fetchCollapsedResults,
                                     boolean useVertexEdgeBulkFetching) throws AtlasBaseException {
        SearchParams searchParams = ret.getSearchParameters();
        AtlasPerfMetrics.MetricRecorder prepareSearchResultMetrics = RequestContext.get().startMetricRecord("prepareSearchResult");
        try {
            if(LOG.isDebugEnabled()){
                LOG.debug("Preparing search results for ({})", ret.getSearchParameters());
            }
            Iterator<Result> iterator = indexQueryResult.getIterator();
            List<Result> results = IteratorUtils.toList(iterator);
            boolean showSearchScore = searchParams.getShowSearchScore();
            if (iterator == null) {
                return;
            }
            Set<String> vertexIds = results.stream().map(result -> {
                AtlasVertex vertex = result.getVertex();
                if (vertex == null) {
                    LOG.warn("vertex in null");
                    return null;
                }
                return vertex.getId().toString();
            }).filter(Objects::nonNull).collect(Collectors.toSet());
            VertexEdgePropertiesCache vertexEdgePropertiesCache;
            if (useVertexEdgeBulkFetching) {
                vertexEdgePropertiesCache = entityRetriever.enrichVertexPropertiesByVertexIds(vertexIds, resultAttributes);
            } else {
                vertexEdgePropertiesCache = null;
            }

            // If valueMap of certain vertex is empty or null then remove that from processing results


            for(Result result : results) {
                AtlasVertex vertex = result.getVertex();

                if (vertex == null) {
                    LOG.warn("vertex in null");
                    continue;
                }
                vertexIds.add(vertex.getId().toString());
                AtlasEntityHeader header;

                if(useVertexEdgeBulkFetching) {
                  header = entityRetriever.toAtlasEntityHeader(vertex, resultAttributes, vertexEdgePropertiesCache);
                } else {
                    header = entityRetriever.toAtlasEntityHeader(vertex, resultAttributes);
                }

                if (showSearchScore) {
                    ret.addEntityScore(header.getGuid(), result.getScore());
                }
                if (fetchCollapsedResults) {
                    Map<String, AtlasSearchResult> collapse = new HashMap<>();

                    Set<String> collapseKeys = result.getCollapseKeys();
                    for (String collapseKey : collapseKeys) {
                        AtlasSearchResult collapseRet = new AtlasSearchResult();
                        collapseRet.setSearchParameters(ret.getSearchParameters());

                        Set<String> collapseResultAttributes = new HashSet<>();
                        if (searchParams.getCollapseAttributes() != null) {
                            collapseResultAttributes.addAll(searchParams.getCollapseAttributes());
                        } else {
                            collapseResultAttributes = resultAttributes;
                        }

                        if (searchParams.getCollapseRelationAttributes() != null) {
                            RequestContext.get().getRelationAttrsForSearch().clear();
                            RequestContext.get().setRelationAttrsForSearch(searchParams.getCollapseRelationAttributes());
                        }

                        DirectIndexQueryResult indexQueryCollapsedResult = result.getCollapseVertices(collapseKey);
                        collapseRet.setApproximateCount(indexQueryCollapsedResult.getApproximateCount());
                        prepareSearchResult(collapseRet, indexQueryCollapsedResult, collapseResultAttributes, false, useVertexEdgeBulkFetching);

                        collapseRet.setSearchParameters(null);
                        collapse.put(collapseKey, collapseRet);
                    }
                    if (!collapse.isEmpty()) {
                        header.setCollapse(collapse);
                    }
                }
                if (searchParams.getShowSearchMetadata()) {
                    ret.addHighlights(header.getGuid(), result.getHighLights());
                    ret.addSort(header.getGuid(), result.getSort());
                } else if (searchParams.getShowHighlights()) {
                    ret.addHighlights(header.getGuid(), result.getHighLights());
                }

                ret.addEntity(header);
            }
        } catch (Exception e) {
                throw e;
        } finally {
            RequestContext.get().endMetricRecord(prepareSearchResultMetrics);
        }

        if (!searchParams.getEnableFullRestriction()) {
            scrubSearchResults(ret, searchParams.getSuppressLogs());
        }
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public List<AtlasEntityHeader> searchUsingTermQualifiedName(int from, int size, String termQName,
                                                        Set<String> attributes, Set<String>relationAttributes) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = getMap("from", from);
        dsl.put("size", size);
        dsl.put("query", getMap("term", getMap("__meanings", getMap("value",termQName))));

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(attributes);
        indexSearchParams.setRelationAttributes(relationAttributes);
        AtlasSearchResult searchResult = null;
        searchResult = directIndexSearch(indexSearchParams);
        List<AtlasEntityHeader> entityHeaders = searchResult.getEntities();
        return  entityHeaders;
    }

    private String getIndexName(IndexSearchParams params) throws AtlasBaseException {
        String vertexIndexName = getESIndex();

        if (StringUtils.isEmpty(params.getPersona()) && StringUtils.isEmpty(params.getPurpose())) {
            return vertexIndexName;
        }

        String qualifiedName = "";
        if (StringUtils.isNotEmpty(params.getPersona())) {
            qualifiedName = params.getPersona();
        } else {
            qualifiedName = params.getPurpose();
        }

        String aliasName = AccessControlUtils.getESAliasName(qualifiedName);

        if (StringUtils.isNotEmpty(aliasName)) {
            if(params.isAccessControlExclusive()) {
                accessControlExclusiveDsl(params, aliasName);
                aliasName = aliasName+","+vertexIndexName;
            }
            return aliasName;
        } else {
            throw new AtlasBaseException("ES alias not found for purpose/persona " + params.getPurpose());
        }
    }

    private void accessControlExclusiveDsl(IndexSearchParams params, String aliasName) {

        List<Map<String, Object>> mustClauses = new ArrayList<>();
        Map<String, Object> clientQuery = (Map<String, Object>) params.getDsl().get("query");

        mustClauses.add(clientQuery);

        List<Map<String, Object>>filterClauses = new ArrayList<>();
        filterClauses.add(getMap("terms", getMap("_index", Collections.singletonList(aliasName))));

        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put("must", mustClauses);
        boolQuery.put("filter",filterClauses);

        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.add(getMap("bool", boolQuery));
        shouldClauses.add(getStaticBoolQuery());

        Map<String, Object> topBoolQuery = getMap("bool", getMap("should", shouldClauses));

        Map copyOfDsl = new HashMap(params.getDsl());
        copyOfDsl.put("query", topBoolQuery);

        params.setDsl(copyOfDsl);
    }

    private Map<String, Object> getStaticBoolQuery() {
        List<Map<String, Object>> mustClauses = new ArrayList<>();
        Map<String, Object> mustClause = getMap("bool", getMap("should", Arrays.asList(
                getMap("term", getMap("daapVisibility", "Public")),
                getMap("term", getMap("daapVisibility", "Protected"))
        )));
        mustClauses.add(mustClause);

        List<Map<String, Object>> filterClauses = new ArrayList<>();
        filterClauses.add(getMap("terms", getMap("_index", Collections.singletonList(VERTEX_INDEX_NAME))));

        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put("must", mustClauses);
        boolQuery.put("filter", filterClauses);

        return getMap("bool", boolQuery);
    }

    private void addPreFiltersToSearchQuery(SearchParams searchParams) {
        try {
            String persona = ((IndexSearchParams) searchParams).getPersona();
            String purpose = ((IndexSearchParams) searchParams).getPurpose();

            AtlasPerfMetrics.MetricRecorder addPreFiltersToSearchQueryMetric = RequestContext.get().startMetricRecord("addPreFiltersToSearchQuery");
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, Object>> mustClauseList = new ArrayList<>();

            List<String> actions = new ArrayList<>();
            actions.add("entity-read");

            Map<String, Object> allPreFiltersBoolClause = AtlasAuthorizationUtils.getPreFilterDsl(persona, purpose, actions);
            mustClauseList.add(allPreFiltersBoolClause);

            String dslString = searchParams.getQuery();
            JsonNode node = mapper.readTree(dslString);
            JsonNode userQueryNode = node.get("query");
            if (userQueryNode != null) {

                String userQueryString = userQueryNode.toString();

                String userQueryBase64 = Base64.getEncoder().encodeToString(userQueryString.getBytes());
                mustClauseList.add(getMap("wrapper", getMap("query", userQueryBase64)));
            }

            JsonNode updateQueryNode = mapper.valueToTree(getMap("bool", getMap("must", mustClauseList)));

            ((ObjectNode) node).set("query", updateQueryNode);
            searchParams.setQuery(node.toString());
            RequestContext.get().endMetricRecord(addPreFiltersToSearchQueryMetric);

        } catch (Exception e) {
            LOG.error("Error -> addPreFiltersToSearchQuery!", e);
        }
    }
}
