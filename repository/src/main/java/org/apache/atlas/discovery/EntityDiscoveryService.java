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
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.*;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
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
import org.apache.atlas.repository.graphdb.janus.*;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertex;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
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
import org.apache.atlas.v1.model.instance.Id;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.janusgraph.graphdb.relations.CacheEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getAllTagNames;
import static org.apache.atlas.repository.graph.GraphHelper.parseLabelsString;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery.CLIENT_ORIGIN_PLAYBOOK;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery.CLIENT_ORIGIN_PRODUCT;
import static org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever.DISPLAY_NAME;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute;


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
    private final DynamicVertexService            dynamicVertexService;
    private final StatsClient                     statsClient;

    // Cache for type to edge names mapping to avoid repeated calls across methods
    private final Map<String, Map<String, Set<String>>> typeEdgeNamesCache;

    private final ElasticsearchDslOptimizer dslOptimizer;

    private EntityGraphRetriever            entityRetriever;

    @Inject
    public EntityDiscoveryService(AtlasTypeRegistry typeRegistry,
                                  AtlasGraph graph,
                                  GraphBackedSearchIndexer indexer,
                                  SearchTracker searchTracker,
                                  UserProfileService userProfileService,
                                  DynamicVertexService dynamicVertexService,
                                  StatsClient statsClient,
                                  EntityGraphRetriever entityRetriever) throws AtlasException {
        this(typeRegistry, graph, indexer, searchTracker, userProfileService, dynamicVertexService, statsClient);
        this.entityRetriever          = entityRetriever;
    }

    public EntityDiscoveryService(AtlasTypeRegistry typeRegistry,
                           AtlasGraph graph,
                           GraphBackedSearchIndexer indexer,
                           SearchTracker searchTracker,
                           UserProfileService userProfileService,
                           DynamicVertexService dynamicVertexService,
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

        this.statsClient = statsClient;
        this.dynamicVertexService = dynamicVertexService;
        this.dslQueryExecutor = AtlasConfiguration.DSL_EXECUTOR_TRAVERSAL.getBoolean()
                ? new TraversalBasedExecutor(typeRegistry, graph, entityRetriever)
                : new ScriptEngineBasedExecutor(typeRegistry, graph, entityRetriever);
        this.typeEdgeNamesCache = new HashMap<>();

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
        this.dynamicVertexService     = null;
        this.typeEdgeNamesCache       = null;
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

    private void prepareSearchResultV1(AtlasSearchResult ret, DirectIndexQueryResult indexQueryResult, Set<String> resultAttributes, boolean fetchCollapsedResults,
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

    private void prepareSearchResultV2(AtlasSearchResult ret, DirectIndexQueryResult indexQueryResult, Set<String> resultAttributes, boolean fetchCollapsedResults) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("prepareSearchResultV2");
        SearchParams searchParams = ret.getSearchParameters();

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Preparing search results for ({})", ret.getSearchParameters());
            }

            Iterator<Result> iterator = indexQueryResult.getIterator();
            if (iterator == null) {
                return;
            }

            // Cache frequently accessed settings
            boolean showSearchScore = searchParams.getShowSearchScore();
            boolean showSearchMetadata = searchParams.getShowSearchMetadata();
            boolean showHighlights = searchParams.getShowHighlights();
            RequestContext context = RequestContext.get();
            boolean includeClassifications = context.includeClassifications();
            boolean includeClassificationNames = context.isIncludeClassificationNames();
            boolean includeMeanings = context.includeMeanings();

            final int BATCH_SIZE = AtlasConfiguration.ATLAS_CASSANDRA_BATCH_SIZE.getInt();
            Map<String, Result> batchResults = new LinkedHashMap<>(BATCH_SIZE);
            Map<String, AtlasEntityHeader> vertexIdHeader = new HashMap<>();

            // Process vertices in batches but collect all relation IDs
            while (iterator.hasNext()) {
                // Clear previous batch data
                batchResults.clear();

                // Collect batch of results
                while (iterator.hasNext() && batchResults.size() < BATCH_SIZE) {
                    Result result = iterator.next();
                    String id = result.getVertexId().toString();
                    batchResults.putIfAbsent(id, result);
                }

                if (batchResults.isEmpty()) {
                    break;
                }

                // Fetch vertex properties in batch
                List<String> batchVertexIds = new ArrayList<>(batchResults.keySet());
                Map<String, DynamicVertex> vertexPropertiesMap = dynamicVertexService.retrieveVertices(batchVertexIds);

                if (vertexPropertiesMap == null || vertexPropertiesMap.isEmpty()) {
                    continue;
                }

                // Process each vertex in the batch
                for (String vertexId : batchVertexIds) {
                    DynamicVertex vertex = vertexPropertiesMap.get(vertexId);
                    if (vertex == null) {
                        continue;
                    }

                    Result result = batchResults.get(vertexId);
                    String typeName = vertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
                    if (typeName == null) {
                        continue;
                    }

                    AtlasEntityType type = typeRegistry.getEntityTypeByName(typeName);

                    // Create entity header
                    AtlasEntityHeader header = new AtlasEntityHeader();
                    String guid = vertex.getProperty(GUID_PROPERTY_KEY, String.class);
                    header.setGuid(guid);
                    header.setTypeName(typeName);

                    // Set timestamp properties
                    Long createTime = vertex.getProperty(TIMESTAMP_PROPERTY_KEY, Long.class);
                    if (createTime != null) {
                        header.setCreateTime(new Date(createTime));
                    }

                    header.setCreatedBy(vertex.getProperty(CREATED_BY_KEY, String.class));

                    Long updateTime = vertex.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
                    if (updateTime != null) {
                        header.setUpdateTime(new Date(updateTime));
                    }

                    header.setUpdatedBy(vertex.getProperty(MODIFIED_BY_KEY, String.class));
                    header.setDisplayText(getDisplayText(vertex, type).toString());
                    header.setLabels(parseLabelsString(vertex.getProperty(LABELS_PROPERTY_KEY, String.class)));

                    // Set incomplete flag
                    Integer value = vertex.getProperty(Constants.IS_INCOMPLETE_PROPERTY_KEY, Integer.class);
                    header.setIsIncomplete(value != null && value.equals(INCOMPLETE_ENTITY_VALUE));

                    // Set entity status
                    String state = vertex.getProperty(Constants.STATE_PROPERTY_KEY, String.class);
                    if (state != null) {
                        Id.EntityState entityState = Id.EntityState.valueOf(state);
                        header.setStatus((entityState == Id.EntityState.DELETED) ? AtlasEntity.Status.DELETED : ACTIVE);
                    } else {
                        header.setStatus(ACTIVE);
                    }

                    Set<String> allRequiredAttrs = new HashSet<>();
                    if (type != null) {
                        allRequiredAttrs.addAll(type.getHeaderAttributes().keySet());
                    }
                    allRequiredAttrs.addAll(resultAttributes);

                    // includes primitives, structs, meanings and enums
                    header.setAttributes(filterMapByKeys(type, vertex, allRequiredAttrs));



                    // Handle classifications if needed
                    // this is additional cassandra call per asset in a batch
                    if (includeClassifications || includeClassificationNames) {
                        List<AtlasClassification> tags = entityRetriever.getAllClassifications(vertexId);
                        if (includeClassifications) {
                            header.setClassifications(tags);
                        }
                        if (includeClassificationNames) {
                            header.setClassificationNames(getAllTagNames(tags));
                        }
                    }

                    // Handle meanings if needed
                    if (includeMeanings) {
                        Object meaningsObj = vertex.getProperty("meanings", List.class);
                        if (meaningsObj instanceof List) {
                            List<AtlasTermAssignmentHeader> termAssignmentHeaders = (List<AtlasTermAssignmentHeader>) meaningsObj;
                            header.setMeanings(termAssignmentHeaders);

                            if (!termAssignmentHeaders.isEmpty()) {
                                List<String> meaningNames = new ArrayList<>(termAssignmentHeaders.size());
                                for (AtlasTermAssignmentHeader term : termAssignmentHeaders) {
                                    String displayText = term.getDisplayText();
                                    if (displayText != null) {
                                        meaningNames.add(displayText);
                                    }
                                }
                                header.setMeaningNames(meaningNames);
                            }
                        }
                    }

                    // Handle business attributes
                    Map<String, Map<String, AtlasBusinessMetadataType.AtlasBusinessAttribute>> businessAttributeS = type.getBusinessAttributes();
                    if (MapUtils.isNotEmpty(businessAttributeS)) {
                        for (Map.Entry<String, Map<String, AtlasBusinessMetadataType.AtlasBusinessAttribute>> entry : businessAttributeS.entrySet()) {
                            String businessAttributeName = entry.getKey();
                            for (Map.Entry<String, AtlasBusinessMetadataType.AtlasBusinessAttribute> attributeTypes : entry.getValue().entrySet()) {
                                String attributeTypeName = attributeTypes.getKey();
                                AtlasBusinessMetadataType.AtlasBusinessAttribute businessAttribute = attributeTypes.getValue();
                                AtlasType atlasType = businessAttribute.getAttributeType();
                                String fqAttributeName = businessAttributeName + "." + attributeTypeName;
                                if (resultAttributes.contains(fqAttributeName)) {
                                    Object attributeValue;
                                    if (atlasType.getTypeCategory().equals(TypeCategory.PRIMITIVE)) {
                                        attributeValue = vertex.getProperty(attributeTypeName, getPrimitiveClass(atlasType.getTypeName()));
                                    } else {
                                        attributeValue = vertex.getProperty(attributeTypeName, Object.class);
                                    }
                                    header.setAttribute(fqAttributeName, attributeValue);
                                }
                            }
                        }
                    }


                    // Store for later relation processing
                    vertexIdHeader.put(vertexId, header);

                    // Add search metadata
                    if (showSearchScore) {
                        ret.addEntityScore(guid, result.getScore());
                    }

                    if (showSearchMetadata) {
                        ret.addHighlights(guid, result.getHighLights());
                        ret.addSort(guid, result.getSort());
                    } else if (showHighlights) {
                        ret.addHighlights(guid, result.getHighLights());
                    }

                    ret.addEntity(header);
                }
            }

            Map<String, Map<String, Set<String>>> edgeVertices = mapEdges(vertexIdHeader, resultAttributes,
                    vertexIdHeader);

            // Collect and process all relation vertex IDs with a single Cassandra call
            if (!edgeVertices.isEmpty()) {
                // Extract all relation vertex IDs from all vertices and all attributes
                // Combine all sets into a single list to avoid duplicates and improve performance
                List<String> relationVertexIds = new ArrayList<>();
                Set<String> uniqueRelationIds = new HashSet<>();

                // Go through each vertex's attributes and collect all relation IDs
                for (Map.Entry<String, Map<String, Set<String>>> entry : edgeVertices.entrySet()) {
                    for (Set<String> relatedIds : entry.getValue().values()) {
                        for (String relatedId : relatedIds) {
                            if (uniqueRelationIds.add(relatedId)) {
                                relationVertexIds.add(relatedId);
                            }
                        }
                    }
                }

                if (!relationVertexIds.isEmpty()) {
                    // Single Cassandra call for all relation vertices
                    Map<String, DynamicVertex> vertexRelationsPropertiesMap =
                            dynamicVertexService.retrieveVertices(relationVertexIds);

                    // Process all entity relations
                    for (Map.Entry<String, Map<String, Set<String>>> entry : edgeVertices.entrySet()) {
                        String vertexId = entry.getKey();
                        Map<String, Set<String>> relationsMap = entry.getValue();

                        AtlasEntityHeader header = vertexIdHeader.get(vertexId);
                        if (header == null) {
                            continue;
                        }

                        String typeName = header.getTypeName();

                        for (Map.Entry<String, Set<String>> attributeNameRelationsEntry : relationsMap.entrySet()) {
                            String attribute = attributeNameRelationsEntry.getKey();
                            Set<String> vertexIDs = attributeNameRelationsEntry.getValue();

                            // Map attribute values from Cassandra data
                            Object attributeValue = mapAttributesFromCassandra(
                                    attribute,
                                    typeName,
                                    vertexIDs,
                                    vertexRelationsPropertiesMap
                            );

                            if (attributeValue != null) {
                                header.setAttribute(attribute, attributeValue);
                            }
                        }
                    }
                }
            }

            scrubSearchResults(ret, searchParams.getSuppressLogs());
        } catch (Exception e) {
            LOG.error("Error preparing search results", e);
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private static Class getPrimitiveClass(String attribTypeName) {
        String attributeTypeName = attribTypeName.toLowerCase();

        switch (attributeTypeName) {
            case ATLAS_TYPE_BOOLEAN:
                return Boolean.class;
            case ATLAS_TYPE_BYTE:
                return Byte.class;
            case ATLAS_TYPE_SHORT:
                return Short.class;
            case ATLAS_TYPE_INT:
                return Integer.class;
            case ATLAS_TYPE_LONG:
            case ATLAS_TYPE_DATE:
                return Long.class;
            case ATLAS_TYPE_FLOAT:
                return Float.class;
            case ATLAS_TYPE_DOUBLE:
                return Double.class;
            case ATLAS_TYPE_BIGINTEGER:
                return BigInteger.class;
            case ATLAS_TYPE_BIGDECIMAL:
                return BigDecimal.class;
            case ATLAS_TYPE_STRING:
                return String.class;
        }

        throw new IllegalArgumentException(String.format("Unknown primitive typename %s", attribTypeName));
    }

    private void prepareSearchResult(AtlasSearchResult ret,  DirectIndexQueryResult indexQueryResult, Set<String> resultAttributes,
                                     boolean fetchCollapsedResults, boolean useVertexEdgeBulkFetching) throws AtlasBaseException {
        if (LEAN_GRAPH_ENABLED) {
            fetchCollapsedResults = false; // TODO: V2 doesn't use this flag in this context
            prepareSearchResultV2(ret, indexQueryResult, resultAttributes, fetchCollapsedResults);
        } else {
            prepareSearchResultV1(ret, indexQueryResult, resultAttributes, fetchCollapsedResults, useVertexEdgeBulkFetching);
        }
    }

    public static Map<String, Object> filterMapByKeys(AtlasEntityType entityType, DynamicVertex vertex, Set<String> resultAttributes) {
        if (vertex.getAllProperties() == null || vertex.getAllProperties().isEmpty() || resultAttributes == null || resultAttributes.isEmpty()) {
            return Collections.emptyMap();
        }

        // Estimate capacity to avoid resize operations
        Map<String, Object> filteredMap = new HashMap<>((int) (Math.min(vertex.getAllProperties().size(), resultAttributes.size()) * 0.75) + 1);

        for (Map.Entry<String, Object> entry : vertex.getAllProperties().entrySet()) {
            String attributeName = entry.getKey();

            // Check if the key from the vertex is in the requested resultAttributes, directly or with a "__" prefix in resultAttributes
            // (The latter part of the condition, resultAttributes.contains("__" + attributeName), is preserved from original logic but might need review depending on intent)
            if (resultAttributes.contains(attributeName) || resultAttributes.contains("__" + attributeName)) {
                AtlasStructType.AtlasAttribute atlasAttribute = entityType.getAttribute(attributeName);
                Object propertyValue;

                if (atlasAttribute != null) {
                    AtlasType attrType = atlasAttribute.getAttributeType();
                    Class<?> clazz;
                    switch (attrType.getTypeCategory()) {
                        case PRIMITIVE:
                            clazz = getPrimitiveClass(attrType.getTypeName());
                            break;
                        case STRUCT:
                            clazz = Map.class; // Structs are Map<String, Object>
                            break;
                        case ARRAY:
                            clazz = List.class; // Arrays are List<Object>
                            break;
                        case MAP:
                            clazz = Map.class; // Maps are Map<Object, Object>
                            break;
                        default:
                            LOG.warn("Unhandled attribute type category {} for attribute {} of type {}. Retrieving as Object.class.",
                                    attrType.getTypeCategory(), attributeName, entityType.getTypeName());
                            clazz = Object.class; // Fallback for unhandled or complex types
                            break;
                    }
                    propertyValue = vertex.getProperty(attributeName, clazz);
                } else {
                    // Attribute is requested in resultAttributes but not formally defined in AtlasEntityType.
                    // This can happen for internal attributes (e.g., '__guid') or other dynamic properties.
                    // Retrieve as Object.class and let the underlying system determine the type.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Attribute '{}' requested but not defined in AtlasEntityType '{}'. Retrieving as Object.class.", attributeName, entityType.getTypeName());
                    }
                    propertyValue = vertex.getProperty(attributeName, Object.class);
                }

                // Preserve original behavior: put the property value, which might be null.
                filteredMap.put(attributeName, propertyValue);
            }
        }
        return filteredMap;
    }

    private Object getDisplayText(DynamicVertex dynamicVertex, AtlasEntityType entityType) throws AtlasBaseException {
        Object ret =  dynamicVertex.getProperty(TYPE_DISPLAYNAME_PROPERTY_KEY, String.class);

        if (entityType != null && ret == null) {
            String displayTextAttribute = entityType.getDisplayTextAttribute();

            if (displayTextAttribute != null) {
                ret = dynamicVertex.getProperty(displayTextAttribute, String.class);
            }

            if (ret == null) {
                ret = dynamicVertex.getProperty(NAME, String.class);

                if (ret == null) {
                    ret = dynamicVertex.getProperty(DISPLAY_NAME, String.class);

                    if (ret == null) {
                        ret = dynamicVertex.getProperty(QUALIFIED_NAME, String.class);
                    }
                }
            }
        }
        return ret;
    }

    private Object mapAttributesFromCassandra(String attributeName, String typeName, Set<String> vertexIDs, Map<String, DynamicVertex> vertexRelationsPropertiesMap) throws AtlasBaseException {
        if (vertexIDs == null || vertexIDs.isEmpty() || StringUtils.isEmpty(attributeName) || StringUtils.isEmpty(typeName)) {
            return null;
        }

        // Cache frequently accessed properties
        AtlasEntityType type = typeRegistry.getEntityTypeByName(typeName);
        if (type == null) {
            LOG.warn("Entity type {} not found in type registry", typeName);
            return null;
        }

        // Find relationship attribute type
        Map<String, AtlasStructType.AtlasAttribute> relationshipAttributes = type.getRelationshipAttributes().get(attributeName);
        if (MapUtils.isEmpty(relationshipAttributes)) {
            return null;
        }

        AtlasAttribute relationAttribute = null;
        TypeCategory typeCategory = null;

        // Get the attribute and its type category
        for (Map.Entry<String, AtlasAttribute> entry : type.getRelationshipAttributes().get(attributeName).entrySet()) {
            AtlasAttribute attribute = entry.getValue();
            if (attributeName.equals(attribute.getName())) {
                relationAttribute = attribute;
                typeCategory = attribute.getAttributeType().getTypeCategory();
                break;
            }
        }

        if (relationAttribute == null || typeCategory == null) {
            LOG.warn("Unable to find relationship attribute {} for type {}", attributeName, typeName);
            return null;
        }

        // Get unique attributes for object ID creation
        Map<String, AtlasAttribute> uniqueAttributes = type.getUniqAttributes();

        // Process based on type category
        switch (typeCategory) {
            case ARRAY:
                // Preallocate the list based on number of vertices
                List<AtlasObjectId> list = new ArrayList<>(vertexIDs.size());
                for (String vertexID : vertexIDs) {
                    DynamicVertex dynamicVertex = vertexRelationsPropertiesMap.get(vertexID);
                    if (dynamicVertex != null) {
                        AtlasObjectId atlasObjectId = new AtlasObjectId();
                        atlasObjectId.setGuid(dynamicVertex.getProperty(GUID_PROPERTY_KEY, String.class));
                        atlasObjectId.setTypeName(dynamicVertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class));
                        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(atlasObjectId.getTypeName());
                        atlasObjectId.setUniqueAttributes(
                                filterMapByKeys(entityType, dynamicVertex, uniqueAttributes.keySet())
                        );
                        atlasObjectId.setAttributes(filterMapByKeys(entityType, dynamicVertex, RequestContext.get().getRelationAttrsForSearch()));
                        list.add(atlasObjectId);
                    }
                }
                return list.isEmpty() ? new ArrayList<>() : list;

            case MAP:
            case STRUCT:
                // Both map and struct handle a single vertex similarly
                if (vertexIDs.size() != 1) {
                    LOG.warn("Expected single vertex ID for {}/{} but got {}", typeCategory, attributeName, vertexIDs.size());
                    return null;
                }

                String vertexId = vertexIDs.iterator().next();
                DynamicVertex dynamicVertex = vertexRelationsPropertiesMap.get(vertexId);
                if (dynamicVertex == null) {
                    return null;
                }

                Map<String, Object> propertiesRetrieved = dynamicVertex.getAllProperties();

                if (typeCategory == TypeCategory.STRUCT) {
                    // For struct, wrap in AtlasStruct
                    AtlasStruct struct = new AtlasStruct(typeName);
                    struct.setAttributes(propertiesRetrieved);
                    return struct;
                }

                // For MAP, return the filtered properties directly
                return propertiesRetrieved;

            case OBJECT_ID_TYPE:
                // Handle object ID type
                if (vertexIDs.size() != 1) {
                    LOG.warn("Expected single vertex ID for OBJECT_ID_TYPE/{} but got {}", attributeName, vertexIDs.size());
                    return null;
                }

                vertexId = vertexIDs.iterator().next();
                dynamicVertex = vertexRelationsPropertiesMap.get(vertexId);
                if (dynamicVertex == null) {
                    return null;
                }


                // Get GUID directly without null checks since it's a critical property
                Object guidObj = dynamicVertex.getProperty(GUID_PROPERTY_KEY, String.class);
                if (guidObj == null) {
                    LOG.warn("No GUID found for vertex ID {}", vertexId);
                    return null;
                }

                String relationTypeName= dynamicVertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
                AtlasEntityType relationType = typeRegistry.getEntityTypeByName(relationTypeName);

                String guid = guidObj.toString();

                // Create unique attributes map reusing the filtered properties map
                Map<String, Object> uniqueAttributesMap =  filterMapByKeys(relationType, dynamicVertex, uniqueAttributes.keySet());

                return new AtlasObjectId(guid, relationTypeName, uniqueAttributesMap,
                        filterMapByKeys(relationType, dynamicVertex, RequestContext.get().getRelationAttrsForSearch()));

            default:
                LOG.warn("Unsupported type category {} for attribute {}/{}", typeCategory, typeName, attributeName);
                return null;
        }
    }

    private Map<String, Map<String, Set<String>>> mapEdges(Map<String, AtlasEntityHeader> vertexIdHeader, Set<String> attributes,
                                                           Map<String, AtlasEntityHeader> vertexHeaders) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("mapEdges");
        try {
            if (CollectionUtils.isEmpty(attributes) || MapUtils.isEmpty(vertexHeaders)) {
                return Collections.emptyMap();
            }

            List<String> vertexIds= new ArrayList<>(vertexIdHeader.keySet());
            // Initialize result map: vertexId -> (attribute -> set of related vertexIds)
            Map<String, Map<String, Set<String>>> resultMap = new HashMap<>();
            for (String vertexId : vertexIds) {
                resultMap.put(vertexId, new HashMap<>());
            }

            // Group vertices by type name for efficient processing
            Map<String, List<String>> verticesByType = new HashMap<>();
            for (String vertexId : vertexIds) {
                AtlasEntityHeader header = vertexHeaders.get(vertexId);
                if (header != null) {
                    String typeName = header.getTypeName();
                    if (StringUtils.isNotEmpty(typeName)) {
                        verticesByType.computeIfAbsent(typeName, k -> new ArrayList<>()).add(vertexId);
                    }
                }
            }

            // Process each type separately
            for (Map.Entry<String, List<String>> typeEntry : verticesByType.entrySet()) {
                String typeName = typeEntry.getKey();
                List<String> typeVertexIds = typeEntry.getValue();

                // Get the entity type
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
                if (entityType == null) {
                    LOG.warn("Entity type {} not found in registry", typeName);
                    continue;
                }


                // Get relationships lookup from cache or compute if not present
                Map<String, Set<String>> relationshipsLookup = typeEdgeNamesCache.get(typeName);
                if (relationshipsLookup == null) {
                    AtlasEntityType type = typeRegistry.getEntityTypeByName(typeName);
                    if (type != null) {
                        relationshipsLookup = entityRetriever.fetchEdgeNames(type);
                        typeEdgeNamesCache.put(typeName, relationshipsLookup);
                    } else {
                        relationshipsLookup = Collections.emptyMap();
                    }
                }

                // Create maps to store attributes by direction
                Map<AtlasAttribute.AtlasRelationshipEdgeDirection, Set<String>> attributesByDirection = new HashMap<>();
                attributesByDirection.put(AtlasAttribute.AtlasRelationshipEdgeDirection.IN, new HashSet<>());
                attributesByDirection.put(AtlasAttribute.AtlasRelationshipEdgeDirection.OUT, new HashSet<>());
                // glossary has some attributes that has BOTH as direction
                attributesByDirection.put(AtlasAttribute.AtlasRelationshipEdgeDirection.BOTH, new HashSet<>());

                Map<String, Map<String, AtlasAttribute>>  typeRelationAttributes =  entityType.getRelationshipAttributes();
                // Find direction for each attribute
                for (String attribute : attributes) {

                    if (!typeRelationAttributes.containsKey(attribute)){
                        continue;
                    }

                    AtlasAttribute.AtlasRelationshipEdgeDirection direction = null;
                    Map<String, AtlasAttribute> relationAttributes = typeRelationAttributes.get(attribute);

                    if (MapUtils.isNotEmpty(relationAttributes)) {
                        // Get the first relationship attribute's direction
                        for (AtlasAttribute relationAttribute : relationAttributes.values()) {
                            direction = relationAttribute.getRelationshipEdgeDirection();
                            if (direction != null) {
                                break;
                            }
                        }
                    }

                    attributesByDirection.get(direction).add(attribute);
                }

                // Process edges for each direction
                for (AtlasAttribute.AtlasRelationshipEdgeDirection direction : AtlasAttribute.AtlasRelationshipEdgeDirection.values()) {
                    Set<String> directionAttributes = attributesByDirection.get(direction);
                    if (!directionAttributes.isEmpty()) {
                        processEdgesByDirection(typeVertexIds,
                                directionAttributes, direction, vertexHeaders, resultMap);
                    }
                }
            }

            return resultMap;
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void processEdgesByDirection(List<String> vertexIds,
                                         Set<String> directionAttributes,
                                         AtlasAttribute.AtlasRelationshipEdgeDirection direction,
                                         Map<String, AtlasEntityHeader> vertexIdHeader,
                                         Map<String, Map<String, Set<String>>> resultMap
    ) {
        if (CollectionUtils.isEmpty(vertexIds) || CollectionUtils.isEmpty(directionAttributes)) {
            return;
        }

        // Build the appropriate traversal based on direction
        GraphTraversal traversal= null;
        switch (direction) {
            case IN:
                traversal = graph.V(vertexIds).inE().has(STATE_PROPERTY_KEY, ACTIVE);
                break;
            case OUT:
                traversal = graph.V(vertexIds).outE().has(STATE_PROPERTY_KEY, ACTIVE);
                break;
            case BOTH:
            default:
                traversal = graph.V(vertexIds).bothE().has(STATE_PROPERTY_KEY, ACTIVE);
                break;
        }

        if (traversal == null) {
            return;
        }

        Set<AtlasJanusEdge> edges = ((AtlasJanusGraphTraversal) traversal).getAtlasEdgeSet();

        // Process each edge
        for (AtlasJanusEdge janusEdge : edges) {
            if (janusEdge == null) {
                continue;
            }

            // Get source and target vertex IDs
            String sourceId = ((CacheEdge) janusEdge.getWrappedElement()).getVertex(0).id().toString();
            String targetId = ((CacheEdge) janusEdge.getWrappedElement()).getVertex(1).id().toString();

            // Determine which vertex in our list this edge connects to
            String ourVertexId;
            String otherVertexId;

            if (vertexIds.contains(sourceId)) {
                ourVertexId = sourceId;
                otherVertexId = targetId;
            } else if (vertexIds.contains(targetId)) {
                ourVertexId = targetId;
                otherVertexId = sourceId;
            } else {
                // This edge doesn't connect to any of our vertices, skip it
                continue;
            }

            // Get or create the attribute map for this vertex
            Map<String, Set<String>> attrMap = resultMap.get(ourVertexId);
            if (attrMap == null) {
                attrMap = new HashMap<>();
                resultMap.put(ourVertexId, attrMap);
            }

            AtlasEntityHeader header = vertexIdHeader.get(ourVertexId);
            if (header == null) {
                continue;
            }

            String typeName = header.getTypeName();


            // Get relationships lookup from cache or compute if not present
            Map<String, Set<String>> relationshipsLookup = typeEdgeNamesCache.get(typeName);

            // Check each attribute that matches this direction
            for (String attribute : directionAttributes) {
                // Check if the edge label matches the attribute
                if (janusEdge.getLabel().contains(attribute)) {
                    attrMap.computeIfAbsent(attribute, k -> new HashSet<>()).add(otherVertexId);
                    continue;
                }

                // Check if the edge type matches in the relationshipsLookup
                String edgeTypeName = janusEdge.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);
                if (MapUtils.isNotEmpty(relationshipsLookup) &&
                        relationshipsLookup.containsKey(edgeTypeName) &&
                        relationshipsLookup.get(edgeTypeName).contains(attribute)) {
                    attrMap.computeIfAbsent(attribute, k -> new HashSet<>()).add(otherVertexId);
                }
            }
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
