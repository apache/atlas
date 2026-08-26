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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQueryParameter;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GraphIndexQueryParameters;
import org.apache.atlas.repository.graphdb.QuickSearchContext;
import org.apache.atlas.repository.graphdb.QuickSearchResult;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;

/**
 * This class is equivalent to legacy FullTextSearchProcessor--except that it uses a better search techniques using SOLR
 * than going through Janus Graph index apis.
 */
public class FreeTextSearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(FreeTextSearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("FreeTextSearchProcessor");

    public static final  String SOLR_QT_PARAMETER         = "qt"; // org.apache.solr.common.params.CommonParams.QT;
    public static final  String SOLR_REQUEST_HANDLER_NAME = "/freetext";

    private static final boolean IS_SOLR_INDEX_BACKEND       = isSolrIndexBackend();
    private static final boolean IS_OPENSEARCH_INDEX_BACKEND = isOpenSearchIndexBackend();

    private final AtlasIndexQuery indexQuery;
    private final boolean         useOpenSearchQuickSearch;
    private final String          openSearchQueryString;
    private final Set<String>     openSearchClassificationTypeNames;
    private final Map<String, String> openSearchIndexFieldNameCache;

    private long openSearchTotalCount = -1L;

    public FreeTextSearchProcessor(SearchContext context) {
        super(context);

        SearchParameters searchParameters = context.getSearchParameters();

        if (IS_OPENSEARCH_INDEX_BACKEND) {
            this.useOpenSearchQuickSearch          = true;
            this.indexQuery                        = null;
            this.openSearchQueryString             = searchParameters.getQuery();
            this.openSearchClassificationTypeNames = resolveOpenSearchClassificationTypeNames(context);
            this.openSearchIndexFieldNameCache     = buildOpenSearchIndexFieldNameCache(context.getTypeRegistry());

            LOG.debug("Using OpenSearch weighted quick-search for query '{}'.", openSearchQueryString);
        } else {
            this.useOpenSearchQuickSearch          = false;
            this.openSearchQueryString             = null;
            this.openSearchClassificationTypeNames = Collections.emptySet();
            this.openSearchIndexFieldNameCache     = Collections.emptyMap();

            StringBuilder queryString = new StringBuilder();

            queryString.append(searchParameters.getQuery());

            if (CollectionUtils.isNotEmpty(context.getEntityTypeNames()) && context.getEntityTypesQryStr().length() <= MAX_QUERY_STR_LENGTH_TYPES) {
                queryString.append(AND_STR).append(context.getEntityTypesQryStr());
            }

            graphIndexQueryBuilder.addActiveStateQueryFilter(queryString);

            if (CollectionUtils.isNotEmpty(context.getClassificationTypeNames()) && context.getClassificationTypesQryStr().length() <= MAX_QUERY_STR_LENGTH_TYPES) {
                queryString.append(AND_STR).append(context.getClassificationTypesQryStr());
            }

            LOG.debug("Using query string '{}'.", queryString);

            indexQuery = context.getGraph().indexQuery(prepareGraphIndexQueryParameters(context, queryString));
        }
    }

    @Override
    public List<AtlasVertex> execute() {
        LOG.debug("==> FreeTextSearchProcessor.execute({})", context);

        if (useOpenSearchQuickSearch) {
            return executeOpenSearchQuickSearch();
        }

        List<AtlasVertex> ret  = new ArrayList<>();
        AtlasPerfTracer   perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FreeTextSearchProcessor.execute(" + context + ")");
        }

        try {
            final int     limit    = context.getSearchParameters().getLimit();
            final Integer marker   = context.getMarker();
            final int     startIdx = marker != null ? marker : context.getSearchParameters().getOffset();

            // query to start at 0, even though startIdx can be higher - because few results in earlier retrieval could
            // have been dropped: like vertices of non-entity or non-active-entity
            //
            // first 'startIdx' number of entries will be ignored
            // if marker is provided, start query with marker offset
            int qryOffset = marker != null ? marker : 0;
            int resultIdx = qryOffset;

            LinkedHashMap<Integer, AtlasVertex> offsetEntityVertexMap = new LinkedHashMap<>();

            try {
                for (; ret.size() < limit; qryOffset += limit) {
                    offsetEntityVertexMap.clear();

                    if (context.terminateSearch()) {
                        LOG.warn("query terminated: {}", context.getSearchParameters());

                        break;
                    }

                    Iterator<AtlasIndexQuery.Result> idxQueryResult = executeIndexQuery(context, indexQuery, qryOffset, limit);

                    final boolean isLastResultPage;
                    int           resultCount = 0;

                    while (idxQueryResult.hasNext()) {
                        AtlasVertex vertex = idxQueryResult.next().getVertex();

                        resultCount++;

                        String entityTypeName = AtlasGraphUtilsV2.getTypeName(vertex);

                        // skip non-entity vertices
                        if (StringUtils.isEmpty(entityTypeName) || StringUtils.isEmpty(AtlasGraphUtilsV2.getIdFromVertex(vertex))) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("FreeTextSearchProcessor.execute(): ignoring non-entity vertex (id={})", vertex.getId());
                            }

                            continue;
                        }

                        //skip internalTypes
                        AtlasEntityType entityType = context.getTypeRegistry().getEntityTypeByName(entityTypeName);

                        if (entityType != null && entityType.isInternalType()) {
                            continue;
                        }

                        if (!context.includeEntityType(entityTypeName)) {
                            continue;
                        }

                        if (CollectionUtils.isNotEmpty(context.getClassificationTypes())) {
                            List<String> entityClassifications = GraphHelper.getAllTraitNames(vertex);

                            if (!context.includeClassificationTypes(entityClassifications)) {
                                continue;
                            }
                        }

                        offsetEntityVertexMap.put((qryOffset + resultCount) - 1, vertex);
                    }

                    isLastResultPage      = resultCount < limit;
                    offsetEntityVertexMap = super.filter(offsetEntityVertexMap);
                    resultIdx             = collectResultVertices(ret, startIdx, limit, resultIdx, offsetEntityVertexMap, marker);

                    if (isLastResultPage) {
                        resultIdx = SearchContext.MarkerUtil.MARKER_END - 1;

                        break;
                    }
                }
            } catch (Throwable t) {
                throw t;
            }

            if (marker != null) {
                nextOffset = resultIdx + 1;
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }

        LOG.debug("<== FreeTextSearchProcessor.execute({}): ret.size()={}", context, ret.size());

        return ret;
    }

    @Override
    public long getResultCount() {
        if (useOpenSearchQuickSearch) {
            if (openSearchTotalCount < 0L) {
                fetchOpenSearchTotalCount();
            }

            return openSearchTotalCount;
        }

        return indexQuery.vertexTotals();
    }

    private List<AtlasVertex> executeOpenSearchQuickSearch() {
        List<AtlasVertex> ret  = new ArrayList<>();
        AtlasPerfTracer   perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FreeTextSearchProcessor.executeOpenSearch(" + context + ")");
        }

        try {
            final int     limit    = context.getSearchParameters().getLimit();
            final Integer marker   = context.getMarker();
            final int     startIdx = marker != null ? marker : context.getSearchParameters().getOffset();
            int           qryOffset = marker != null ? marker : 0;
            int           resultIdx = qryOffset;

            LinkedHashMap<Integer, AtlasVertex> offsetEntityVertexMap = new LinkedHashMap<>();

            for (; ret.size() < limit; qryOffset += limit) {
                offsetEntityVertexMap.clear();

                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                QuickSearchResult searchResult = runOpenSearchQuickSearch(qryOffset, limit);
                List<String>      guids        = searchResult.getEntityGuids();

                if (openSearchTotalCount < 0L) {
                    openSearchTotalCount = searchResult.getTotalCount();
                }

                int resultCount = 0;

                for (String guid : guids) {
                    resultCount++;

                    AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(context.getGraph(), guid);

                    if (vertex == null) {
                        continue;
                    }

                    String entityTypeName = AtlasGraphUtilsV2.getTypeName(vertex);

                    if (StringUtils.isEmpty(entityTypeName) || StringUtils.isEmpty(AtlasGraphUtilsV2.getIdFromVertex(vertex))) {
                        continue;
                    }

                    AtlasEntityType entityType = context.getTypeRegistry().getEntityTypeByName(entityTypeName);

                    if (entityType != null && entityType.isInternalType()) {
                        continue;
                    }

                    if (!context.includeEntityType(entityTypeName)) {
                        continue;
                    }

                    if (CollectionUtils.isNotEmpty(context.getClassificationTypes())) {
                        List<String> entityClassifications = GraphHelper.getAllTraitNames(vertex);

                        if (!context.includeClassificationTypes(entityClassifications)) {
                            continue;
                        }
                    }

                    offsetEntityVertexMap.put((qryOffset + resultCount) - 1, vertex);
                }

                boolean isLastResultPage = resultCount < limit;

                offsetEntityVertexMap = super.filter(offsetEntityVertexMap);
                resultIdx             = collectResultVertices(ret, startIdx, limit, resultIdx, offsetEntityVertexMap, marker);

                if (isLastResultPage) {
                    resultIdx = SearchContext.MarkerUtil.MARKER_END - 1;

                    break;
                }
            }

            if (marker != null) {
                nextOffset = resultIdx + 1;
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }

        LOG.debug("<== FreeTextSearchProcessor.executeOpenSearch({}): ret.size()={}", context, ret.size());

        return ret;
    }

    private QuickSearchResult runOpenSearchQuickSearch(int offset, int limit) {
        SearchParameters searchParameters = context.getSearchParameters();
        Set<AtlasEntityType> entityTypesForQuery = resolveOpenSearchEntityTypes(context);
        QuickSearchContext quickSearchContext = new QuickSearchContext(openSearchQueryString,
                searchParameters.getEntityFilters(), entityTypesForQuery, openSearchClassificationTypeNames,
                openSearchIndexFieldNameCache, searchParameters.getExcludeDeletedEntities(),
                searchParameters.getIncludeSubTypes(), offset, limit);

        try {
            AtlasGraphIndexClient graphIndexClient = context.getGraph().getGraphIndexClient();

            return graphIndexClient.quickSearch(quickSearchContext);
        } catch (AtlasException e) {
            LOG.error("Failed to run OpenSearch weighted quick search.", e);

            return new QuickSearchResult(Collections.emptyList(), 0L);
        }
    }

    private void fetchOpenSearchTotalCount() {
        QuickSearchResult countProbe = runOpenSearchQuickSearch(0, 0);

        openSearchTotalCount = countProbe.getTotalCount();
    }

    public static Set<String> resolveOpenSearchClassificationTypeNames(SearchContext context) {
        if (CollectionUtils.isEmpty(context.getClassificationTypeNames())
                || context.getClassificationTypesQryStr().length() > MAX_QUERY_STR_LENGTH_TYPES) {
            return Collections.emptySet();
        }

        return new HashSet<>(context.getClassificationTypeNames());
    }

    public static Set<AtlasEntityType> resolveOpenSearchEntityTypes(SearchContext context) {
        if (CollectionUtils.isEmpty(context.getEntityTypeNames())
                || context.getEntityTypesQryStr().length() > MAX_QUERY_STR_LENGTH_TYPES) {
            return Collections.emptySet();
        }

        return context.getEntityTypes();
    }

    public static Map<String, String> buildOpenSearchIndexFieldNameCache(AtlasTypeRegistry typeRegistry) {
        Map<String, String> cache = new HashMap<>();

        putIndexFieldName(cache, typeRegistry, Constants.ENTITY_TYPE_PROPERTY_KEY);
        putIndexFieldName(cache, typeRegistry, Constants.STATE_PROPERTY_KEY);
        putIndexFieldName(cache, typeRegistry, CLASSIFICATION_NAMES_KEY);
        putIndexFieldName(cache, typeRegistry, PROPAGATED_CLASSIFICATION_NAMES_KEY);

        return cache;
    }

    private static void putIndexFieldName(Map<String, String> cache, AtlasTypeRegistry typeRegistry, String propertyKey) {
        String indexFieldName = typeRegistry.getIndexFieldName(propertyKey);

        if (StringUtils.isNotEmpty(indexFieldName)) {
            cache.put(propertyKey, indexFieldName);
        }
    }

    private GraphIndexQueryParameters prepareGraphIndexQueryParameters(SearchContext context, StringBuilder queryString) {
        List<AtlasIndexQueryParameter> parameters = new ArrayList<>();

        if (IS_SOLR_INDEX_BACKEND) {
            parameters.add(context.getGraph().indexQueryParameter(SOLR_QT_PARAMETER, SOLR_REQUEST_HANDLER_NAME));
        }

        return new GraphIndexQueryParameters(Constants.VERTEX_INDEX, queryString.toString(), 0, parameters);
    }

    private static boolean isSolrIndexBackend() {
        try {
            String indexBackEnd = ApplicationProperties.get().getString(ApplicationProperties.INDEX_BACKEND_CONF);

            return ApplicationProperties.INDEX_BACKEND_SOLR.equalsIgnoreCase(indexBackEnd);
        } catch (AtlasException e) {
            LOG.error("Failed to get application property {}. Assuming Solr index backend", ApplicationProperties.INDEX_BACKEND_SOLR, e);
        }

        return true; // default to Solr
    }

    public static boolean isOpenSearchIndexBackend() {
        try {
            String indexBackEnd = ApplicationProperties.get().getString(ApplicationProperties.INDEX_BACKEND_CONF);

            return ApplicationProperties.INDEX_BACKEND_OPENSEARCH.equalsIgnoreCase(indexBackEnd);
        } catch (AtlasException e) {
            LOG.error("Failed to get application property {}.", ApplicationProperties.INDEX_BACKEND_CONF, e);
        }

        return false;
    }
}
