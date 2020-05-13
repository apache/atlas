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

import org.apache.atlas.SortOrder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.SearchPredicateUtil;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;

/**
 * This class is needed when this is a registered classification type or wildcard search,
 * registered classification includes special type as well. (tag filters will be ignored, and front-end should not enable
 * tag-filter for special classification types, including wildcard search - classification name contains *)
 */
public class ClassificationSearchProcessor extends SearchProcessor {

    private static final Logger LOG       = LoggerFactory.getLogger(ClassificationSearchProcessor.class);
    private static final Logger PERF_LOG  = AtlasPerfTracer.getPerfLogger("ClassificationSearchProcessor");

    private final AtlasIndexQuery        indexQuery;
    private final AtlasIndexQuery        classificationIndexQuery;
    private final AtlasGraphQuery        tagGraphQueryWithAttributes;
    private final Map<String, Object>    gremlinQueryBindings;
    private final String                 gremlinTagFilterQuery;
    private final Predicate              traitPredicate;
    private final Predicate              isEntityPredicate;

    // Some index engines may take space as a delimiter, when basic search is
    // executed, unsatisfying results may be returned.
    // eg, an entity A has classification "cls" and B has "cls 1"
    // when user execute a exact search for "cls", only A should be returned
    // but both A and B are returned. To avoid this, we should filter the res.
    private boolean   whiteSpaceFilter = false;

    public ClassificationSearchProcessor(SearchContext context) {
        super(context);

        final AtlasClassificationType classificationType    = context.getClassificationType();
        final FilterCriteria          filterCriteria        = context.getSearchParameters().getTagFilters();
        final Set<String>             indexAttributes       = new HashSet<>();
        final Set<String>             graphAttributes       = new HashSet<>();
        final Set<String>             allAttributes         = new HashSet<>();
        final Set<String>             typeAndSubTypes       = context.getClassificationTypes();
        final String                  typeAndSubTypesQryStr = context.getClassificationTypesQryStr();
        final boolean isBuiltInType                         = context.isBuiltInClassificationType();
        final boolean isWildcardSearch                      = context.isWildCardSearch();

        processSearchAttributes(classificationType, filterCriteria, indexAttributes, graphAttributes, allAttributes);

        /* for classification search, if any attribute can't be handled by index query - switch to all filter by Graph query
           There are four cases in the classification type :
           1. unique classification type, including not classified, single wildcard (*), match all classified
           2. wildcard search, including starting/ending/mid wildcard, like cls*, *c*, *ion.
           3. registered classification type, like PII, PHI
           4. classification is not present in the search parameter
           each of above cases with either has empty/or not tagFilters
         */
        final boolean useIndexSearchForEntity = (classificationType != null || isWildcardSearch) &&
                                                !context.hasAttributeFilter(filterCriteria)  &&
                                                (typeAndSubTypesQryStr.length() <= MAX_QUERY_STR_LENGTH_TAGS);

        /* If classification's attributes can be applied index filter, we can use direct index
         * to query classification index as well.
         */
        final boolean useIndexSearchForClassification = (!isBuiltInType && !isWildcardSearch) &&
                                                        (typeAndSubTypesQryStr.length() <= MAX_QUERY_STR_LENGTH_TAGS) &&
                                                        CollectionUtils.isNotEmpty(indexAttributes) &&
                                                        canApplyIndexFilter(classificationType, filterCriteria, false);

        traitPredicate    = buildTraitPredict(classificationType);
        isEntityPredicate = SearchPredicateUtil.generateIsEntityVertexPredicate(context.getTypeRegistry());

        AtlasGraph graph = context.getGraph();

        // index query directly on entity
        if (useIndexSearchForEntity) {

            StringBuilder queryString = new StringBuilder();
            graphIndexQueryBuilder.addActiveStateQueryFilter(queryString);

            if (isWildcardSearch) {

                // tagFilters is not allowed in wildcard search
                graphIndexQueryBuilder.addClassificationTypeFilter(queryString);
            } else {
                if (isBuiltInType) {

                    // tagFilters is not allowed in unique classificationType search
                    graphIndexQueryBuilder.addClassificationFilterForBuiltInTypes(queryString);

                } else {

                    // only registered classification will search for subtypes
                    graphIndexQueryBuilder.addClassificationAndSubTypesQueryFilter(queryString);
                    whiteSpaceFilter = true;
                }
            }

            String indexQueryString = STRAY_AND_PATTERN.matcher(queryString).replaceAll(")");
            indexQueryString        = STRAY_OR_PATTERN.matcher(indexQueryString).replaceAll(")");
            indexQueryString        = STRAY_ELIPSIS_PATTERN.matcher(indexQueryString).replaceAll("");
            indexQuery              = graph.indexQuery(Constants.VERTEX_INDEX, indexQueryString);

            LOG.debug("Using query string  '{}'.", indexQuery);
        } else {
            indexQuery = null;
        }

        // index query directly on classification
        if (useIndexSearchForClassification) {

            StringBuilder queryString = new StringBuilder();

            graphIndexQueryBuilder.addActiveStateQueryFilter(queryString);
            graphIndexQueryBuilder.addTypeAndSubTypesQueryFilter(queryString, typeAndSubTypesQryStr);

            constructFilterQuery(queryString, classificationType, filterCriteria, indexAttributes);

            String indexQueryString = STRAY_AND_PATTERN.matcher(queryString).replaceAll(")");
            indexQueryString = STRAY_OR_PATTERN.matcher(indexQueryString).replaceAll(")");
            indexQueryString = STRAY_ELIPSIS_PATTERN.matcher(indexQueryString).replaceAll("");

            Predicate typeNamePredicate  = isClassificationRootType() ? null : SearchPredicateUtil.getINPredicateGenerator().generatePredicate(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes, String.class);

            if (typeNamePredicate != null) {
                inMemoryPredicate = inMemoryPredicate == null ? typeNamePredicate : PredicateUtils.andPredicate(inMemoryPredicate, typeNamePredicate);
            }

            Predicate attributePredicate = constructInMemoryPredicate(classificationType, filterCriteria, indexAttributes);

            if (attributePredicate != null) {
                inMemoryPredicate = inMemoryPredicate == null ? attributePredicate : PredicateUtils.andPredicate(inMemoryPredicate, attributePredicate);
            }

            this.classificationIndexQuery = graph.indexQuery(Constants.VERTEX_INDEX, indexQueryString);
        } else {
            classificationIndexQuery = null;
        }

        // only registered classification will search with tag filters
        if (!isWildcardSearch && !isBuiltInType && !graphAttributes.isEmpty()) {

            AtlasGremlinQueryProvider queryProvider = AtlasGremlinQueryProvider.INSTANCE;
            AtlasGraphQuery query = graph.query();

            if (!isClassificationRootType()) {
                query.in(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes);
            }

            tagGraphQueryWithAttributes = toGraphFilterQuery(classificationType, filterCriteria, allAttributes, query);
            gremlinQueryBindings       = new HashMap<>();
            StringBuilder gremlinQuery = new StringBuilder();

            gremlinQuery.append("g.V().has('__guid', within(guids))");
            gremlinQuery.append(queryProvider.getQuery(AtlasGremlinQueryProvider.AtlasGremlinQuery.BASIC_SEARCH_CLASSIFICATION_FILTER));
            gremlinQuery.append(".as('e').filter(out()");
            gremlinQuery.append(queryProvider.getQuery(AtlasGremlinQueryProvider.AtlasGremlinQuery.BASIC_SEARCH_TYPE_FILTER));

            constructGremlinFilterQuery(gremlinQuery, gremlinQueryBindings, context.getClassificationType(), context.getSearchParameters().getTagFilters());

            // After filtering on tags go back to e and output the list of entity vertices
            gremlinQuery.append(").toList()");

            gremlinQueryBindings.put("traitNames", typeAndSubTypes);
            gremlinQueryBindings.put("typeNames", typeAndSubTypes); // classification typeName

            gremlinTagFilterQuery = gremlinQuery.toString();

            if (LOG.isDebugEnabled()) {
                LOG.debug("gremlinTagFilterQuery={}", gremlinTagFilterQuery);
            }
        } else {
            tagGraphQueryWithAttributes = null;
            gremlinTagFilterQuery = null;
            gremlinQueryBindings = null;
        }
    }

    @Override
    public List<AtlasVertex> execute() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ClassificationSearchProcessor.execute({})", context);
        }

        List<AtlasVertex> ret = new ArrayList<>();

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ClassificationSearchProcessor.execute(" + context +  ")");
        }

        try {
            final int     startIdx   = context.getSearchParameters().getOffset();
            final int     limit      = context.getSearchParameters().getLimit();

            // query to start at 0, even though startIdx can be higher - because few results in earlier retrieval could
            // have been dropped: like non-active-entities or duplicate-entities (same entity pointed to by multiple
            // classifications in the result)
            //
            // first 'startIdx' number of entries will be ignored
            int qryOffset = 0;
            int resultIdx = qryOffset;

            final Set<String>       processedGuids         = new HashSet<>();
            final List<AtlasVertex> entityVertices         = new ArrayList<>();
            final List<AtlasVertex> classificationVertices = new ArrayList<>();

            final String          sortBy                = context.getSearchParameters().getSortBy();
            final SortOrder       sortOrder             = context.getSearchParameters().getSortOrder();

            for (; ret.size() < limit; qryOffset += limit) {
                entityVertices.clear();
                classificationVertices.clear();

                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                boolean isLastResultPage = true;

                if (indexQuery != null) {
                    Iterator<AtlasIndexQuery.Result> queryResult;
                    if (StringUtils.isNotEmpty(sortBy)) {
                        Order qrySortOrder = sortOrder == SortOrder.ASCENDING ? Order.asc : Order.desc;
                        queryResult = indexQuery.vertices(qryOffset, limit, sortBy, qrySortOrder);
                    } else {
                        queryResult = indexQuery.vertices(qryOffset, limit);
                    }

                    getVerticesFromIndexQueryResult(queryResult, entityVertices);
                    isLastResultPage = entityVertices.size() < limit;

                    // Do in-memory filtering
                    CollectionUtils.filter(entityVertices, traitPredicate);
                    CollectionUtils.filter(entityVertices, isEntityPredicate);

                } else {
                    if (tagGraphQueryWithAttributes != null) {

                        Iterator<AtlasVertex> queryResult = tagGraphQueryWithAttributes.vertices(qryOffset, limit).iterator();

                        getVertices(queryResult, classificationVertices);

                        isLastResultPage = classificationVertices.size() < limit;

                    } else if (classificationIndexQuery != null){

                        Iterator<AtlasIndexQuery.Result> queryResult = classificationIndexQuery.vertices(qryOffset, limit);

                        getVerticesFromIndexQueryResult(queryResult, classificationVertices);

                        isLastResultPage = classificationVertices.size() < limit;

                        // Do in-memory filtering before the graph query
                        CollectionUtils.filter(classificationVertices, inMemoryPredicate);
                    }
                }

                // Since tag filters are present, we need to collect the entity vertices after filtering the classification
                // vertex results (as these might be lower in number)
                if (CollectionUtils.isNotEmpty(classificationVertices)) {
                    for (AtlasVertex classificationVertex : classificationVertices) {
                        Iterable<AtlasEdge> edges = classificationVertex.getEdges(AtlasEdgeDirection.IN, Constants.CLASSIFICATION_LABEL);

                        for (AtlasEdge edge : edges) {
                            AtlasVertex entityVertex = edge.getOutVertex();

                            String guid = AtlasGraphUtilsV2.getIdFromVertex(entityVertex);

                            if (processedGuids.contains(guid)) {
                                continue;
                            }

                            entityVertices.add(entityVertex);

                            processedGuids.add(guid);
                        }
                    }
                }

                if (whiteSpaceFilter) {
                    filterWhiteSpaceClassification(entityVertices);
                }
                    // Do in-memory filtering
                CollectionUtils.filter(entityVertices, isEntityPredicate);

                super.filter(entityVertices);

                resultIdx = collectResultVertices(ret, startIdx, limit, resultIdx, entityVertices);

                if (isLastResultPage) {
                    break;
                }
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ClassificationSearchProcessor.execute({}): ret.size()={}", context, ret.size());
        }

        return ret;
    }

    @Override
    public void filter(List<AtlasVertex> entityVertices) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ClassificationSearchProcessor.filter({})", entityVertices.size());
        }
        //in case of classification type + graph attributes
        if (gremlinTagFilterQuery != null && gremlinQueryBindings != null) {
            // Now filter on the tag attributes
            Set<String> guids = getGuids(entityVertices);

            // Clear prior results
            entityVertices.clear();

            if (CollectionUtils.isNotEmpty(guids)) {
                gremlinQueryBindings.put("guids", guids);

                try {
                    AtlasGraph        graph               = context.getGraph();
                    ScriptEngine      gremlinScriptEngine = graph.getGremlinScriptEngine();
                    List<AtlasVertex> atlasVertices       = (List<AtlasVertex>) graph.executeGremlinScript(gremlinScriptEngine, gremlinQueryBindings, gremlinTagFilterQuery, false);

                    if (CollectionUtils.isNotEmpty(atlasVertices)) {
                        entityVertices.addAll(atlasVertices);
                    }
                } catch (AtlasBaseException | ScriptException e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
        } else if (inMemoryPredicate != null) {
            //in case of classification type + index attributes
            CollectionUtils.filter(entityVertices, traitPredicate);

            //filter attributes (filterCriteria). Find classification vertex(typeName = classification) from entity vertex (traitName = classification)
            final Set<String> processedGuids = new HashSet<>();
            List<AtlasVertex> matchEntityVertices = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(entityVertices)) {
                for (AtlasVertex entityVertex : entityVertices) {
                    Iterable<AtlasEdge> edges = entityVertex.getEdges(AtlasEdgeDirection.OUT, Constants.CLASSIFICATION_LABEL);

                    for (AtlasEdge edge : edges) {
                        AtlasVertex classificationVertex = edge.getInVertex();

                        AtlasVertex matchVertex = (AtlasVertex) CollectionUtils.find(Collections.singleton(classificationVertex), inMemoryPredicate);
                        if (matchVertex != null) {
                            String guid = AtlasGraphUtilsV2.getIdFromVertex(entityVertex);

                            if (processedGuids.contains(guid)) {
                                continue;
                            }

                            matchEntityVertices.add(entityVertex);
                            processedGuids.add(guid);
                            break;

                        }
                    }
                }
            }
            entityVertices.clear();
            entityVertices.addAll(matchEntityVertices);

        } else {
            //in case of only classsification type
            CollectionUtils.filter(entityVertices, traitPredicate);
            CollectionUtils.filter(entityVertices, isEntityPredicate);
        }

        super.filter(entityVertices);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ClassificationSearchProcessor.filter(): ret.size()={}", entityVertices.size());
        }
    }

    @Override
    public long getResultCount() {
        return (indexQuery != null) ? indexQuery.vertexTotals() : -1;
    }
}
