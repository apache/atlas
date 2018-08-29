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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.SearchPredicateUtil;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_CLASSIFIED;
import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_NOT_CLASSIFIED;
import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_WILDCARD_CLASSIFICATION;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator.EQUAL;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator.NOT_EQUAL;


public class ClassificationSearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(ClassificationSearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("ClassificationSearchProcessor");

    private final AtlasIndexQuery indexQuery;
    private final AtlasGraphQuery tagGraphQueryWithAttributes;
    private final AtlasGraphQuery entityGraphQueryTraitNames;
    private       Predicate       entityPredicateTraitNames;

    private final String              gremlinTagFilterQuery;
    private final Map<String, Object> gremlinQueryBindings;


    public ClassificationSearchProcessor(SearchContext context) {
        super(context);

        final AtlasClassificationType classificationType = context.getClassificationType();
        final FilterCriteria          filterCriteria     = context.getSearchParameters().getTagFilters();
        final Set<String>             indexAttributes    = new HashSet<>();
        final Set<String>             graphAttributes    = new HashSet<>();
        final Set<String>             allAttributes      = new HashSet<>();
        final Set<String>             typeAndSubTypes;
        final String                  typeAndSubTypesQryStr;

        if (context.getSearchParameters().getIncludeSubClassifications()) {
            typeAndSubTypes       = classificationType.getTypeAndAllSubTypes();
            typeAndSubTypesQryStr = classificationType.getTypeAndAllSubTypesQryStr();
        } else {
            typeAndSubTypes       = Collections.singleton(classificationType.getTypeName());
            typeAndSubTypesQryStr = classificationType.getTypeQryStr();
        }

        processSearchAttributes(classificationType, filterCriteria, indexAttributes, graphAttributes, allAttributes);

        // for classification search, if any attribute can't be handled by index query - switch to all filter by Graph query
        boolean useIndexSearch = classificationType != MATCH_ALL_WILDCARD_CLASSIFICATION &&
                                 classificationType != MATCH_ALL_CLASSIFIED &&
                                 classificationType != MATCH_ALL_NOT_CLASSIFIED &&
                                 typeAndSubTypesQryStr.length() <= MAX_QUERY_STR_LENGTH_TAGS &&
                                 CollectionUtils.isEmpty(graphAttributes) &&
                                 canApplyIndexFilter(classificationType, filterCriteria, false);

        AtlasGraph graph = context.getGraph();

        if (useIndexSearch) {
            StringBuilder indexQuery = new StringBuilder();

            constructTypeTestQuery(indexQuery, typeAndSubTypesQryStr);
            constructFilterQuery(indexQuery, classificationType, filterCriteria, indexAttributes);

            String indexQueryString = STRAY_AND_PATTERN.matcher(indexQuery).replaceAll(")");

            indexQueryString = STRAY_OR_PATTERN.matcher(indexQueryString).replaceAll(")");
            indexQueryString = STRAY_ELIPSIS_PATTERN.matcher(indexQueryString).replaceAll("");

            this.indexQuery = graph.indexQuery(Constants.VERTEX_INDEX, indexQueryString);

            Predicate typeNamePredicate  = SearchPredicateUtil.getINPredicateGenerator()
                                                              .generatePredicate(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes, String.class);
            Predicate attributePredicate = constructInMemoryPredicate(classificationType, filterCriteria, indexAttributes);
            if (attributePredicate != null) {
                inMemoryPredicate = PredicateUtils.andPredicate(typeNamePredicate, attributePredicate);
            } else {
                inMemoryPredicate = typeNamePredicate;
            }
        } else {
            indexQuery = null;
        }

        if (context.getSearchParameters().getTagFilters() != null) {
            // Now filter on the tag attributes
            AtlasGremlinQueryProvider queryProvider = AtlasGremlinQueryProvider.INSTANCE;

            tagGraphQueryWithAttributes = toGraphFilterQuery(classificationType, filterCriteria, allAttributes, graph.query().in(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes));
            entityGraphQueryTraitNames  = null;
            entityPredicateTraitNames   = null;

            gremlinQueryBindings = new HashMap<>();

            StringBuilder gremlinQuery = new StringBuilder();
            gremlinQuery.append("g.V().has('__guid', T.in, guids)");
            gremlinQuery.append(queryProvider.getQuery(AtlasGremlinQueryProvider.AtlasGremlinQuery.BASIC_SEARCH_CLASSIFICATION_FILTER));
            gremlinQuery.append(".as('e').out()");
            gremlinQuery.append(queryProvider.getQuery(AtlasGremlinQueryProvider.AtlasGremlinQuery.BASIC_SEARCH_TYPE_FILTER));

            constructGremlinFilterQuery(gremlinQuery, gremlinQueryBindings, context.getClassificationType(), context.getSearchParameters().getTagFilters());
            // After filtering on tags go back to e and output the list of entity vertices
            gremlinQuery.append(".back('e').toList()");

            gremlinQueryBindings.put("traitNames", typeAndSubTypes);
            gremlinQueryBindings.put("typeNames", typeAndSubTypes); // classification typeName

            gremlinTagFilterQuery = gremlinQuery.toString();

            if (LOG.isDebugEnabled()) {
                LOG.debug("gremlinTagFilterQuery={}", gremlinTagFilterQuery);
            }
        } else {
            tagGraphQueryWithAttributes         = null;
            List<AtlasGraphQuery> orConditions  = new LinkedList<>();

            if (classificationType == MATCH_ALL_WILDCARD_CLASSIFICATION || classificationType == MATCH_ALL_CLASSIFIED) {
                orConditions.add(graph.query().createChildQuery().has(TRAIT_NAMES_PROPERTY_KEY, NOT_EQUAL, null));
                orConditions.add(graph.query().createChildQuery().has(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, NOT_EQUAL, null));

                entityGraphQueryTraitNames = graph.query().or(orConditions);
                entityPredicateTraitNames  = PredicateUtils.orPredicate(
                        SearchPredicateUtil.getNotEmptyPredicateGenerator().generatePredicate(TRAIT_NAMES_PROPERTY_KEY, null, List.class),
                        SearchPredicateUtil.getNotEmptyPredicateGenerator().generatePredicate(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, null, List.class));
            } else if (classificationType == MATCH_ALL_NOT_CLASSIFIED) {
                orConditions.add(graph.query().createChildQuery().has(GUID_PROPERTY_KEY, NOT_EQUAL, null).has(ENTITY_TYPE_PROPERTY_KEY, NOT_EQUAL, null)
                                                                 .has(TRAIT_NAMES_PROPERTY_KEY, EQUAL, null).has(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, EQUAL, null));

                entityGraphQueryTraitNames = graph.query().or(orConditions);
                entityPredicateTraitNames  = PredicateUtils.andPredicate(
                        SearchPredicateUtil.getIsNullPredicateGenerator().generatePredicate(TRAIT_NAMES_PROPERTY_KEY, null, List.class),
                        SearchPredicateUtil.getIsNullPredicateGenerator().generatePredicate(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, null, List.class));
            } else {
                orConditions.add(graph.query().createChildQuery().in(TRAIT_NAMES_PROPERTY_KEY, typeAndSubTypes));
                orConditions.add(graph.query().createChildQuery().in(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, typeAndSubTypes));

                entityGraphQueryTraitNames = graph.query().or(orConditions);
                entityPredicateTraitNames  = PredicateUtils.orPredicate(
                        SearchPredicateUtil.getContainsAnyPredicateGenerator().generatePredicate(TRAIT_NAMES_PROPERTY_KEY, classificationType.getTypeAndAllSubTypes(), List.class),
                        SearchPredicateUtil.getContainsAnyPredicateGenerator().generatePredicate(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationType.getTypeAndAllSubTypes(), List.class));
            }

            if (context.getSearchParameters().getExcludeDeletedEntities()) {
                entityGraphQueryTraitNames.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");

                final Predicate activePredicate = SearchPredicateUtil.getEQPredicateGenerator().generatePredicate(Constants.STATE_PROPERTY_KEY, "ACTIVE", String.class);

                entityPredicateTraitNames = PredicateUtils.andPredicate(entityPredicateTraitNames, activePredicate);
            }

            gremlinTagFilterQuery = null;
            gremlinQueryBindings  = null;
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
            final boolean activeOnly = context.getSearchParameters().getExcludeDeletedEntities();

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

            for (; ret.size() < limit; qryOffset += limit) {
                entityVertices.clear();
                classificationVertices.clear();

                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                final boolean isLastResultPage;

                if (indexQuery != null) {
                    Iterator<AtlasIndexQuery.Result> queryResult = indexQuery.vertices(qryOffset, limit);

                    getVerticesFromIndexQueryResult(queryResult, classificationVertices);

                    isLastResultPage = classificationVertices.size() < limit;

                    // Do in-memory filtering before the graph query
                    CollectionUtils.filter(classificationVertices, inMemoryPredicate);
                } else {
                    if (context.getSearchParameters().getTagFilters() == null) {
                        // We can use single graph query to determine in this case
                        Iterator<AtlasVertex> queryResult = entityGraphQueryTraitNames.vertices(qryOffset, limit).iterator();

                        getVertices(queryResult, entityVertices);

                        isLastResultPage = entityVertices.size() < limit;
                    } else {
                        Iterator<AtlasVertex> queryResult = tagGraphQueryWithAttributes.vertices(qryOffset, limit).iterator();

                        getVertices(queryResult, classificationVertices);

                        isLastResultPage = classificationVertices.size() < limit;

                        // Do in-memory filtering before the graph query
                        CollectionUtils.filter(classificationVertices, inMemoryPredicate);
                    }
                }

                // Since tag filters are present, we need to collect the entity vertices after filtering the classification
                // vertex results (as these might be lower in number)
                if (CollectionUtils.isNotEmpty(classificationVertices)) {
                    for (AtlasVertex classificationVertex : classificationVertices) {
                        Iterable<AtlasEdge> edges = classificationVertex.getEdges(AtlasEdgeDirection.IN);

                        for (AtlasEdge edge : edges) {
                            AtlasVertex entityVertex = edge.getOutVertex();

                            if (activeOnly && AtlasGraphUtilsV2.getState(entityVertex) != AtlasEntity.Status.ACTIVE) {
                                continue;
                            }

                            String guid = AtlasGraphUtilsV2.getIdFromVertex(entityVertex);

                            if (processedGuids.contains(guid)) {
                                continue;
                            }

                            entityVertices.add(entityVertex);

                            processedGuids.add(guid);
                        }
                    }
                }

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
        } else if (entityPredicateTraitNames != null) {
            CollectionUtils.filter(entityVertices, entityPredicateTraitNames);
        }

        super.filter(entityVertices);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ClassificationSearchProcessor.filter(): ret.size()={}", entityVertices.size());
        }
    }
}
