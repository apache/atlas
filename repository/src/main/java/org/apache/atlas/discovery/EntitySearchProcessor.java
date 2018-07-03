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

import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.util.SearchPredicateUtil;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_CLASSIFIED;
import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_NOT_CLASSIFIED;
import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_WILDCARD_CLASSIFICATION;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator.EQUAL;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator.NOT_EQUAL;

public class EntitySearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(EntitySearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("EntitySearchProcessor");

    private final AtlasIndexQuery indexQuery;
    private final AtlasGraphQuery graphQuery;
    private       Predicate       graphQueryPredicate;
    private       Predicate       filterGraphQueryPredicate;

    public EntitySearchProcessor(SearchContext context) {
        super(context);

        final AtlasEntityType entityType      = context.getEntityType();
        final FilterCriteria  filterCriteria  = context.getSearchParameters().getEntityFilters();
        final Set<String>     indexAttributes = new HashSet<>();
        final Set<String>     graphAttributes = new HashSet<>();
        final Set<String>     allAttributes   = new HashSet<>();
        final Set<String>     typeAndSubTypes;
        final String          typeAndSubTypesQryStr;

        if (context.getSearchParameters().getIncludeSubTypes()) {
            typeAndSubTypes       = entityType.getTypeAndAllSubTypes();
            typeAndSubTypesQryStr = entityType.getTypeAndAllSubTypesQryStr();
        } else {
            typeAndSubTypes       = Collections.singleton(entityType.getTypeName());
            typeAndSubTypesQryStr = entityType.getTypeQryStr();
        }

        final AtlasClassificationType classificationType = context.getClassificationType();
        final boolean                 filterClassification;
        final Set<String>             classificationTypeAndSubTypes;

        if (classificationType != null) {
            filterClassification = !context.needClassificationProcessor();

            if (context.getSearchParameters().getIncludeSubClassifications()) {
                classificationTypeAndSubTypes = classificationType.getTypeAndAllSubTypes();
            } else {
                classificationTypeAndSubTypes = Collections.singleton(classificationType.getTypeName());
            }
        } else {
            filterClassification          = false;
            classificationTypeAndSubTypes = Collections.emptySet();
        }

        final Predicate typeNamePredicate = SearchPredicateUtil.getINPredicateGenerator()
                                                               .generatePredicate(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes, String.class);
        final Predicate activePredicate   = SearchPredicateUtil.getEQPredicateGenerator()
                                                               .generatePredicate(Constants.STATE_PROPERTY_KEY, "ACTIVE", String.class);
        final Predicate traitPredicate;

        if (classificationType == MATCH_ALL_WILDCARD_CLASSIFICATION || classificationType == MATCH_ALL_CLASSIFIED) {
            traitPredicate = PredicateUtils.orPredicate(SearchPredicateUtil.getNotEmptyPredicateGenerator().generatePredicate(TRAIT_NAMES_PROPERTY_KEY, null, List.class),
                                                        SearchPredicateUtil.getNotEmptyPredicateGenerator().generatePredicate(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, null, List.class));
        } else if (classificationType == MATCH_ALL_NOT_CLASSIFIED) {
            traitPredicate = PredicateUtils.andPredicate(SearchPredicateUtil.getIsNullPredicateGenerator().generatePredicate(TRAIT_NAMES_PROPERTY_KEY, null, List.class),
                                                         SearchPredicateUtil.getIsNullPredicateGenerator().generatePredicate(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, null, List.class));
        } else {
            traitPredicate = PredicateUtils.orPredicate(SearchPredicateUtil.getContainsAnyPredicateGenerator().generatePredicate(TRAIT_NAMES_PROPERTY_KEY, classificationTypeAndSubTypes, List.class),
                                                        SearchPredicateUtil.getContainsAnyPredicateGenerator().generatePredicate(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationTypeAndSubTypes, List.class));
        }

        processSearchAttributes(entityType, filterCriteria, indexAttributes, graphAttributes, allAttributes);

        final boolean typeSearchByIndex = !filterClassification && typeAndSubTypesQryStr.length() <= MAX_QUERY_STR_LENGTH_TYPES;
        final boolean attrSearchByIndex = !filterClassification && CollectionUtils.isNotEmpty(indexAttributes) && canApplyIndexFilter(entityType, filterCriteria, false);

        StringBuilder indexQuery = new StringBuilder();

        if (typeSearchByIndex) {
            constructTypeTestQuery(indexQuery, typeAndSubTypesQryStr);

            // TypeName check to be done in-memory as well to address ATLAS-2121 (case sensitivity)
            inMemoryPredicate = typeNamePredicate;
        }

        if (attrSearchByIndex) {
            constructFilterQuery(indexQuery, entityType, filterCriteria, indexAttributes);

            Predicate attributePredicate = constructInMemoryPredicate(entityType, filterCriteria, indexAttributes);
            if (inMemoryPredicate != null) {
                inMemoryPredicate = PredicateUtils.andPredicate(inMemoryPredicate, attributePredicate);
            } else {
                inMemoryPredicate = attributePredicate;
            }
        } else {
            graphAttributes.addAll(indexAttributes);
        }

        if (indexQuery.length() > 0) {
            if (context.getSearchParameters().getExcludeDeletedEntities()) {
                constructStateTestQuery(indexQuery);
            }

            String indexQueryString = STRAY_AND_PATTERN.matcher(indexQuery).replaceAll(")");

            indexQueryString = STRAY_OR_PATTERN.matcher(indexQueryString).replaceAll(")");
            indexQueryString = STRAY_ELIPSIS_PATTERN.matcher(indexQueryString).replaceAll("");

            this.indexQuery = context.getGraph().indexQuery(Constants.VERTEX_INDEX, indexQueryString);
        } else {
            this.indexQuery = null;
        }

        if (CollectionUtils.isNotEmpty(graphAttributes) || !typeSearchByIndex) {
            AtlasGraphQuery query = context.getGraph().query();

            if (!typeSearchByIndex) {
                query.in(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes);
            }

            // If we need to filter on the trait names then we need to build the query and equivalent in-memory predicate
            if (filterClassification) {
                List<AtlasGraphQuery> orConditions = new LinkedList<>();

                if (classificationType == MATCH_ALL_WILDCARD_CLASSIFICATION || classificationType == MATCH_ALL_CLASSIFIED) {
                    orConditions.add(query.createChildQuery().has(TRAIT_NAMES_PROPERTY_KEY, NOT_EQUAL, null));
                    orConditions.add(query.createChildQuery().has(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, NOT_EQUAL, null));
                } else if (classificationType == MATCH_ALL_NOT_CLASSIFIED) {
                    orConditions.add(query.createChildQuery().has(TRAIT_NAMES_PROPERTY_KEY, EQUAL, null)
                                                             .has(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, EQUAL, null));
                } else {
                    orConditions.add(query.createChildQuery().in(TRAIT_NAMES_PROPERTY_KEY, classificationTypeAndSubTypes));
                    orConditions.add(query.createChildQuery().in(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationTypeAndSubTypes));
                }

                query.or(orConditions);

                // Construct a parallel in-memory predicate
                if (graphQueryPredicate != null) {
                    graphQueryPredicate = PredicateUtils.andPredicate(graphQueryPredicate, traitPredicate);
                } else {
                    graphQueryPredicate = traitPredicate;
                }
            }

            graphQuery = toGraphFilterQuery(entityType, filterCriteria, graphAttributes, query);

            // Prepare in-memory predicate for attribute filtering
            Predicate attributePredicate = constructInMemoryPredicate(entityType, filterCriteria, graphAttributes);

            if (attributePredicate != null) {
                if (graphQueryPredicate != null) {
                    graphQueryPredicate = PredicateUtils.andPredicate(graphQueryPredicate, attributePredicate);
                } else {
                    graphQueryPredicate = attributePredicate;
                }
            }

            // Filter condition for the STATUS
            if (context.getSearchParameters().getExcludeDeletedEntities() && this.indexQuery == null) {
                graphQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
                if (graphQueryPredicate != null) {
                    graphQueryPredicate = PredicateUtils.andPredicate(graphQueryPredicate, activePredicate);
                } else {
                    graphQueryPredicate = activePredicate;
                }
            }
        } else {
            graphQuery = null;
            graphQueryPredicate = null;
        }


        // Prepare the graph query and in-memory filter for the filtering phase
        filterGraphQueryPredicate = typeNamePredicate;

        Predicate attributesPredicate = constructInMemoryPredicate(entityType, filterCriteria, allAttributes);

        if (attributesPredicate != null) {
            filterGraphQueryPredicate = PredicateUtils.andPredicate(filterGraphQueryPredicate, attributesPredicate);
        }

        if (filterClassification) {
            filterGraphQueryPredicate = PredicateUtils.andPredicate(filterGraphQueryPredicate, traitPredicate);
        }

        // Filter condition for the STATUS
        if (context.getSearchParameters().getExcludeDeletedEntities()) {
            filterGraphQueryPredicate = PredicateUtils.andPredicate(filterGraphQueryPredicate, activePredicate);
        }
    }

    @Override
    public List<AtlasVertex> execute() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> EntitySearchProcessor.execute({})", context);
        }

        List<AtlasVertex> ret = new ArrayList<>();

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntitySearchProcessor.execute(" + context +  ")");
        }

        try {
            final int startIdx = context.getSearchParameters().getOffset();
            final int limit    = context.getSearchParameters().getLimit();

            // when subsequent filtering stages are involved, query should start at 0 even though startIdx can be higher
            //
            // first 'startIdx' number of entries will be ignored
            int qryOffset = (nextProcessor != null || (graphQuery != null && indexQuery != null)) ? 0 : startIdx;
            int resultIdx = qryOffset;

            final List<AtlasVertex> entityVertices = new ArrayList<>();

            for (; ret.size() < limit; qryOffset += limit) {
                entityVertices.clear();

                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                final boolean isLastResultPage;

                if (indexQuery != null) {
                    Iterator<AtlasIndexQuery.Result> idxQueryResult = indexQuery.vertices(qryOffset, limit);

                    getVerticesFromIndexQueryResult(idxQueryResult, entityVertices);

                    isLastResultPage = entityVertices.size() < limit;

                    // Do in-memory filtering before the graph query
                    CollectionUtils.filter(entityVertices, inMemoryPredicate);

                    if (graphQueryPredicate != null) {
                        CollectionUtils.filter(entityVertices, graphQueryPredicate);
                    }
                } else {
                    Iterator<AtlasVertex> queryResult = graphQuery.vertices(qryOffset, limit).iterator();

                    getVertices(queryResult, entityVertices);

                    isLastResultPage = entityVertices.size() < limit;
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
            LOG.debug("<== EntitySearchProcessor.execute({}): ret.size()={}", context, ret.size());
        }

        return ret;
    }

    @Override
    public void filter(List<AtlasVertex> entityVertices) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> EntitySearchProcessor.filter({})", entityVertices.size());
        }

        // Since we already have the entity vertices, a in-memory filter will be faster than fetching the same
        // vertices again with the required filtering
        if (filterGraphQueryPredicate != null) {
            LOG.debug("Filtering in-memory");
            CollectionUtils.filter(entityVertices, filterGraphQueryPredicate);
        }

        super.filter(entityVertices);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== EntitySearchProcessor.filter(): ret.size()={}", entityVertices.size());
        }
    }
}
