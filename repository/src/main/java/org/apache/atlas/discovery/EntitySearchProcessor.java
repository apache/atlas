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
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EntitySearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(EntitySearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("EntitySearchProcessor");

    private final AtlasIndexQuery indexQuery;
    private final AtlasGraphQuery graphQuery;
    private final AtlasGraphQuery filterGraphQuery;

    public EntitySearchProcessor(SearchContext context) {
        super(context);

        final AtlasEntityType entityType            = context.getEntityType();
        final FilterCriteria  filterCriteria        = context.getSearchParameters().getEntityFilters();
        final Set<String>     typeAndSubTypes       = entityType.getTypeAndAllSubTypes();
        final String          typeAndSubTypesQryStr = entityType.getTypeAndAllSubTypesQryStr();
        final Set<String>     indexAttributes       = new HashSet<>();
        final Set<String>     graphAttributes       = new HashSet<>();
        final Set<String>     allAttributes         = new HashSet<>();

        final AtlasClassificationType classificationType   = context.getClassificationType();
        final boolean                 filterClassification = classificationType != null && !context.needClassificationProcessor();


        processSearchAttributes(entityType, filterCriteria, indexAttributes, graphAttributes, allAttributes);

        final boolean typeSearchByIndex = !filterClassification && typeAndSubTypesQryStr.length() <= MAX_QUERY_STR_LENGTH_TYPES;
        final boolean attrSearchByIndex = !filterClassification && CollectionUtils.isNotEmpty(indexAttributes) && canApplyIndexFilter(entityType, filterCriteria, false);

        StringBuilder indexQuery = new StringBuilder();

        if (typeSearchByIndex) {
            constructTypeTestQuery(indexQuery, typeAndSubTypesQryStr);
        }

        if (attrSearchByIndex) {
            constructFilterQuery(indexQuery, entityType, filterCriteria, indexAttributes);

            constructInMemoryPredicate(entityType, filterCriteria, indexAttributes);
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

            if (filterClassification) {
                query.in(Constants.TRAIT_NAMES_PROPERTY_KEY, classificationType.getTypeAndAllSubTypes());
            }

            graphQuery = toGraphFilterQuery(entityType, filterCriteria, graphAttributes, query);

            if (context.getSearchParameters().getExcludeDeletedEntities() && this.indexQuery == null) {
                graphQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
            }
        } else {
            graphQuery = null;
        }

        AtlasGraphQuery query = context.getGraph().query().in(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes);

        if (filterClassification) {
            query.in(Constants.TRAIT_NAMES_PROPERTY_KEY, classificationType.getTypeAndAllSubTypes());
        }

        filterGraphQuery = toGraphFilterQuery(entityType, filterCriteria, allAttributes, query);

        if (context.getSearchParameters().getExcludeDeletedEntities()) {
            filterGraphQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
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

                if (indexQuery != null) {
                    Iterator<AtlasIndexQuery.Result> idxQueryResult = indexQuery.vertices(qryOffset, limit);

                    if (!idxQueryResult.hasNext()) { // no more results from index query - end of search
                        break;
                    }

                    getVerticesFromIndexQueryResult(idxQueryResult, entityVertices);

                    // Do in-memory filtering before the graph query
                    CollectionUtils.filter(entityVertices, inMemoryPredicate);

                    if (graphQuery != null) {
                        Set<String> guids = getGuids(entityVertices);

                        entityVertices.clear();

                        if (CollectionUtils.isNotEmpty(guids)) {
                            AtlasGraphQuery guidQuery = context.getGraph().query().in(Constants.GUID_PROPERTY_KEY, guids);

                            guidQuery.addConditionsFrom(graphQuery);

                            getVertices(guidQuery.vertices().iterator(), entityVertices);
                        }
                    }
                } else {
                    Iterator<AtlasVertex> queryResult = graphQuery.vertices(qryOffset, limit).iterator();

                    if (!queryResult.hasNext()) { // no more results from query - end of search
                        break;
                    }

                    getVertices(queryResult, entityVertices);
                }

                super.filter(entityVertices);

                resultIdx = collectResultVertices(ret, startIdx, limit, resultIdx, entityVertices);
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

        Set<String> guids = getGuids(entityVertices);

        entityVertices.clear();

        if (CollectionUtils.isNotEmpty(guids)) {
            AtlasGraphQuery query = context.getGraph().query().in(Constants.GUID_PROPERTY_KEY, guids);

            query.addConditionsFrom(filterGraphQuery);

            getVertices(query.vertices().iterator(), entityVertices);
        }

        super.filter(entityVertices);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== EntitySearchProcessor.filter(): ret.size()={}", entityVertices.size());
        }
    }
}
