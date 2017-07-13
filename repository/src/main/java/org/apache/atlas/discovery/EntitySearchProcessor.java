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
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EntitySearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(EntitySearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("EntitySearchProcessor");

    private final AtlasIndexQuery indexQuery;
    private final AtlasGraphQuery partialGraphQuery;
    private final AtlasGraphQuery allGraphQuery;

    public EntitySearchProcessor(SearchContext context) {
        super(context);

        AtlasEntityType         entityType         = context.getEntityType();
        AtlasClassificationType classificationType = context.getClassificationType();
        FilterCriteria          filterCriteria     = context.getSearchParameters().getEntityFilters();
        Set<String>             typeAndSubTypes    = entityType.getTypeAndAllSubTypes();
        Set<String>             solrAttributes     = new HashSet<>();
        Set<String>             gremlinAttributes  = new HashSet<>();
        Set<String>             allAttributes      = new HashSet<>();


        processSearchAttributes(entityType, filterCriteria, solrAttributes, gremlinAttributes, allAttributes);

        boolean useSolrSearch = typeAndSubTypes.size() <= MAX_ENTITY_TYPES_IN_INDEX_QUERY && canApplySolrFilter(entityType, filterCriteria, false);

        if (useSolrSearch) {
            StringBuilder solrQuery = new StringBuilder();

            constructTypeTestQuery(solrQuery, typeAndSubTypes);
            constructFilterQuery(solrQuery, entityType, filterCriteria, solrAttributes);

            String solrQueryString = STRAY_AND_PATTERN.matcher(solrQuery).replaceAll(")");

            solrQueryString = STRAY_OR_PATTERN.matcher(solrQueryString).replaceAll(")");
            solrQueryString = STRAY_ELIPSIS_PATTERN.matcher(solrQueryString).replaceAll("");

            indexQuery = context.getGraph().indexQuery(Constants.VERTEX_INDEX, solrQueryString);

            if (CollectionUtils.isNotEmpty(gremlinAttributes) || classificationType != null) {
                AtlasGraphQuery query = context.getGraph().query();

                addClassificationNameConditionIfNecessary(query);

                partialGraphQuery = toGremlinFilterQuery(entityType, filterCriteria, gremlinAttributes, query);
            } else {
                partialGraphQuery = null;
            }
        } else {
            indexQuery      = null;
            partialGraphQuery = null;
        }

        AtlasGraphQuery query = context.getGraph().query().in(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes);

        addClassificationNameConditionIfNecessary(query);

        allGraphQuery = toGremlinFilterQuery(entityType, filterCriteria, allAttributes, query);

        if (context.getSearchParameters().getExcludeDeletedEntities()) {
            allGraphQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
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
            int qryOffset = (nextProcessor == null) ? context.getSearchParameters().getOffset() : 0;
            int limit     = context.getSearchParameters().getLimit();
            int resultIdx = qryOffset;

            while (ret.size() < limit) {
                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                List<AtlasVertex> vertices;

                if (indexQuery != null) {
                    Iterator<AtlasIndexQuery.Result> queryResult = indexQuery.vertices(qryOffset, limit);

                    if (!queryResult.hasNext()) { // no more results from solr - end of search
                        break;
                    }

                    vertices = getVerticesFromIndexQueryResult(queryResult);

                    if (partialGraphQuery != null) {
                        AtlasGraphQuery guidQuery = context.getGraph().query().in(Constants.GUID_PROPERTY_KEY, getGuids(vertices));

                        guidQuery.addConditionsFrom(partialGraphQuery);

                        vertices = getVertices(guidQuery.vertices().iterator());
                    }
                } else {
                    Iterator<AtlasVertex> queryResult = allGraphQuery.vertices(qryOffset, limit).iterator();

                    if (!queryResult.hasNext()) { // no more results from query - end of search
                        break;
                    }

                    vertices = getVertices(queryResult);
                }

                qryOffset += limit;

                vertices = super.filter(vertices);

                for (AtlasVertex vertex : vertices) {
                    resultIdx++;

                    if (resultIdx < context.getSearchParameters().getOffset()) {
                        continue;
                    }

                    ret.add(vertex);

                    if (ret.size() == limit) {
                        break;
                    }
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
    public List<AtlasVertex> filter(List<AtlasVertex> entityVertices) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> EntitySearchProcessor.filter({})", entityVertices.size());
        }

        AtlasGraphQuery query = context.getGraph().query().in(Constants.GUID_PROPERTY_KEY, getGuids(entityVertices));

        query.addConditionsFrom(allGraphQuery);

        List<AtlasVertex> ret = getVertices(query.vertices().iterator());

        ret = super.filter(ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== EntitySearchProcessor.filter({}): ret.size()={}", entityVertices.size(), ret.size());
        }

        return ret;
    }

    private void addClassificationNameConditionIfNecessary(AtlasGraphQuery query) {
        if (context.getClassificationType() != null && !context.needClassificationProcessor()) {
            query.in(Constants.TRAIT_NAMES_PROPERTY_KEY, context.getClassificationType().getTypeAndAllSubTypes());
        }
    }
}
