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
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
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
    private final AtlasGraphQuery graphQuery;
    private final AtlasGraphQuery filterGraphQuery;

    public EntitySearchProcessor(SearchContext context) {
        super(context);

        final AtlasEntityType entityType        = context.getEntityType();
        final FilterCriteria  filterCriteria    = context.getSearchParameters().getEntityFilters();
        final Set<String>     typeAndSubTypes   = entityType.getTypeAndAllSubTypes();
        final Set<String>     solrAttributes    = new HashSet<>();
        final Set<String>     gremlinAttributes = new HashSet<>();
        final Set<String>     allAttributes     = new HashSet<>();

        final AtlasClassificationType classificationType   = context.getClassificationType();
        final boolean                 filterClassification = classificationType != null && !context.needClassificationProcessor();


        processSearchAttributes(entityType, filterCriteria, solrAttributes, gremlinAttributes, allAttributes);

        final boolean typeSearchBySolr = !filterClassification && typeAndSubTypes.size() <= MAX_ENTITY_TYPES_IN_INDEX_QUERY;
        final boolean attrSearchBySolr = !filterClassification && CollectionUtils.isNotEmpty(solrAttributes) && canApplySolrFilter(entityType, filterCriteria, false);

        StringBuilder solrQuery = new StringBuilder();

        if (typeSearchBySolr) {
            constructTypeTestQuery(solrQuery, entityType, typeAndSubTypes);
        }

        if (attrSearchBySolr) {
            constructFilterQuery(solrQuery, entityType, filterCriteria, solrAttributes);
        } else {
            gremlinAttributes.addAll(solrAttributes);
        }

        if (solrQuery.length() > 0) {
            String solrQueryString = STRAY_AND_PATTERN.matcher(solrQuery).replaceAll(")");

            solrQueryString = STRAY_OR_PATTERN.matcher(solrQueryString).replaceAll(")");
            solrQueryString = STRAY_ELIPSIS_PATTERN.matcher(solrQueryString).replaceAll("");

            indexQuery = context.getGraph().indexQuery(Constants.VERTEX_INDEX, solrQueryString);
        } else {
            indexQuery = null;
        }

        if (CollectionUtils.isNotEmpty(gremlinAttributes) || !typeSearchBySolr) {
            AtlasGraphQuery query = context.getGraph().query();

            if (!typeSearchBySolr) {
                query.in(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes);
            }

            if (filterClassification) {
                query.in(Constants.TRAIT_NAMES_PROPERTY_KEY, classificationType.getTypeAndAllSubTypes());
            }

            graphQuery = toGremlinFilterQuery(entityType, filterCriteria, gremlinAttributes, query);

            if (context.getSearchParameters().getExcludeDeletedEntities() && indexQuery == null) {
                graphQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
            }
        } else {
            graphQuery = null;
        }

        AtlasGraphQuery query = context.getGraph().query().in(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes);

        if (filterClassification) {
            query.in(Constants.TRAIT_NAMES_PROPERTY_KEY, classificationType.getTypeAndAllSubTypes());
        }

        filterGraphQuery = toGremlinFilterQuery(entityType, filterCriteria, allAttributes, query);

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
            final int startIdx  = context.getSearchParameters().getOffset();
            final int limit     = context.getSearchParameters().getLimit();
            int       qryOffset = (nextProcessor == null && (graphQuery == null || indexQuery == null)) ? startIdx : 0;
            int       resultIdx = qryOffset;

            final List<AtlasVertex> entityVertices = new ArrayList<>();

            for (; ret.size() < limit; qryOffset += limit) {
                entityVertices.clear();

                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                if (indexQuery != null) {
                    Iterator<AtlasIndexQuery.Result> idxQueryResult = indexQuery.vertices(qryOffset, limit);

                    if (!idxQueryResult.hasNext()) { // no more results from solr - end of search
                        break;
                    }

                    while (idxQueryResult.hasNext()) {
                        AtlasVertex vertex = idxQueryResult.next().getVertex();

                        // skip non-entity vertices
                        if (!AtlasGraphUtilsV1.isEntityVertex(vertex)) {
                            LOG.warn("EntitySearchProcessor.execute(): ignoring non-entity vertex (id={})", vertex.getId()); // might cause duplicate entries in result

                            continue;
                        }

                        entityVertices.add(vertex);
                    }

                    if (graphQuery != null) {
                        AtlasGraphQuery guidQuery = context.getGraph().query().in(Constants.GUID_PROPERTY_KEY, getGuids(entityVertices));

                        guidQuery.addConditionsFrom(graphQuery);

                        getVertices(guidQuery.vertices().iterator(), entityVertices);
                    }
                } else {
                    Iterator<AtlasVertex> queryResult = graphQuery.vertices(qryOffset, limit).iterator();

                    if (!queryResult.hasNext()) { // no more results from query - end of search
                        break;
                    }

                    getVertices(queryResult, entityVertices);
                }

                super.filter(entityVertices);

                for (AtlasVertex entityVertex : entityVertices) {
                    resultIdx++;

                    if (resultIdx <= startIdx) {
                        continue;
                    }

                    ret.add(entityVertex);

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
    public void filter(List<AtlasVertex> entityVertices) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> EntitySearchProcessor.filter({})", entityVertices.size());
        }

        AtlasGraphQuery query = context.getGraph().query().in(Constants.GUID_PROPERTY_KEY, getGuids(entityVertices));

        query.addConditionsFrom(filterGraphQuery);

        entityVertices.clear();
        getVertices(query.vertices().iterator(), entityVertices);

        super.filter(entityVertices);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== EntitySearchProcessor.filter(): ret.size()={}", entityVertices.size());
        }
    }
}
