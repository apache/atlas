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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class ClassificationSearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(ClassificationSearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("ClassificationSearchProcessor");

    private final AtlasIndexQuery indexQuery;
    private final AtlasGraphQuery allGraphQuery;
    private final AtlasGraphQuery filterGraphQuery;

    public ClassificationSearchProcessor(SearchContext context) {
        super(context);

        final AtlasClassificationType classificationType    = context.getClassificationType();
        final FilterCriteria          filterCriteria        = context.getSearchParameters().getTagFilters();
        final Set<String>             typeAndSubTypes       = classificationType.getTypeAndAllSubTypes();
        final String                  typeAndSubTypesQryStr = classificationType.getTypeAndAllSubTypesQryStr();
        final Set<String>             solrAttributes        = new HashSet<>();
        final Set<String>             gremlinAttributes     = new HashSet<>();
        final Set<String>             allAttributes         = new HashSet<>();


        processSearchAttributes(classificationType, filterCriteria, solrAttributes, gremlinAttributes, allAttributes);

        // for classification search, if any attribute can't be handled by Solr - switch to all Gremlin
        boolean useSolrSearch = typeAndSubTypesQryStr.length() <= MAX_QUERY_STR_LENGTH_TAGS && CollectionUtils.isEmpty(gremlinAttributes) && canApplySolrFilter(classificationType, filterCriteria, false);

        if (useSolrSearch) {
            StringBuilder solrQuery = new StringBuilder();

            constructTypeTestQuery(solrQuery, typeAndSubTypesQryStr);
            constructFilterQuery(solrQuery, classificationType, filterCriteria, solrAttributes);

            String solrQueryString = STRAY_AND_PATTERN.matcher(solrQuery).replaceAll(")");

            solrQueryString = STRAY_OR_PATTERN.matcher(solrQueryString).replaceAll(")");
            solrQueryString = STRAY_ELIPSIS_PATTERN.matcher(solrQueryString).replaceAll("");

            indexQuery = context.getGraph().indexQuery(Constants.VERTEX_INDEX, solrQueryString);
        } else {
            indexQuery = null;
        }

        AtlasGraphQuery query = context.getGraph().query().in(Constants.TYPE_NAME_PROPERTY_KEY, typeAndSubTypes);

        allGraphQuery = toGremlinFilterQuery(classificationType, filterCriteria, allAttributes, query);

        query = context.getGraph().query().in(Constants.TRAIT_NAMES_PROPERTY_KEY, typeAndSubTypes);

        filterGraphQuery = query; // TODO: filer based on tag attributes
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

                if (indexQuery != null) {
                    Iterator<AtlasIndexQuery.Result> queryResult = indexQuery.vertices(qryOffset, limit);

                    if (!queryResult.hasNext()) { // no more results from solr - end of search
                        break;
                    }

                    getVerticesFromIndexQueryResult(queryResult, classificationVertices);
                } else {
                    Iterator<AtlasVertex> queryResult = allGraphQuery.vertices(qryOffset, limit).iterator();

                    if (!queryResult.hasNext()) { // no more results - end of search
                        break;
                    }

                    getVertices(queryResult, classificationVertices);
                }

                for (AtlasVertex classificationVertex : classificationVertices) {
                    Iterable<AtlasEdge> edges = classificationVertex.getEdges(AtlasEdgeDirection.IN);

                    for (AtlasEdge edge : edges) {
                        AtlasVertex entityVertex = edge.getOutVertex();

                        if (activeOnly && AtlasGraphUtilsV1.getState(entityVertex) != AtlasEntity.Status.ACTIVE) {
                            continue;
                        }

                        String guid = AtlasGraphUtilsV1.getIdFromVertex(entityVertex);

                        if (processedGuids.contains(guid)) {
                            continue;
                        }

                        entityVertices.add(entityVertex);

                        processedGuids.add(guid);
                    }
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
            LOG.debug("<== ClassificationSearchProcessor.execute({}): ret.size()={}", context, ret.size());
        }

        return ret;
    }

    @Override
    public void filter(List<AtlasVertex> entityVertices) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ClassificationSearchProcessor.filter({})", entityVertices.size());
        }

        AtlasGraphQuery query = context.getGraph().query().in(Constants.GUID_PROPERTY_KEY, getGuids(entityVertices));

        query.addConditionsFrom(filterGraphQuery);

        entityVertices.clear();
        getVertices(query.vertices().iterator(), entityVertices);

        super.filter(entityVertices);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ClassificationSearchProcessor.filter(): ret.size()={}", entityVertices.size());
        }
    }
}
