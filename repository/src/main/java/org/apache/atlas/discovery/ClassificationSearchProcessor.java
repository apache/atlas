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

        AtlasClassificationType classificationType = context.getClassificationType();
        FilterCriteria          filterCriteria     = context.getSearchParameters().getTagFilters();
        Set<String>             typeAndSubTypes    = classificationType.getTypeAndAllSubTypes();
        Set<String>             solrAttributes     = new HashSet<>();
        Set<String>             gremlinAttributes  = new HashSet<>();
        Set<String>             allAttributes      = new HashSet<>();


        processSearchAttributes(classificationType, filterCriteria, solrAttributes, gremlinAttributes, allAttributes);

        // for classification search, if any attribute can't be handled by Solr - switch to all Gremlin
        boolean useSolrSearch = typeAndSubTypes.size() <= MAX_CLASSIFICATION_TYPES_IN_INDEX_QUERY && CollectionUtils.isEmpty(gremlinAttributes) && canApplySolrFilter(classificationType, filterCriteria, false);

        if (useSolrSearch) {
            StringBuilder solrQuery = new StringBuilder();

            constructTypeTestQuery(solrQuery, typeAndSubTypes);
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
            int         qryOffset      = (nextProcessor == null) ? context.getSearchParameters().getOffset() : 0;
            int         limit          = context.getSearchParameters().getLimit();
            int         resultIdx      = qryOffset;
            Set<String> processedGuids = new HashSet<>();

            while (ret.size() < limit) {
                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                List<AtlasVertex> classificationVertices;

                if (indexQuery != null) {
                    Iterator<AtlasIndexQuery.Result> queryResult = indexQuery.vertices(qryOffset, limit);

                    if (!queryResult.hasNext()) { // no more results from solr - end of search
                        break;
                    }

                    classificationVertices = getVerticesFromIndexQueryResult(queryResult);
                } else {
                    Iterator<AtlasVertex> queryResult = allGraphQuery.vertices(qryOffset, limit).iterator();

                    if (!queryResult.hasNext()) { // no more results - end of search
                        break;
                    }

                    classificationVertices = getVertices(queryResult);
                }

                qryOffset += limit;

                List<AtlasVertex> entityVertices = new ArrayList<>();

                for (AtlasVertex classificationVertex : classificationVertices) {
                    Iterable<AtlasEdge> edges = classificationVertex.getEdges(AtlasEdgeDirection.IN);

                    for (AtlasEdge edge : edges) {
                        AtlasVertex entityVertex = edge.getOutVertex();
                        String      guid         = AtlasGraphUtilsV1.getIdFromVertex(entityVertex);

                        if (!processedGuids.contains(guid)) {
                            if (!context.getSearchParameters().getExcludeDeletedEntities() || AtlasGraphUtilsV1.getState(entityVertex) == AtlasEntity.Status.ACTIVE) {
                                entityVertices.add(entityVertex);
                            }

                            processedGuids.add(guid);
                        }
                    }
                }

                entityVertices = super.filter(entityVertices);

                for (AtlasVertex entityVertex : entityVertices) {
                    resultIdx++;

                    if (resultIdx < context.getSearchParameters().getOffset()) {
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
    public List<AtlasVertex> filter(List<AtlasVertex> entityVertices) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ClassificationSearchProcessor.filter({})", entityVertices.size());
        }

        AtlasGraphQuery query = context.getGraph().query().in(Constants.GUID_PROPERTY_KEY, getGuids(entityVertices));

        query.addConditionsFrom(filterGraphQuery);

        List<AtlasVertex> ret = getVertices(query.vertices().iterator());

        ret = super.filter(ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ClassificationSearchProcessor.filter({}): ret.size()={}", entityVertices.size(), ret.size());
        }

        return ret;
    }
}
