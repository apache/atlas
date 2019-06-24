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

import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_CLASSIFIED;
import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_NOT_CLASSIFIED;
import static org.apache.atlas.discovery.SearchContext.MATCH_ALL_WILDCARD_CLASSIFICATION;


public class FullTextSearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(FullTextSearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("FullTextSearchProcessor");

    private final AtlasIndexQuery indexQuery;

    public FullTextSearchProcessor(SearchContext context) {
        super(context);

        SearchParameters searchParameters = context.getSearchParameters();
        StringBuilder    queryString      = new StringBuilder();

        queryString.append(INDEX_SEARCH_PREFIX + "\"").append(Constants.ENTITY_TEXT_PROPERTY_KEY).append("\":(").append(searchParameters.getQuery());

        // if search includes entity-type criteria, adding a filter here can help avoid unnecessary
        // processing (and rejection) by subsequent EntitySearchProcessor
        if (context.getEntityType() != null) {
            String typeAndSubTypeNamesQryStr = context.getEntityType().getTypeAndAllSubTypesQryStr();

            if (typeAndSubTypeNamesQryStr.length() <= MAX_QUERY_STR_LENGTH_TYPES) {
                queryString.append(AND_STR).append(typeAndSubTypeNamesQryStr);
            } else {
                LOG.warn("'{}' has too many subtypes (query-string-length={}) to include in index-query; might cause poor performance",
                         context.getEntityType().getTypeName(), typeAndSubTypeNamesQryStr.length());
            }
        }

        // if search includes classification criteria, adding a filter here can help avoid unnecessary
        // processing (and rejection) by subsequent ClassificationSearchProcessor or EntitySearchProcessor
        if (context.getClassificationType() != null && context.getClassificationType() != MATCH_ALL_WILDCARD_CLASSIFICATION &&
                                                       context.getClassificationType() != MATCH_ALL_CLASSIFIED &&
                                                       context.getClassificationType() != MATCH_ALL_NOT_CLASSIFIED) {
            String typeAndSubTypeNamesStr = context.getClassificationType().getTypeAndAllSubTypesQryStr();

            if (typeAndSubTypeNamesStr.length() <= MAX_QUERY_STR_LENGTH_TAGS) {
                queryString.append(AND_STR).append(typeAndSubTypeNamesStr);
            } else {
                LOG.warn("'{}' has too many subtypes (query-string-length={}) to include in index-query; might cause poor performance",
                        context.getClassificationType().getTypeName(), typeAndSubTypeNamesStr.length());
            }
        }

        queryString.append(")");

        indexQuery = context.getGraph().indexQuery(Constants.FULLTEXT_INDEX, queryString.toString());
    }

    @Override
    public List<AtlasVertex> execute() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> FullTextSearchProcessor.execute({})", context);
        }

        List<AtlasVertex> ret = new ArrayList<>();

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FullTextSearchProcessor.execute(" + context +  ")");
        }

        try {
            final int     startIdx   = context.getSearchParameters().getOffset();
            final int     limit      = context.getSearchParameters().getLimit();
            final boolean activeOnly = context.getSearchParameters().getExcludeDeletedEntities();

            // query to start at 0, even though startIdx can be higher - because few results in earlier retrieval could
            // have been dropped: like vertices of non-entity or non-active-entity
            //
            // first 'startIdx' number of entries will be ignored
            int qryOffset = 0;
            int resultIdx = qryOffset;

            final List<AtlasVertex> entityVertices = new ArrayList<>();

            for (; ret.size() < limit; qryOffset += limit) {
                entityVertices.clear();

                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                Iterator<AtlasIndexQuery.Result> idxQueryResult = indexQuery.vertices(qryOffset, limit);

                final boolean isLastResultPage;
                int           resultCount = 0;

                while (idxQueryResult.hasNext()) {
                    AtlasVertex vertex = idxQueryResult.next().getVertex();

                    resultCount++;

                    // skip non-entity vertices
                    if (!AtlasGraphUtilsV2.isEntityVertex(vertex)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("FullTextSearchProcessor.execute(): ignoring non-entity vertex (id={})", vertex.getId());
                        }

                        continue;
                    }

                    if (activeOnly && AtlasGraphUtilsV2.getState(vertex) != AtlasEntity.Status.ACTIVE) {
                        continue;
                    }

                    entityVertices.add(vertex);
                }

                isLastResultPage = resultCount < limit;

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
            LOG.debug("<== FullTextSearchProcessor.execute({}): ret.size()={}", context, ret.size());
        }

        return ret;
    }
}
