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
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * This class is equivalent to legacy FullTextSearchProcessor--except that it uses a better search techniques using SOLR
 * than going through Janus Graph index apis.
 */
public class FreeTextSearchProcessor extends SearchProcessor {
    private static final Logger LOG                         = LoggerFactory.getLogger(FreeTextSearchProcessor.class);
    private static final Logger PERF_LOG                    = AtlasPerfTracer.getPerfLogger("FreeTextSearchProcessor");
    public  static final String SOLR_QT_PARAMETER           = CommonParams.QT;
    public  static final String SOLR_REQUEST_HANDLER_NAME   = "/freetext";

    private final AtlasIndexQuery indexQuery;

    public FreeTextSearchProcessor(SearchContext context) {
        super(context);

        SearchParameters searchParameters = context.getSearchParameters();
        StringBuilder    queryString      = new StringBuilder();

        queryString.append(searchParameters.getQuery());

        if (CollectionUtils.isNotEmpty(context.getEntityTypes()) && context.getEntityTypesQryStr().length() <= MAX_QUERY_STR_LENGTH_TYPES) {
            queryString.append(AND_STR).append(context.getEntityTypesQryStr());
        }

        graphIndexQueryBuilder.addActiveStateQueryFilter(queryString);

        if (CollectionUtils.isNotEmpty(context.getClassificationTypes()) && context.getClassificationTypesQryStr().length() <= MAX_QUERY_STR_LENGTH_TYPES) {
            queryString.append(AND_STR).append(context.getClassificationTypesQryStr());
        }

        // just use the query string as is
        LOG.debug("Using query string  '{}'.", queryString);

        indexQuery = context.getGraph().indexQuery(prepareGraphIndexQueryParameters(context, queryString));
    }

    private GraphIndexQueryParameters prepareGraphIndexQueryParameters(SearchContext context, StringBuilder queryString) {
        List<AtlasIndexQueryParameter> parameters = new ArrayList<>();

        parameters.add(context.getGraph().indexQueryParameter(SOLR_QT_PARAMETER, SOLR_REQUEST_HANDLER_NAME));

        return new GraphIndexQueryParameters(Constants.VERTEX_INDEX, queryString.toString(), 0, parameters);
    }

    @Override
    public List<AtlasVertex> execute() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> FreeTextSearchProcessor.execute({})", context);
        }

        List<AtlasVertex> ret = new ArrayList<>();

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FreeTextSearchProcessor.execute(" + context +  ")");
        }

        try {
            final int startIdx = context.getSearchParameters().getOffset();
            final int limit    = context.getSearchParameters().getLimit();

            // query to start at 0, even though startIdx can be higher - because few results in earlier retrieval could
            // have been dropped: like vertices of non-entity or non-active-entity
            //
            // first 'startIdx' number of entries will be ignored
            int qryOffset = 0;
            int resultIdx = qryOffset;

            final List<AtlasVertex> entityVertices = new ArrayList<>();
            try {
                for (; ret.size() < limit; qryOffset += limit) {
                    entityVertices.clear();

                    if (context.terminateSearch()) {
                        LOG.warn("query terminated: {}", context.getSearchParameters());

                        break;
                    }

                    Iterator<AtlasIndexQuery.Result> idxQueryResult = executeIndexQuery(context, indexQuery, qryOffset, limit);

                    final boolean isLastResultPage;
                    int resultCount = 0;

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

                        if (!context.includeEntityType(entityTypeName)) {
                            continue;
                        }

                        if (context.getClassificationType() != null) {
                            List<String> entityClassifications = GraphHelper.getAllTraitNames(vertex);

                            if (!context.includeClassificationTypes(entityClassifications)) {
                                continue;
                            }
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
            } catch (Throwable t) {
                throw t;
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== FreeTextSearchProcessor.execute({}): ret.size()={}", context, ret.size());
        }

        return ret;
    }

    @Override
    public long getResultCount() {
        return indexQuery.vertexTotals();
    }
}
