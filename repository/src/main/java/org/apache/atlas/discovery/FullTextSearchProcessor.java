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
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class FullTextSearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(FullTextSearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("FullTextSearchProcessor");

    private final AtlasIndexQuery indexQuery;

    public FullTextSearchProcessor(SearchContext context) {
        super(context);

        SearchParameters searchParameters = context.getSearchParameters();
        StringBuilder    queryString      = new StringBuilder();

        queryString.append("v.\"").append(Constants.ENTITY_TEXT_PROPERTY_KEY).append("\":(").append(searchParameters.getQuery());

        // if search includes entity-type criteria, adding a filter here can help avoid unnecessary
        // processing (and rejection) by subsequent EntitySearchProcessor
        if (context.getEntityType() != null) {
            Set<String> typeAndSubTypeNames = context.getEntityType().getTypeAndAllSubTypes();

            if (typeAndSubTypeNames.size() <= MAX_ENTITY_TYPES_IN_INDEX_QUERY) {
                queryString.append(AND_STR).append("(").append(StringUtils.join(typeAndSubTypeNames, SPACE_STRING)).append(")");
            } else {
                LOG.warn("'{}' has too many subtypes ({}) to include in index-query; might cause poor performance",
                         context.getEntityType().getTypeName(), typeAndSubTypeNames.size());
            }
        }

        // if search includes classification criteria, adding a filter here can help avoid unnecessary
        // processing (and rejection) by subsequent ClassificationSearchProcessor or EntitySearchProcessor
        if (context.getClassificationType() != null) {
            Set<String> typeAndSubTypeNames = context.getClassificationType().getTypeAndAllSubTypes();

            if (typeAndSubTypeNames.size() <= MAX_CLASSIFICATION_TYPES_IN_INDEX_QUERY) {
                queryString.append(AND_STR).append("(").append(StringUtils.join(typeAndSubTypeNames, SPACE_STRING)).append(")");
            } else {
                LOG.warn("'{}' has too many subtypes ({}) to include in index-query; might cause poor performance",
                        context.getClassificationType().getTypeName(), typeAndSubTypeNames.size());
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
            final int startIdx  = context.getSearchParameters().getOffset();
            final int limit     = context.getSearchParameters().getLimit();
            int       qryOffset = nextProcessor == null ? startIdx : 0;
            int       resultIdx = qryOffset;

            final List<AtlasVertex> entityVertices = new ArrayList<>();

            for (; ret.size() < limit; qryOffset += limit) {
                entityVertices.clear();

                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                Iterator<AtlasIndexQuery.Result> idxQueryResult = indexQuery.vertices(qryOffset, limit);

                if (!idxQueryResult.hasNext()) { // no more results from solr - end of search
                    break;
                }

                while (idxQueryResult.hasNext()) {
                    AtlasVertex vertex = idxQueryResult.next().getVertex();

                    // skip non-entity vertices
                    if (!AtlasGraphUtilsV1.isEntityVertex(vertex)) {
                        LOG.warn("FullTextSearchProcessor.execute(): ignoring non-entity vertex (id={})", vertex.getId()); // might cause duplicate entries in result

                        continue;
                    }

                    entityVertices.add(vertex);
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
            LOG.debug("<== FullTextSearchProcessor.execute({}): ret.size()={}", context, ret.size());
        }

        return ret;
    }
}
