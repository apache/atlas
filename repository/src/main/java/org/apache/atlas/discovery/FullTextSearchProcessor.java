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
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class FullTextSearchProcessor extends SearchProcessor {
    private static final Logger LOG      = LoggerFactory.getLogger(FullTextSearchProcessor.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("FullTextSearchProcessor");

    private final AtlasIndexQuery indexQuery;

    public FullTextSearchProcessor(SearchContext context) {
        super(context);

        SearchParameters searchParameters = context.getSearchParameters();
        String           queryString      = String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, searchParameters.getQuery());

        indexQuery = context.getGraph().indexQuery(Constants.FULLTEXT_INDEX, queryString);
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
            int qryOffset = nextProcessor == null ? context.getSearchParameters().getOffset() : 0;
            int limit     = context.getSearchParameters().getLimit();
            int resultIdx = qryOffset;

            while (ret.size() < limit) {
                if (context.terminateSearch()) {
                    LOG.warn("query terminated: {}", context.getSearchParameters());

                    break;
                }

                Iterator<AtlasIndexQuery.Result> idxQueryResult = indexQuery.vertices(qryOffset, limit);

                if (!idxQueryResult.hasNext()) { // no more results from solr - end of search
                    break;
                }

                qryOffset += limit;

                List<AtlasVertex> vertices = getVerticesFromIndexQueryResult(idxQueryResult);

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
            LOG.debug("<== FullTextSearchProcessor.execute({}): ret.size()={}", context, ret.size());
        }

        return ret;
    }
}
