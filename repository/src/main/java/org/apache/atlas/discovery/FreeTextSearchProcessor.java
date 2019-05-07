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
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfTracer;
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
    public  static final String SOLR_QT_PARAMETER           = "qt";
    public  static final String SOLR_REQUEST_HANDLER_NAME   = "/freetext";
    private static final int    MAX_TYPES_STRING_SIZE       = 1000;

    private final AtlasIndexQuery indexQuery;

    public FreeTextSearchProcessor(SearchContext context) {
        super(context);

        SearchParameters searchParameters = context.getSearchParameters();
        StringBuilder    queryString      = new StringBuilder();

        queryString.append(searchParameters.getQuery());

        String queryFields = null;
        // if search includes entity-type criteria, adding a filter here can help avoid unnecessary
        // processing (and rejection) by subsequent EntitySearchProcessor
        if (context.getEntityType() != null) {
            String typeString = context.getEntityType().getTypeAndAllSubTypesQryStr();
            if (typeString.length() > MAX_TYPES_STRING_SIZE) {
                LOG.info("Dropping the use of types string optimization as there are too many types {} for select type {}.", typeString, context.getEntityType().getTypeName());
            } else {
                LOG.debug("Using the use of types string optimization as there are too many types {} for select type {}.", typeString, context.getEntityType().getTypeName());

                final Set<String> types = context.getEntityType().getTypeAndAllSubTypes();
                final AtlasGraphManagement managementSystem = context.getGraph().getManagementSystem();
                AtlasPropertyKey entityTypeNamePropertyKey = managementSystem.getPropertyKey(AtlasGraphUtilsV2.encodePropertyKey(Constants.ENTITY_TYPE_PROPERTY_KEY));
                String encodedPropertyName = managementSystem.getIndexFieldName(Constants.VERTEX_INDEX, entityTypeNamePropertyKey);



                StringBuilder typesStringBuilder = new StringBuilder();
                for(String typeName: types) {
                    typesStringBuilder.append(" ").append(typeName);
                }
                //append the query with type and substypes listed in it
                String typesString = typesStringBuilder.toString();
                queryString.append(" AND +").append(encodedPropertyName).append(":[");
                queryString.append(typesStringBuilder.toString());
                queryString.append("]");
            }
        }

        //just use the query string as is
        LOG.debug("Using query string  '{}'.", queryString);
        indexQuery = context.getGraph().indexQuery(prepareGraphIndexQueryParameters(context, queryString));
    }

    private GraphIndexQueryParameters prepareGraphIndexQueryParameters(SearchContext context, StringBuilder queryString) {
        List<AtlasIndexQueryParameter> parameters = new ArrayList<AtlasIndexQueryParameter>();
        parameters.add(context.getGraph().indexQueryParameter(SOLR_QT_PARAMETER, SOLR_REQUEST_HANDLER_NAME));
        return new GraphIndexQueryParameters(Constants.VERTEX_INDEX, queryString.toString(), 0, parameters);
    }

    @Override
    public List<AtlasVertex> execute() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> FullTextSearchProcessorUsingFreeText.execute({})", context);
        }

        List<AtlasVertex> ret = new ArrayList<>();

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FullTextSearchProcessorUsingFreeText.execute(" + context +  ")");
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
            try {
                for (; ret.size() < limit; qryOffset += limit) {
                    entityVertices.clear();

                    if (context.terminateSearch()) {
                        LOG.warn("query terminated: {}", context.getSearchParameters());

                        break;
                    }

                    Iterator<AtlasIndexQuery.Result> idxQueryResult = indexQuery.vertices(qryOffset, limit);

                    final boolean isLastResultPage;
                    int resultCount = 0;

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
            } catch (Throwable t) {
                throw t;
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
