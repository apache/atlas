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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.AtlasSearchResult.AttributeSearchResult;
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.AtlasEntityHeaderWithAssociations;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.query.Expressions.AliasExpression;
import org.apache.atlas.query.Expressions.Expression;
import org.apache.atlas.query.Expressions.SelectExpression;
import org.apache.atlas.query.GremlinQuery;
import org.apache.atlas.query.GremlinTranslator;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.query.QueryParser;
import org.apache.atlas.query.QueryProcessor;
import org.apache.atlas.query.SelectExpressionHelper;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery.Result;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.util.Either;
import scala.util.parsing.combinator.Parsers.NoSuccess;

import javax.inject.Inject;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasErrorCode.DISCOVERY_QUERY_FAILED;

public class EntityDiscoveryService implements AtlasDiscoveryService {

    private final AtlasGraph graph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;
    private static final Logger LOG = LoggerFactory.getLogger(EntityDiscoveryService.class);

    private final static String PROPERTY_KEY_NAME        = AtlasBaseTypeDef.ATLAS_TYPE_ASSET + "." + AtlasClient.NAME;
    private final static String PROPERTY_KEY_DESCRIPTION = AtlasBaseTypeDef.ATLAS_TYPE_ASSET + "." + AtlasClient.DESCRIPTION;
    private final static String PROPERTY_KEY_OWNER       = AtlasBaseTypeDef.ATLAS_TYPE_ASSET + "." + AtlasClient.OWNER;

    @Inject
    EntityDiscoveryService(MetadataRepository metadataRepository) {
        this.graph = AtlasGraphProvider.getGraphInstance();
        this.graphPersistenceStrategy = new DefaultGraphPersistenceStrategy(metadataRepository);
    }

    @Override
    public AtlasSearchResult searchUsingDslQuery(String dslQuery, int limit, int offset) throws AtlasBaseException {
        AtlasSearchResult ret = new AtlasSearchResult(dslQuery, AtlasQueryType.DSL);
        GremlinQuery gremlinQuery = toGremlinQuery(dslQuery, limit, offset);

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Executing DSL query: {}", dslQuery);
            }

            Object result = graph.executeGremlinScript(gremlinQuery.queryStr(), false);

            if (result instanceof List && CollectionUtils.isNotEmpty((List)result)) {
                List   queryResult  = (List) result;
                Object firstElement = queryResult.get(0);

                if (firstElement instanceof AtlasVertex) {
                    for (Object element : queryResult) {
                        if (element instanceof AtlasVertex) {
                            ret.addEntity(toAtlasEntityHeaderwithAssociations((AtlasVertex)element));
                        } else {
                            LOG.warn("searchUsingDslQuery({}): expected an AtlasVertex; found unexpected entry in result {}", dslQuery, element);
                        }
                    }
                } else if (firstElement instanceof Map &&
                           (((Map)firstElement).containsKey("theInstance") || ((Map)firstElement).containsKey("theTrait"))) {
                    for (Object element : queryResult) {
                        if (element instanceof Map) {
                            Map map = (Map)element;

                            if (map.containsKey("theInstance")) {
                                Object value = map.get("theInstance");

                                if (value instanceof List && CollectionUtils.isNotEmpty((List)value)) {
                                    Object entry = ((List)value).get(0);

                                    if (entry instanceof AtlasVertex) {
                                        ret.addEntity(toAtlasEntityHeaderwithAssociations((AtlasVertex)entry));
                                    }
                                }
                            }
                        } else {
                            LOG.warn("searchUsingDslQuery({}): expected a trait result; found unexpected entry in result {}", dslQuery, element);
                        }
                    }
                } else if (gremlinQuery.hasSelectList()) {
                    ret.setAttributes(toAttributesResult(queryResult, gremlinQuery));
                }
            }

        } catch (ScriptException e) {
            throw new AtlasBaseException(DISCOVERY_QUERY_FAILED, gremlinQuery.queryStr());
        }

        return ret;
    }

    @Override
    public AtlasSearchResult searchUsingFullTextQuery(String fullTextQuery, int limit, int offset) {
        AtlasSearchResult ret      = new AtlasSearchResult(fullTextQuery, AtlasQueryType.FULL_TEXT);
        QueryParams       params   = validateSearchParams(limit, offset);
        AtlasIndexQuery   idxQuery = toAtlasIndexQuery(fullTextQuery);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing Full text query: {}", fullTextQuery);
        }
        ret.setFullTextResult(getIndexQueryResults(idxQuery, params));

        return ret;
    }

    private List<AtlasFullTextResult> getIndexQueryResults(AtlasIndexQuery query, QueryParams params) {
        List<AtlasFullTextResult> ret  = new ArrayList<>();
        Iterator<Result>          iter = query.vertices();

        while (iter.hasNext() && ret.size() < params.limit()) {
            Result idxQueryResult = iter.next();
            AtlasVertex vertex = idxQueryResult.getVertex();
            String guid = vertex != null ? vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class) : null;

            if (guid != null) {
                AtlasEntityHeaderWithAssociations entity = toAtlasEntityHeaderwithAssociations(vertex);
                Double score = idxQueryResult.getScore();
                ret.add(new AtlasFullTextResult(entity, score));
            }
        }

        return ret;
    }

    private GremlinQuery toGremlinQuery(String query, int limit, int offset) throws AtlasBaseException {
        QueryParams params = validateSearchParams(limit, offset);
        Either<NoSuccess, Expression> either = QueryParser.apply(query, params);

        if (either.isLeft()) {
            throw new AtlasBaseException(DISCOVERY_QUERY_FAILED, query);
        }

        Expression   expression      = either.right().get();
        Expression   validExpression = QueryProcessor.validate(expression);
        GremlinQuery gremlinQuery    = new GremlinTranslator(validExpression, graphPersistenceStrategy).translate();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Translated Gremlin Query: {}", gremlinQuery.queryStr());
        }

        return gremlinQuery;
    }

    private QueryParams validateSearchParams(int limitParam, int offsetParam) {
        int defaultLimit = AtlasConfiguration.SEARCH_DEFAULT_LIMIT.getInt();
        int maxLimit     = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();

        int limit = defaultLimit;
        if (limitParam > 0 && limitParam <= maxLimit) {
            limit = limitParam;
        }

        int offset = 0;
        if (offsetParam > 0) {
            offset = offsetParam;
        }

        return new QueryParams(limit, offset);
    }

    private AtlasEntityHeaderWithAssociations toAtlasEntityHeaderwithAssociations(AtlasVertex vertex) {
        if (vertex == null) {
            return null;
        }

        AtlasEntityHeaderWithAssociations ret = new AtlasEntityHeaderWithAssociations();

        String typeName = vertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);

        ret.setTypeName(typeName);
        ret.setGuid(vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class));
        ret.setDisplayText(vertex.getProperty(Constants.QUALIFIED_NAME, String.class));
        ret.setAttribute(AtlasClient.NAME, vertex.getProperty(PROPERTY_KEY_NAME, String.class));
        ret.setAttribute(AtlasClient.DESCRIPTION, vertex.getProperty(PROPERTY_KEY_DESCRIPTION, String.class));
        ret.setAttribute(AtlasClient.OWNER, vertex.getProperty(PROPERTY_KEY_OWNER, String.class));

        Collection<String> classificationNames = vertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);

        if (CollectionUtils.isNotEmpty(classificationNames)) {
            ret.setClassificationNames(new ArrayList<>(classificationNames));
        }

        String state = vertex.getProperty(Constants.STATE_PROPERTY_KEY, String.class);
        if (state != null) {
            Status status = (state.equalsIgnoreCase("ACTIVE") ? Status.ACTIVE : Status.DELETED);
            ret.setStatus(status);
        }

        return ret;
    }

    private AtlasIndexQuery toAtlasIndexQuery(String fullTextQuery) {
        String graphQuery = String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, fullTextQuery);
        return graph.indexQuery(Constants.FULLTEXT_INDEX, graphQuery);
    }

    private AttributeSearchResult toAttributesResult(List list, GremlinQuery query) {
        AttributeSearchResult ret = new AttributeSearchResult();
        List<String> names = new ArrayList<>();
        List<List<Object>> values = new ArrayList<>();

        // extract select attributes from gremlin query
        Option<SelectExpression> selectExpr = SelectExpressionHelper.extractSelectExpression(query.expr());
        if (selectExpr.isDefined()) {
            List<AliasExpression> aliases = selectExpr.get().toJavaList();

            if (CollectionUtils.isNotEmpty(aliases)) {
                for (AliasExpression alias : aliases) {
                    names.add(alias.alias());
                }
                ret.setName(names);
            }
        }

        for (Object mapObj : list) {
            Map map = (mapObj instanceof Map ? (Map) mapObj : null);
            if (MapUtils.isNotEmpty(map)) {
                for (Object key : map.keySet()) {
                    Object vals = map.get(key);
                    values.add((List<Object>) vals);
                }
                ret.setValues(values);
            }
        }

        return ret;
    }
}
