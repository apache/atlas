/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.discovery.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.discovery.DiscoveryException;
import org.apache.atlas.discovery.DiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.query.Expressions;
import org.apache.atlas.query.GremlinEvaluator;
import org.apache.atlas.query.GremlinQuery;
import org.apache.atlas.query.GremlinQueryResult;
import org.apache.atlas.query.GremlinTranslator;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.query.QueryParser;
import org.apache.atlas.query.QueryProcessor;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.util.CompiledQueryCacheKey;
import org.apache.atlas.util.NoopGremlinQuery;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.util.Either;
import scala.util.parsing.combinator.Parsers;

/**
 * Graph backed implementation of Search.
 */
@Singleton
public class GraphBackedDiscoveryService implements DiscoveryService {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedDiscoveryService.class);

    private final AtlasGraph graph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;

    public final static String SCORE = "score";

    @Inject
    GraphBackedDiscoveryService(MetadataRepository metadataRepository)
    throws DiscoveryException {
        this.graph = AtlasGraphProvider.getGraphInstance();
        this.graphPersistenceStrategy = new DefaultGraphPersistenceStrategy(metadataRepository);
    }

    //For titan 0.5.4, refer to http://s3.thinkaurelius.com/docs/titan/0.5.4/index-backends.html for indexed query
    //http://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query
    // .html#query-string-syntax for query syntax
    @Override
    @GraphTransaction
    public String searchByFullText(String query, QueryParams queryParams) throws DiscoveryException {
        String graphQuery = String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, query);
        LOG.debug("Full text query: {}", graphQuery);
        Iterator<AtlasIndexQuery.Result<?, ?>> results =graph.indexQuery(Constants.FULLTEXT_INDEX, graphQuery).vertices();
        JSONArray response = new JSONArray();

        int index = 0;
        while (results.hasNext() && index < queryParams.offset()) {
            results.next();
            index++;
        }

        while (results.hasNext() && response.length() < queryParams.limit()) {

            AtlasIndexQuery.Result<?,?> result = results.next();
            AtlasVertex<?,?> vertex = result.getVertex();

            JSONObject row = new JSONObject();
            String guid = GraphHelper.getGuid(vertex);
            if (guid != null) { //Filter non-class entities
                try {
                    row.put("guid", guid);
                    row.put(AtlasClient.TYPENAME, GraphHelper.getTypeName(vertex));
                    row.put(SCORE, result.getScore());
                } catch (JSONException e) {
                    LOG.error("Unable to create response", e);
                    throw new DiscoveryException("Unable to create response");
                }

                response.put(row);
            }
        }
        return response.toString();
    }

    @Override
    @GraphTransaction
    public String searchByDSL(String dslQuery, QueryParams queryParams) throws DiscoveryException {
        GremlinQueryResult queryResult = evaluate(dslQuery, queryParams);
        return queryResult.toJson();
    }

    public GremlinQueryResult evaluate(String dslQuery, QueryParams queryParams) throws DiscoveryException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("Executing dsl query={}", dslQuery);
        }
        try {
            GremlinQuery gremlinQuery = parseAndTranslateDsl(dslQuery, queryParams);
            if(gremlinQuery instanceof NoopGremlinQuery) {
                return new GremlinQueryResult(dslQuery, ((NoopGremlinQuery)gremlinQuery).getDataType(), Collections.emptyList());
            }

            return new GremlinEvaluator(gremlinQuery, graphPersistenceStrategy, graph).evaluate();

        } catch (Exception e) { // unable to catch ExpressionException
            throw new DiscoveryException("Invalid expression : " + dslQuery, e);
        }
    }

    private GremlinQuery parseAndTranslateDsl(String dslQuery, QueryParams queryParams) throws DiscoveryException {

        CompiledQueryCacheKey entry = new CompiledQueryCacheKey(dslQuery, queryParams);
        GremlinQuery gremlinQuery = QueryProcessor.compiledQueryCache().get(entry);
        if(gremlinQuery == null) {
            Expressions.Expression validatedExpression = parseQuery(dslQuery, queryParams);

            //If the final limit is 0, don't launch the query, return with 0 rows
            if (validatedExpression instanceof Expressions.LimitExpression
                    && ((Integer)((Expressions.LimitExpression) validatedExpression).limit().rawValue()) == 0) {
                gremlinQuery = new NoopGremlinQuery(validatedExpression.dataType());
            }
            else {
                gremlinQuery = new GremlinTranslator(validatedExpression, graphPersistenceStrategy).translate();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query = {}", validatedExpression);
                    LOG.debug("Expression Tree = {}", validatedExpression.treeString());
                    LOG.debug("Gremlin Query = {}", gremlinQuery.queryStr());
                }
            }
            QueryProcessor.compiledQueryCache().put(entry, gremlinQuery);
        }
        return gremlinQuery;
    }

    private Expressions.Expression parseQuery(String dslQuery, QueryParams queryParams) throws DiscoveryException {
        Either<Parsers.NoSuccess, Expressions.Expression> either = QueryParser.apply(dslQuery, queryParams);
        if (either.isRight()) {
            Expressions.Expression expression = either.right().get();
            Expressions.Expression validatedExpression = QueryProcessor.validate(expression);
            return validatedExpression;
        } else {
            throw new DiscoveryException("Invalid expression : " + dslQuery + ". " + either.left());
        }

    }

    /**
     * Assumes the User is familiar with the persistence structure of the Repository.
     * The given query is run uninterpreted against the underlying Graph Store.
     * The results are returned as a List of Rows. each row is a Map of Key,Value pairs.
     *
     * @param gremlinQuery query in gremlin dsl format
     * @return List of Maps
     * @throws org.apache.atlas.discovery.DiscoveryException
     */
    @Override
    @GraphTransaction
    public List<Map<String, String>> searchByGremlin(String gremlinQuery) throws DiscoveryException {
        LOG.debug("Executing gremlin query={}", gremlinQuery);
        try {
            Object o = graph.executeGremlinScript(gremlinQuery, false);
            return extractResult(o);
        } catch (AtlasBaseException e) {
            throw new DiscoveryException(e);
        }
    }

    private List<Map<String, String>> extractResult(final Object o) throws DiscoveryException {
        List<Map<String, String>> result = new ArrayList<>();
        if (o instanceof List) {
            List l = (List) o;

            for (Object value : l) {
                Map<String, String> oRow = new HashMap<>();
                if (value instanceof Map) {
                    @SuppressWarnings("unchecked") Map<Object, Object> iRow = (Map) value;
                    for (Map.Entry e : iRow.entrySet()) {
                        Object k = e.getKey();
                        Object v = e.getValue();
                        oRow.put(k.toString(), v.toString());
                    }
                } else if (value instanceof AtlasVertex) {
                    AtlasVertex<?,?> vertex = (AtlasVertex<?,?>)value;
                    for (String key : vertex.getPropertyKeys()) {
                        Object propertyValue = GraphHelper.getProperty(vertex,  key);
                        if (propertyValue != null) {
                            oRow.put(key, propertyValue.toString());
                        }
                    }

                } else if (value instanceof String) {
                    oRow.put("", value.toString());
                } else if(value instanceof AtlasEdge) {
                    AtlasEdge edge = (AtlasEdge) value;
                    oRow.put("id", edge.getId().toString());
                    oRow.put("label", edge.getLabel());
                    oRow.put("inVertex", edge.getInVertex().getId().toString());
                    oRow.put("outVertex", edge.getOutVertex().getId().toString());
                    for (String propertyKey : edge.getPropertyKeys()) {
                        oRow.put(propertyKey, GraphHelper.getProperty(edge, propertyKey).toString());
                    }
                } else {
                    throw new DiscoveryException(String.format("Cannot process result %s", String.valueOf(value)));
                }

                result.add(oRow);
            }
        }
        else {
            result.add(new HashMap<String, String>() {{
                put("result", o.toString());
            }});
        }
        return result;
    }
}
