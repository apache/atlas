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

package org.apache.hadoop.metadata.discovery.graph;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.hadoop.metadata.discovery.DiscoveryException;
import org.apache.hadoop.metadata.discovery.DiscoveryService;
import org.apache.hadoop.metadata.query.Expressions;
import org.apache.hadoop.metadata.query.GremlinEvaluator;
import org.apache.hadoop.metadata.query.GremlinQuery;
import org.apache.hadoop.metadata.query.GremlinQueryResult;
import org.apache.hadoop.metadata.query.GremlinTranslator;
import org.apache.hadoop.metadata.query.QueryParser;
import org.apache.hadoop.metadata.query.QueryProcessor;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.repository.graph.GraphHelper;
import org.apache.hadoop.metadata.repository.graph.TitanGraphService;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.parsing.combinator.Parsers;

import javax.inject.Inject;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphBackedDiscoveryService implements DiscoveryService {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedDiscoveryService.class);

    private final TitanGraph titanGraph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;

    @Inject
    GraphBackedDiscoveryService(TitanGraphService graphService,
                                MetadataRepository metadataRepository) throws DiscoveryException {
        this.titanGraph = graphService.getTitanGraph();
        this.graphPersistenceStrategy = new DefaultGraphPersistenceStrategy(metadataRepository);
    }

    private static void searchWalker(Vertex vtx, final int max, int counter,
                                     HashMap<String, JSONObject> e,
                                     HashMap<String, JSONObject> v, String edgesToFollow) {
        counter++;
        if (counter <= max) {
            Map<String, String> jsonVertexMap = new HashMap<>();
            Iterator<Edge> edgeIterator;

            // If we're doing a lineage traversal, only follow the edges specified by the query.
            // Otherwise return them all.
            if (edgesToFollow != null) {
                IteratorChain ic = new IteratorChain();

                for (String iterateOn : edgesToFollow.split(",")) {
                    ic.addIterator(vtx.query().labels(iterateOn).edges().iterator());
                }

                edgeIterator = ic;

            } else {
                edgeIterator = vtx.query().edges().iterator();
            }

            //Iterator<Edge> edgeIterator = vtx.query().labels("Fathered").edges().iterator();
            jsonVertexMap.put("HasRelationships", ((Boolean) edgeIterator.hasNext()).toString());

            for (String pKey : vtx.getPropertyKeys()) {
                jsonVertexMap.put(pKey, vtx.getProperty(pKey).toString());
            }

            // Add to the Vertex map.
            v.put(vtx.getId().toString(), new JSONObject(jsonVertexMap));

            // Follow this Vertex's edges if this isn't the last level of depth
            if (counter < max) {
                while (edgeIterator != null && edgeIterator.hasNext()) {

                    Edge edge = edgeIterator.next();
                    String label = edge.getLabel();

                    Map<String, String> jsonEdgeMap = new HashMap<>();
                    String tail = edge.getVertex(Direction.OUT).getId().toString();
                    String head = edge.getVertex(Direction.IN).getId().toString();

                    jsonEdgeMap.put("tail", tail);
                    jsonEdgeMap.put("head", head);
                    jsonEdgeMap.put("label", label);

                    Direction d;
                    if (tail.equals(vtx.getId().toString())) {
                        d = Direction.IN;
                    } else {
                        d = Direction.OUT;
                    }

   	   				/* If we want an Edge's property keys, uncomment here.  Or we can parameterize
                           it.
   	   			 	   Code is here now for reference/memory-jogging.
   					for (String pKey: edge.getPropertyKeys()) {
   	   					jsonEdgeMap.put(pKey, edge.getProperty(pKey).toString());
   	   				}
   	   			    */

                    e.put(edge.getId().toString(), new JSONObject(jsonEdgeMap));
                    searchWalker(edge.getVertex(d), max, counter, e, v, edgesToFollow);
                }
            }
        }
    }

    /**
     * Search using query DSL.
     *
     * @param dslQuery query in DSL format.
     * @return JSON representing the type and results.
     */
    @Override
    public String searchByDSL(String dslQuery) throws DiscoveryException {
        QueryParser queryParser = new QueryParser();
        Either<Parsers.NoSuccess, Expressions.Expression> either = queryParser.apply(dslQuery);
        if (either.isRight()) {
            Expressions.Expression expression = either.right().get();
            GremlinQueryResult queryResult = evaluate(expression);
            return queryResult.toJson();
        }

        throw new DiscoveryException("Invalid expression : " + dslQuery);
    }

    private GremlinQueryResult evaluate(Expressions.Expression expression) {
        Expressions.Expression validatedExpression = QueryProcessor.validate(expression);
        GremlinQuery gremlinQuery =
                new GremlinTranslator(validatedExpression, graphPersistenceStrategy).translate();
        System.out.println("---------------------");
        System.out.println("Query = " + validatedExpression);
        System.out.println("Expression Tree = " + validatedExpression.treeString());
        System.out.println("Gremlin Query = " + gremlinQuery.queryStr());
        System.out.println("---------------------");
        return new GremlinEvaluator(gremlinQuery, graphPersistenceStrategy, titanGraph).evaluate();
    }

    /**
     * Assumes the User is familiar with the persistence structure of the Repository.
     * The given query is run uninterpreted against the underlying Graph Store.
     * The results are returned as a List of Rows. each row is a Map of Key,Value pairs.
     *
     * @param gremlinQuery query in gremlin dsl format
     * @return List of Maps
     * @throws org.apache.hadoop.metadata.discovery.DiscoveryException
     */
    @Override
    public List<Map<String, String>> searchByGremlin(String gremlinQuery)
            throws DiscoveryException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("gremlin-groovy");
        Bindings bindings = engine.createBindings();
        bindings.put("g", titanGraph);

        try {
            Object o = engine.eval(gremlinQuery, bindings);
            if (!(o instanceof List)) {
                throw new DiscoveryException(
                        String.format("Cannot process gremlin result %s", o.toString()));
            }

            List l = (List) o;
            List<Map<String, String>> result = new ArrayList<>();
            for (Object r : l) {

                Map<String, String> oRow = new HashMap<>();
                if (r instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> iRow = (Map) r;
                    for (Map.Entry e : iRow.entrySet()) {
                        Object k = e.getKey();
                        Object v = e.getValue();
                        oRow.put(k.toString(), v.toString());
                    }
                } else if (r instanceof TitanVertex) {
                    Iterable<TitanProperty> ps = ((TitanVertex) r).getProperties();
                    for (TitanProperty tP : ps) {
                        String pName = tP.getPropertyKey().getName();
                        Object pValue = ((TitanVertex) r).getProperty(pName);
                        if (pValue != null) {
                            oRow.put(pName, pValue.toString());
                        }
                    }

                } else if (r instanceof String) {
                    oRow.put("", r.toString());
                } else {
                    throw new DiscoveryException(
                            String.format("Cannot process gremlin result %s", o.toString()));
                }

                result.add(oRow);
            }
            return result;

        } catch (ScriptException se) {
            throw new DiscoveryException(se);
        }
    }

    /*
     * Simple direct graph search and depth traversal.
     * @param searchText is plain text
     * @param prop is the Vertex property to search.
     */
    @Override
    public Map<String, HashMap<String, JSONObject>> textSearch(String searchText,
                                                               int depth, String prop) {

        HashMap<String, HashMap<String, JSONObject>> result = new HashMap<>();

        // HashMaps, which contain sub JOSN Objects to be relayed back to the parent.
        HashMap<String, JSONObject> vertices = new HashMap<>();
        HashMap<String, JSONObject> edges = new HashMap<>();

        /* todo: Later - when we allow search limitation by "type".
        ArrayList<String> typesList = new ArrayList<String>();
        for (String s: types.split(",")) {

        	// Types validity check.
        	if (typesList.contains(s)) {
        		LOG.error("Specifyed type is not a member of the Type System= {}", s);
        		throw new WebApplicationException(
                        Servlets.getErrorResponse("Invalid type specified in query.", Response
                        .Status.INTERNAL_SERVER_ERROR));
        	}
        	typesList.add(s);
        }*/

        int resultCount = 0;

        //for (Result<Vertex> v: g.indexQuery(Constants.VERTEX_INDEX, "v." + prop + ":(" +
        // searchText + ")").vertices()) {
        for (Vertex v : ((GraphQuery) titanGraph.query().has(prop, searchText)).vertices()) {

            //searchWalker(v.getElement(), depth, 0, edges, vertices, null);
            searchWalker(v, depth, 0, edges, vertices, null);
            resultCount++;

        }

        LOG.debug("Search for {} returned {} results.", searchText, resultCount);

        result.put("vertices", vertices);
        result.put("edges", edges);

        return result;
    }

    /*
     * Simple graph walker for search interface, which allows following of specific edges only.
     * @param edgesToFollow is a comma-separated-list of edges to follow.
     */
    @Override
    public Map<String, HashMap<String, JSONObject>> relationshipWalk(String guid, int depth,
                                                                     String edgesToFollow) {

        HashMap<String, HashMap<String, JSONObject>> result = new HashMap<>();

        // HashMaps, which contain sub JOSN Objects to be relayed back to the parent.
        HashMap<String, JSONObject> vertices = new HashMap<>();
        HashMap<String, JSONObject> edges = new HashMap<>();

        // Get the Vertex with the specified GUID.
        Vertex v = GraphHelper.findVertexByGUID(titanGraph, guid);

        if (v != null) {
            searchWalker(v, depth, 0, edges, vertices, edgesToFollow);
            LOG.debug("Vertex {} found for guid {}", v, guid);
        } else {
            LOG.debug("Vertex not found for guid {}", guid);
        }

        result.put("vertices", vertices);
        result.put("edges", edges);

        return result;

    }

    /**
     * Return a Set of indexed properties in the graph.
     * No parameters.
     */
    @Override
    public Set<String> getGraphIndexedFields() {
        return titanGraph.getIndexedKeys(Vertex.class);
    }
}
