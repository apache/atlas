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
package org.apache.atlas.repository.graphdb.titan0;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import com.tinkerpop.pipes.util.structures.Row;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.repository.graphdb.titan0.query.Titan0GraphQuery;
import org.apache.atlas.repository.graphdb.utils.IteratorToIterableAdapter;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Titan 0.5.4 implementation of AtlasGraph.
 */
public class Titan0Graph implements AtlasGraph<Titan0Vertex, Titan0Edge> {
    private static final Logger LOG = LoggerFactory.getLogger(Titan0Graph.class);

    private final Set<String> multiProperties;

    public Titan0Graph() {
        //determine multi-properties once at startup
        TitanManagement mgmt = null;
        try {
            mgmt = Titan0GraphDatabase.getGraphInstance().getManagementSystem();
            Iterable<PropertyKey> keys = mgmt.getRelationTypes(PropertyKey.class);
            multiProperties = Collections.synchronizedSet(new HashSet<String>());
            for(PropertyKey key : keys) {
                if (key.getCardinality() != Cardinality.SINGLE) {
                    multiProperties.add(key.getName());
                }
            }
        } finally {
            if (mgmt != null) {
                mgmt.rollback();
            }
        }
    }

    @Override
    public AtlasEdge<Titan0Vertex, Titan0Edge> addEdge(AtlasVertex<Titan0Vertex, Titan0Edge> outVertex,
            AtlasVertex<Titan0Vertex, Titan0Edge> inVertex, String edgeLabel) {
        try {
            Edge edge = getGraph().addEdge(null, outVertex.getV().getWrappedElement(),
                    inVertex.getV().getWrappedElement(), edgeLabel);
            return GraphDbObjectFactory.createEdge(this, edge);
        } catch (SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }


    @Override
    public AtlasGraphQuery<Titan0Vertex, Titan0Edge> query() {

        return new Titan0GraphQuery(this);
    }

    @Override
    public AtlasEdge<Titan0Vertex, Titan0Edge> getEdge(String edgeId) {
        Edge edge = getGraph().getEdge(edgeId);
        return GraphDbObjectFactory.createEdge(this, edge);
    }

    @Override
    public void removeEdge(AtlasEdge<Titan0Vertex, Titan0Edge> edge) {
        getGraph().removeEdge(edge.getE().getWrappedElement());

    }

    @Override
    public void removeVertex(AtlasVertex<Titan0Vertex, Titan0Edge> vertex) {
        getGraph().removeVertex(vertex.getV().getWrappedElement());

    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> getEdges() {
        Iterable<Edge> edges = getGraph().getEdges();
        return wrapEdges(edges);
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> getVertices() {
        Iterable<Vertex> vertices = getGraph().getVertices();
        return wrapVertices(vertices);
    }

    @Override
    public AtlasVertex<Titan0Vertex, Titan0Edge> addVertex() {
        Vertex result = getGraph().addVertex(null);
        return GraphDbObjectFactory.createVertex(this, result);
    }

    @Override
    public void commit() {
        getGraph().commit();
    }

    @Override
    public void rollback() {
        getGraph().rollback();
    }

    @Override
    public AtlasIndexQuery<Titan0Vertex, Titan0Edge> indexQuery(String fulltextIndex, String graphQuery) {
        return indexQuery(fulltextIndex, graphQuery, 0);
    }

    @Override
    public AtlasIndexQuery<Titan0Vertex, Titan0Edge> indexQuery(String fulltextIndex, String graphQuery, int offset) {
        TitanIndexQuery query = getGraph().indexQuery(fulltextIndex, graphQuery).offset(offset);
        return new Titan0IndexQuery(this, query);
    }

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new Titan0GraphManagement(this, getGraph().getManagementSystem());
    }

    @Override
    public void shutdown() {
        getGraph().shutdown();
    }

    @Override
    public Set<String> getVertexIndexKeys() {
        return getIndexKeys(Vertex.class);
    }

    @Override
    public Set<String> getEdgeIndexKeys() {
        return getIndexKeys(Edge.class);
    }

    private Set<String> getIndexKeys(Class<? extends Element> titanClass) {

        return getGraph().getIndexedKeys(titanClass);
    }

    @Override
    public AtlasVertex<Titan0Vertex, Titan0Edge> getVertex(String vertexId) {
        Vertex v = getGraph().getVertex(vertexId);
        return GraphDbObjectFactory.createVertex(this, v);
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> getVertices(String key, Object value) {

        Iterable<Vertex> result = getGraph().getVertices(key, value);
        return wrapVertices(result);
    }

    private Object convertGremlinValue(Object rawValue) {

        if (rawValue instanceof Vertex) {
            return GraphDbObjectFactory.createVertex(this, (Vertex) rawValue);
        } else if (rawValue instanceof Edge) {
            return GraphDbObjectFactory.createEdge(this, (Edge) rawValue);
        } else if (rawValue instanceof Row) {
            Row rowValue = (Row)rawValue;
            Map<String, Object> result = new HashMap<>(rowValue.size());
            List<String> columnNames = rowValue.getColumnNames();
            for(int i = 0; i < rowValue.size(); i++) {
                String key = columnNames.get(i);
                Object value = convertGremlinValue(rowValue.get(i));
                result.put(key, value);
            }
            return result;
        } else if (rawValue instanceof List) {
            return Lists.transform((List)rawValue, new Function<Object, Object>() {
                @Override
                public Object apply(Object input) {
                    return convertGremlinValue(input);
                }
            });
        } else if (rawValue instanceof Collection) {
            throw new UnsupportedOperationException("Unhandled collection type: " + rawValue.getClass());
        }
        return rawValue;
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {

        return GremlinVersion.TWO;
    }

    private List<Object> convertPathQueryResultToList(Object rawValue) {
        return (List<Object>) rawValue;
    }


    @Override
    public void clear() {
        TitanGraph graph = getGraph();
        if (graph.isOpen()) {
            // only a shut down graph can be cleared
            graph.shutdown();
        }
        TitanCleanup.clear(graph);
    }

    private TitanGraph getGraph() {
        // return the singleton instance of the graph in the plugin
        return Titan0GraphDatabase.getGraphInstance();
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {
        GraphSONWriter.outputGraph(getGraph(), os);
    }

    @Override
    public Object executeGremlinScript(String query, boolean isPath) throws AtlasBaseException {

        Object result = executeGremlinScript(query);
        return convertGremlinScriptResult(isPath, result);
    }

    private Object convertGremlinScriptResult(boolean isPath, Object result) {
        if (isPath) {
            List<Object> path = convertPathQueryResultToList(result);

            List<Object> convertedResult = new ArrayList<>(path.size());
            for(Object o : path) {
                convertedResult.add(convertGremlinValue(o));
            }
            return convertedResult;
        } else {
            return convertGremlinValue(result);
        }
    }

    @Override
    public ScriptEngine getGremlinScriptEngine() throws AtlasBaseException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine        engine  = manager.getEngineByName("gremlin-groovy");

        if (engine == null) {
            throw new AtlasBaseException(AtlasErrorCode.FAILED_TO_OBTAIN_GREMLIN_SCRIPT_ENGINE, "gremlin-groovy");
        }

        //Do not cache script compilations due to memory implications
        engine.getContext().setAttribute("#jsr223.groovy.engine.keep.globals", "phantom", ScriptContext.ENGINE_SCOPE);

        return engine;
    }

    @Override
    public void releaseGremlinScriptEngine(ScriptEngine scriptEngine) {
        // no action needed
    }

    @Override
    public Object executeGremlinScript(ScriptEngine scriptEngine, Map<? extends  String, ? extends  Object> userBindings, String query, boolean isPath) throws ScriptException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("executeGremlinScript(query={}, userBindings={})", query, userBindings);
        }

        Bindings bindings = scriptEngine.createBindings();

        if (userBindings != null) {
            bindings.putAll(userBindings);
        }

        bindings.put("g", getGraph());

        Object result = scriptEngine.eval(query, bindings);

        return convertGremlinScriptResult(isPath, result);
    }

    private Object executeGremlinScript(String gremlinQuery) throws AtlasBaseException {
        Object       result = null;
        ScriptEngine engine = getGremlinScriptEngine();

        try {
            Bindings bindings = engine.createBindings();

            bindings.put("g", getGraph());

            result = engine.eval(gremlinQuery, bindings);
        } catch (ScriptException e) {
            throw new AtlasBaseException(AtlasErrorCode.GREMLIN_SCRIPT_EXECUTION_FAILED, gremlinQuery);
        } finally {
            releaseGremlinScriptEngine(engine);
        }

        return result;
    }

    @Override
    public GroovyExpression generatePersisentToLogicalConversionExpression(GroovyExpression expr, AtlasType type) {

        //nothing special needed, value is stored in required type
        return expr;
    }

    @Override
    public boolean isPropertyValueConversionNeeded(AtlasType type) {

        return false;
    }

    @Override
    public boolean requiresInitialIndexedPredicate() {
        return false;
    }

    @Override
    public GroovyExpression getInitialIndexedPredicate(GroovyExpression expr) {
        return expr;
    }

    @Override
    public GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean inSelect, boolean isPath) {
        return expr;
    }

    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> wrapEdges(Iterator<Edge> it) {

        Iterable<Edge> iterable = new IteratorToIterableAdapter<>(it);
        return wrapEdges(iterable);
    }

    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> wrapVertices(Iterator<Vertex> it) {
        Iterable<Vertex> iterable = new IteratorToIterableAdapter<>(it);
        return wrapVertices(iterable);
    }

    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> wrapVertices(Iterable<Vertex> it) {

        return Iterables.transform(it, new Function<Vertex, AtlasVertex<Titan0Vertex, Titan0Edge>>(){

            @Override
            public AtlasVertex<Titan0Vertex, Titan0Edge> apply(Vertex input) {
                return GraphDbObjectFactory.createVertex(Titan0Graph.this, input);
            }
        });

    }

    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> wrapEdges(Iterable<Edge> it) {
        Iterable<Edge> result = it;
        return Iterables.transform(result, new Function<Edge, AtlasEdge<Titan0Vertex, Titan0Edge>>(){

            @Override
            public AtlasEdge<Titan0Vertex, Titan0Edge> apply(Edge input) {
                return GraphDbObjectFactory.createEdge(Titan0Graph.this, input);
            }
        });
    }

    @Override
    public boolean isMultiProperty(String propertyName) {
        return multiProperties.contains(propertyName);
    }

    public void addMultiProperties(Set<String> names) {
        multiProperties.addAll(names);
    }
}
