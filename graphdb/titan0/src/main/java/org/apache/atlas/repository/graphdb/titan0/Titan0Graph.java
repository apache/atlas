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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.repository.graphdb.titan0.query.Titan0GraphQuery;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.utils.IteratorToIterableAdapter;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import com.tinkerpop.pipes.util.structures.Row;


/**
 * Titan 0.5.4 implementation of AtlasGraph.
 */
public class Titan0Graph implements AtlasGraph<Titan0Vertex, Titan0Edge> {

    public Titan0Graph() {

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
        TitanIndexQuery query = getGraph().indexQuery(fulltextIndex, graphQuery);
        return new Titan0IndexQuery(this, query);
    }

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new Titan0DatabaseManager(getGraph().getManagementSystem());
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

    @Override
    public Object getGremlinColumnValue(Object rowValue, String colName, int idx) {
        Row<List> rV = (Row<List>) rowValue;
        Object value = rV.getColumn(colName).get(idx);
        return convertGremlinValue(value);
    }

    @Override
    public Object convertGremlinValue(Object rawValue) {
        if (rawValue instanceof Vertex) {
            return GraphDbObjectFactory.createVertex(this, (Vertex) rawValue);
        }
        if (rawValue instanceof Edge) {
            return GraphDbObjectFactory.createEdge(this, (Edge) rawValue);
        }
        return rawValue;
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {

        return GremlinVersion.TWO;
    }

    @Override
    public List<Object> convertPathQueryResultToList(Object rawValue) {
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
        return Titan0Database.getGraphInstance();
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {
        GraphSONWriter.outputGraph(getGraph(), os);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasGraph#executeGremlinScript(java.
     * lang.String)
     */
    @Override
    public Object executeGremlinScript(String gremlinQuery) throws ScriptException {

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("gremlin-groovy");
        Bindings bindings = engine.createBindings();
        bindings.put("g", getGraph());
        Object result = engine.eval(gremlinQuery, bindings);
        return result;
    }

    @Override
    public String generatePersisentToLogicalConversionExpression(String expr, IDataType<?> type) {

        //nothing special needed, value is stored in required type
        return expr;
    }

    @Override
    public boolean isPropertyValueConversionNeeded(IDataType<?> type) {

        return false;
    }

    @Override
    public boolean requiresInitialIndexedPredicate() {
        return false;
    }

    @Override
    public String getInitialIndexedPredicate() {
        return "";
    }

    @Override
    public String getOutputTransformationPredicate(boolean inSelect, boolean isPath) {
        return "";
    }

    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> wrapEdges(Iterator<Edge> it) {

        Iterable<Edge> iterable = new IteratorToIterableAdapter<Edge>(it);
        return wrapEdges(iterable);
    }

    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> wrapVertices(Iterator<Vertex> it) {
        Iterable<Vertex> iterable = new IteratorToIterableAdapter<Vertex>(it);
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
        Iterable<Edge> result = (Iterable<Edge>)it;
        return Iterables.transform(result, new Function<Edge, AtlasEdge<Titan0Vertex, Titan0Edge>>(){

            @Override
            public AtlasEdge<Titan0Vertex, Titan0Edge> apply(Edge input) {
                return GraphDbObjectFactory.createEdge(Titan0Graph.this, input);
            }
        });
    }
}
