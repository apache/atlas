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
package org.apache.atlas.repository.graphdb.janus;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.repository.graphdb.janus.query.AtlasJanusGraphQuery;
import org.apache.atlas.repository.graphdb.utils.IteratorToIterableAdapter;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.atlas.repository.Constants.INDEX_SEARCH_VERTEX_PREFIX_DEFAULT;
import static org.apache.atlas.repository.Constants.INDEX_SEARCH_VERTEX_PREFIX_PROPERTY;

/**
 * Janus implementation of AtlasGraph.
 */
public class AtlasJanusGraph implements AtlasGraph<AtlasJanusVertex, AtlasJanusEdge> {

    private static Configuration APPLICATION_PROPERTIES = null;

    private final ConvertGremlinValueFunction GREMLIN_VALUE_CONVERSION_FUNCTION = new ConvertGremlinValueFunction();
    private final Set<String>                 multiProperties                   = new HashSet<>();

    public AtlasJanusGraph() {
        //determine multi-properties once at startup
        JanusGraphManagement mgmt = null;

        try {
            mgmt = AtlasJanusGraphDatabase.getGraphInstance().openManagement();

            Iterable<PropertyKey> keys = mgmt.getRelationTypes(PropertyKey.class);

            for (PropertyKey key : keys) {
                if (key.cardinality() != Cardinality.SINGLE) {
                    multiProperties.add(key.name());
                }
            }
        } finally {
            if (mgmt != null) {
                mgmt.rollback();
            }
        }
    }

    @Override
    public AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> addEdge(AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> outVertex,
                                                               AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> inVertex,
                                                               String edgeLabel) {
        try {
            Vertex oV   = outVertex.getV().getWrappedElement();
            Vertex iV   = inVertex.getV().getWrappedElement();
            Edge   edge = oV.addEdge(edgeLabel, iV);

            return GraphDbObjectFactory.createEdge(this, edge);
        } catch (SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public AtlasGraphQuery<AtlasJanusVertex, AtlasJanusEdge> query() {
        return new AtlasJanusGraphQuery(this);
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> V(final Object... vertexIds) {
        AtlasGraphTraversal traversal = new AtlasJanusGraphTraversal(this, getGraph().traversal());
        traversal.getBytecode().addStep(GraphTraversal.Symbols.V, vertexIds);
        traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, vertexIds));
        return traversal;
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> E(final Object... edgeIds) {
        AtlasGraphTraversal traversal = new AtlasJanusGraphTraversal(this, getGraph().traversal());
        traversal.getBytecode().addStep(GraphTraversal.Symbols.E, edgeIds);
        traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, edgeIds));
        return traversal;
    }

    @Override
    public AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> getEdge(String edgeId) {
        Iterator<Edge> it = getGraph().edges(edgeId);
        Edge           e  = getSingleElement(it, edgeId);

        return GraphDbObjectFactory.createEdge(this, e);
    }

    @Override
    public void removeEdge(AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> edge) {
        Edge wrapped = edge.getE().getWrappedElement();

        wrapped.remove();
    }

    @Override
    public void removeVertex(AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> vertex) {
        Vertex wrapped = vertex.getV().getWrappedElement();

        wrapped.remove();
    }

    @Override
    public Iterable<AtlasEdge<AtlasJanusVertex, AtlasJanusEdge>> getEdges() {
        Iterator<Edge> edges = getGraph().edges();

        return wrapEdges(edges);
    }

    @Override
    public Iterable<AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>> getVertices() {
        Iterator<Vertex> vertices = getGraph().vertices();

        return wrapVertices(vertices);
    }

    @Override
    public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> addVertex() {
        Vertex result = getGraph().addVertex();

        return GraphDbObjectFactory.createVertex(this, result);
    }

    @Override
    public void commit() {
        getGraph().tx().commit();
    }

    @Override
    public void rollback() {
        getGraph().tx().rollback();
    }

    @Override
    public AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> indexQuery(String fulltextIndex, String graphQuery) {
        return indexQuery(fulltextIndex, graphQuery, 0);
    }

    @Override
    public AtlasIndexQuery<AtlasJanusVertex, AtlasJanusEdge> indexQuery(String fulltextIndex, String graphQuery, int offset) {
        String               prefix = getIndexQueryPrefix();
        JanusGraphIndexQuery query  = getGraph().indexQuery(fulltextIndex, graphQuery).setElementIdentifier(prefix).offset(offset);

        return new AtlasJanusIndexQuery(this, query);
    }

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new AtlasJanusGraphManagement(this, getGraph().openManagement());
    }

    @Override
    public void shutdown() {
        getGraph().close();
    }

    @Override
    public Set<String> getEdgeIndexKeys() {
        return getIndexKeys(Edge.class);
    }

    @Override
    public Set<String> getVertexIndexKeys() {
        return getIndexKeys(Vertex.class);
    }

    @Override
    public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getVertex(String vertexId) {
        Iterator<Vertex> it     = getGraph().vertices(vertexId);
        Vertex           vertex = getSingleElement(it, vertexId);

        return GraphDbObjectFactory.createVertex(this, vertex);
    }

    @Override
    public Iterable<AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>> getVertices(String key, Object value) {
        AtlasGraphQuery<AtlasJanusVertex, AtlasJanusEdge> query = query();

        query.has(key, value);

        return query.vertices();
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {
        return GremlinVersion.THREE;
    }

    @Override
    public void clear() {
        JanusGraph graph = getGraph();

        if (graph.isOpen()) {
            // only a shut down graph can be cleared
            graph.close();
        }

        try {
            JanusGraphFactory.drop(graph);
        } catch (BackendException ignoreEx) {
        }
    }

    private JanusGraph getGraph() {
        return AtlasJanusGraphDatabase.getGraphInstance();
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {
        GraphSONMapper         mapper  = getGraph().io(IoCore.graphson()).mapper().create();
        GraphSONWriter.Builder builder = GraphSONWriter.build();

        builder.mapper(mapper);

        GraphSONWriter writer = builder.create();

        writer.writeGraph(os, getGraph());
    }

    @Override
    public GremlinGroovyScriptEngine getGremlinScriptEngine() {
        DefaultImportCustomizer.Builder importBuilder = DefaultImportCustomizer.build()
                                                                               .addClassImports(java.util.function.Function.class)
                                                                               .addMethodImports(__.class.getMethods())
                                                                               .addMethodImports(P.class.getMethods());
        GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(importBuilder.create());

        return scriptEngine;
    }

    @Override
    public void releaseGremlinScriptEngine(ScriptEngine scriptEngine) {
        if (scriptEngine instanceof GremlinGroovyScriptEngine) {
            try {
                ((GremlinGroovyScriptEngine) scriptEngine).close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public Object executeGremlinScript(String query, boolean isPath) throws AtlasBaseException {
        Object result = executeGremlinScript(query);

        return convertGremlinValue(result);
    }

    @Override
    public Object executeGremlinScript(ScriptEngine scriptEngine, Map<? extends String, ? extends Object> userBindings,
                                       String query, boolean isPath) throws ScriptException {
        Bindings bindings = scriptEngine.createBindings();

        bindings.putAll(userBindings);
        bindings.put("g", getGraph().traversal());

        Object result = scriptEngine.eval(query, bindings);

        return convertGremlinValue(result);
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
    public GroovyExpression getInitialIndexedPredicate(GroovyExpression parent) {
        return parent;
    }

    @Override
    public GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean isSelect, boolean isPath) {
        return expr;
    }

    @Override
    public boolean isMultiProperty(String propertyName) {
        return multiProperties.contains(propertyName);
    }

    public Iterable<AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>> wrapVertices(Iterable<? extends Vertex> it) {
        return StreamSupport.stream(it.spliterator(), false).map(input -> GraphDbObjectFactory.createVertex(AtlasJanusGraph.this, input)).collect(Collectors.toList());
    }

    public Iterable<AtlasEdge<AtlasJanusVertex, AtlasJanusEdge>> wrapEdges(Iterator<? extends Edge> it) {
        Iterable<? extends Edge> iterable = new IteratorToIterableAdapter<>(it);

        return wrapEdges(iterable);
    }

    public Iterable<AtlasEdge<AtlasJanusVertex, AtlasJanusEdge>> wrapEdges(Iterable<? extends Edge> it) {
        return StreamSupport.stream(it.spliterator(), false).map(input -> GraphDbObjectFactory.createEdge(AtlasJanusGraph.this, input)).collect(Collectors.toList());
    }

    public void addMultiProperties(Set<String> names) {
        multiProperties.addAll(names);
    }


    private String getIndexQueryPrefix() {
        final String ret;

        initApplicationProperties();

        if (APPLICATION_PROPERTIES == null) {
            ret = INDEX_SEARCH_VERTEX_PREFIX_DEFAULT;
        } else {
            ret = APPLICATION_PROPERTIES.getString(INDEX_SEARCH_VERTEX_PREFIX_PROPERTY, INDEX_SEARCH_VERTEX_PREFIX_DEFAULT);
        }

        return ret;
    }

    private Iterable<AtlasVertex<AtlasJanusVertex, AtlasJanusEdge>> wrapVertices(Iterator<? extends Vertex> it) {
        Iterable<? extends Vertex> iterable = new IteratorToIterableAdapter<>(it);

        return wrapVertices(iterable);
    }

    private static <T> T getSingleElement(Iterator<T> it, String id) {
        if (!it.hasNext()) {
            return null;
        }

        T element = it.next();

        if (it.hasNext()) {
            throw new RuntimeException("Multiple items were found with the id " + id);
        }

        return element;
    }

    private Object convertGremlinValue(Object rawValue) {
        if (rawValue instanceof Vertex) {
            return GraphDbObjectFactory.createVertex(this, (Vertex) rawValue);
        } else if (rawValue instanceof Edge) {
            return GraphDbObjectFactory.createEdge(this, (Edge) rawValue);
        } else if (rawValue instanceof Map) {
            Map<String, Object> rowValue = (Map<String, Object>) rawValue;

            return Maps.transformValues(rowValue, GREMLIN_VALUE_CONVERSION_FUNCTION);
        } else if (rawValue instanceof ImmutablePath) {
            ImmutablePath path = (ImmutablePath) rawValue;

            return convertGremlinValue(path.objects());
        } else if (rawValue instanceof List) {
            return Lists.transform((List) rawValue, GREMLIN_VALUE_CONVERSION_FUNCTION);
        } else if (rawValue instanceof Collection) {
            throw new UnsupportedOperationException("Unhandled collection type: " + rawValue.getClass());
        }

        return rawValue;
    }

    private Set<String> getIndexKeys(Class<? extends Element> janusGraphElementClass) {
        JanusGraphManagement      mgmt    = getGraph().openManagement();
        Iterable<JanusGraphIndex> indices = mgmt.getGraphIndexes(janusGraphElementClass);
        Set<String>               result  = new HashSet<String>();

        for (JanusGraphIndex index : indices) {
            result.add(index.name());
        }

        mgmt.commit();

        return result;

    }

    private Object executeGremlinScript(String gremlinQuery) throws AtlasBaseException {
        GremlinGroovyScriptEngine scriptEngine = getGremlinScriptEngine();

        try {
            Bindings bindings = scriptEngine.createBindings();

            bindings.put("graph", getGraph());
            bindings.put("g", getGraph().traversal());

            Object result = scriptEngine.eval(gremlinQuery, bindings);

            return result;
        } catch (ScriptException e) {
            throw new AtlasBaseException(AtlasErrorCode.GREMLIN_SCRIPT_EXECUTION_FAILED, e, gremlinQuery);
        } finally {
            releaseGremlinScriptEngine(scriptEngine);
        }
    }

    private void initApplicationProperties() {
        if (APPLICATION_PROPERTIES == null) {
            try {
                APPLICATION_PROPERTIES = ApplicationProperties.get();
            } catch (AtlasException ex) {
                // ignore
            }
        }
    }


    private final class ConvertGremlinValueFunction implements Function<Object, Object> {
        @Override
        public Object apply(Object input) {
            return convertGremlinValue(input);
        }
    }
}
