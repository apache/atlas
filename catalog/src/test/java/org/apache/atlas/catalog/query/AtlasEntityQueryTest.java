/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog.query;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.Pipe;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.repository.Constants;
import org.easymock.Capture;
import org.testng.annotations.Test;

import java.util.*;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit tests for AtlasEntityQuery.
 */
@SuppressWarnings("unchecked")
public class AtlasEntityQueryTest {
    //todo: add tests for instance query and getInitialPipeline()
    @Test
    public void testExecute_Collection() throws Exception {
        TitanGraph graph = createStrictMock(TitanGraph.class);
        QueryExpression expression = createStrictMock(QueryExpression.class);
        ResourceDefinition resourceDefinition = createStrictMock(ResourceDefinition.class);
        Request request = createStrictMock(Request.class);
        GremlinPipeline initialPipeline = createStrictMock(GremlinPipeline.class);
        Pipe queryPipe = createStrictMock(Pipe.class);
        Pipe expressionPipe = createStrictMock(Pipe.class);
        Pipe notDeletedPipe = createStrictMock(Pipe.class);
        GremlinPipeline rootPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline queryPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline expressionPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline notDeletedPipeline = createStrictMock(GremlinPipeline.class);
        Vertex vertex1 = createStrictMock(Vertex.class);
        VertexWrapper vertex1Wrapper = createStrictMock(VertexWrapper.class);

        List<Vertex> results = new ArrayList<>();
        results.add(vertex1);

        Map<String, Object> vertex1PropertyMap = new HashMap<>();
        vertex1PropertyMap.put("prop1", "prop1.value1");
        vertex1PropertyMap.put("prop2", "prop2.value1");

        Map<String, Object> filteredVertex1PropertyMap = new HashMap<>();
        filteredVertex1PropertyMap.put("prop1", "prop1.value1");

        // mock expectations
        expect(initialPipeline.add(queryPipe)).andReturn(queryPipeline);
        expect(initialPipeline.add(notDeletedPipe)).andReturn(notDeletedPipeline);
        expect(initialPipeline.as("root")).andReturn(rootPipeline);
        expect(expression.asPipe()).andReturn(expressionPipe);
        expect(rootPipeline.add(expressionPipe)).andReturn(expressionPipeline);
        expect(expressionPipeline.back("root")).andReturn(rootPipeline);
        expect(rootPipeline.toList()).andReturn(results);
        graph.commit();
        expect(vertex1Wrapper.getPropertyMap()).andReturn(vertex1PropertyMap);
        expect(resourceDefinition.filterProperties(request, vertex1PropertyMap)).andReturn(filteredVertex1PropertyMap);
        expect(resourceDefinition.resolveHref(filteredVertex1PropertyMap)).andReturn("/foo/bar");
        expect(request.getCardinality()).andReturn(Request.Cardinality.COLLECTION);

        replay(graph, expression, resourceDefinition, request, initialPipeline, queryPipe, expressionPipe,
                notDeletedPipe, rootPipeline, queryPipeline, expressionPipeline, notDeletedPipeline,
                vertex1, vertex1Wrapper);
        // end mock expectations

        AtlasEntityQuery query = new TestAtlasEntityQuery(expression, resourceDefinition, request,
                initialPipeline, queryPipe, notDeletedPipe, graph, vertex1Wrapper);

        // invoke method being tested
        Collection<Map<String, Object>> queryResults = query.execute();

        assertEquals(queryResults.size(), 1);
        Map<String, Object> queryResultMap = queryResults.iterator().next();
        assertEquals(queryResultMap.size(), 2);
        assertEquals(queryResultMap.get("prop1"), "prop1.value1");
        assertEquals(queryResultMap.get("href"), "/foo/bar");

        verify(graph, expression, resourceDefinition, request, initialPipeline, queryPipe, expressionPipe,
                notDeletedPipe, rootPipeline, queryPipeline, expressionPipeline, notDeletedPipeline,
                vertex1, vertex1Wrapper);
    }

    @Test
    public void testExecute_Collection_rollbackOnException() throws Exception {
        TitanGraph graph = createStrictMock(TitanGraph.class);
        QueryExpression expression = createStrictMock(QueryExpression.class);
        ResourceDefinition resourceDefinition = createStrictMock(ResourceDefinition.class);
        Request request = createStrictMock(Request.class);
        GremlinPipeline initialPipeline = createStrictMock(GremlinPipeline.class);
        Pipe queryPipe = createStrictMock(Pipe.class);
        Pipe expressionPipe = createStrictMock(Pipe.class);
        Pipe notDeletedPipe = createStrictMock(Pipe.class);
        GremlinPipeline rootPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline queryPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline expressionPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline notDeletedPipeline = createStrictMock(GremlinPipeline.class);

        // mock expectations
        expect(initialPipeline.add(queryPipe)).andReturn(queryPipeline);
        expect(initialPipeline.add(notDeletedPipe)).andReturn(notDeletedPipeline);
        expect(initialPipeline.as("root")).andReturn(rootPipeline);
        expect(expression.asPipe()).andReturn(expressionPipe);
        expect(rootPipeline.add(expressionPipe)).andReturn(expressionPipeline);
        expect(expressionPipeline.back("root")).andReturn(rootPipeline);
        expect(rootPipeline.toList()).andThrow(new RuntimeException("something bad happened"));
        graph.rollback();

        replay(graph, expression, resourceDefinition, request, initialPipeline, queryPipe, expressionPipe,
                notDeletedPipe, rootPipeline, queryPipeline, expressionPipeline, notDeletedPipeline);
        // end mock expectations

        AtlasEntityQuery query = new TestAtlasEntityQuery(expression, resourceDefinition, request,
                initialPipeline, queryPipe, notDeletedPipe, graph, null);

        try {
            // invoke method being tested
            query.execute();
            fail("expected exception");
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "something bad happened");
        }

        verify(graph, expression, resourceDefinition, request, initialPipeline, queryPipe, expressionPipe,
                notDeletedPipe, rootPipeline, queryPipeline, expressionPipeline, notDeletedPipeline);
    }

    @Test
    public void testExecute_Collection_update() throws Exception {
        TitanGraph graph = createStrictMock(TitanGraph.class);
        QueryExpression expression = createStrictMock(QueryExpression.class);
        ResourceDefinition resourceDefinition = createStrictMock(ResourceDefinition.class);
        Request request = createStrictMock(Request.class);
        GremlinPipeline initialPipeline = createStrictMock(GremlinPipeline.class);
        Pipe queryPipe = createStrictMock(Pipe.class);
        Pipe expressionPipe = createStrictMock(Pipe.class);
        Pipe notDeletedPipe = createStrictMock(Pipe.class);
        GremlinPipeline rootPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline queryPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline expressionPipeline = createStrictMock(GremlinPipeline.class);
        GremlinPipeline notDeletedPipeline = createStrictMock(GremlinPipeline.class);
        Vertex vertex1 = createStrictMock(Vertex.class);
        VertexWrapper vertex1Wrapper = createStrictMock(VertexWrapper.class);
        Capture<Long> modifiedTimestampCapture = newCapture();

        List<Vertex> results = new ArrayList<>();
        results.add(vertex1);

        Map<String, Object> vertex1PropertyMap = new HashMap<>();
        vertex1PropertyMap.put("prop1", "prop1.value1");
        vertex1PropertyMap.put("prop2", "prop2.value1");

        Map<String, Object> filteredVertex1PropertyMap = new HashMap<>();
        filteredVertex1PropertyMap.put("prop1", "prop1.value1");

        Map<String, Object> updateProperties = new HashMap<>();
        updateProperties.put("prop3", "newValue");

        // mock expectations
        expect(initialPipeline.add(queryPipe)).andReturn(queryPipeline);
        expect(initialPipeline.add(notDeletedPipe)).andReturn(notDeletedPipeline);
        expect(initialPipeline.as("root")).andReturn(rootPipeline);
        expect(expression.asPipe()).andReturn(expressionPipe);
        expect(rootPipeline.add(expressionPipe)).andReturn(expressionPipeline);
        expect(expressionPipeline.back("root")).andReturn(rootPipeline);
        expect(rootPipeline.toList()).andReturn(results);
        graph.commit();
        vertex1Wrapper.setProperty("prop3", "newValue");
        vertex1Wrapper.setProperty(eq(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY), capture(modifiedTimestampCapture));
        expect(vertex1Wrapper.getPropertyMap()).andReturn(vertex1PropertyMap);
        expect(resourceDefinition.filterProperties(request, vertex1PropertyMap)).andReturn(filteredVertex1PropertyMap);
        expect(resourceDefinition.resolveHref(filteredVertex1PropertyMap)).andReturn("/foo/bar");
        expect(request.getCardinality()).andReturn(Request.Cardinality.COLLECTION);

        replay(graph, expression, resourceDefinition, request, initialPipeline, queryPipe, expressionPipe,
                notDeletedPipe, rootPipeline, queryPipeline, expressionPipeline, notDeletedPipeline,
                vertex1, vertex1Wrapper);
        // end mock expectations

        AtlasEntityQuery query = new TestAtlasEntityQuery(expression, resourceDefinition, request,
                initialPipeline, queryPipe, notDeletedPipe, graph, vertex1Wrapper);

        long startTime = System.currentTimeMillis();
        // invoke method being tested
        Collection<Map<String, Object>> queryResults = query.execute(updateProperties);
        long endTime = System.currentTimeMillis();

        assertEquals(queryResults.size(), 1);
        Map<String, Object> queryResultMap = queryResults.iterator().next();
        assertEquals(queryResultMap.size(), 2);
        assertEquals(queryResultMap.get("prop1"), "prop1.value1");
        assertEquals(queryResultMap.get("href"), "/foo/bar");

        long modifiedTimestamp = modifiedTimestampCapture.getValue();
        assertTrue(modifiedTimestamp >= startTime && modifiedTimestamp <= endTime);

        verify(graph, expression, resourceDefinition, request, initialPipeline, queryPipe, expressionPipe,
                notDeletedPipe, rootPipeline, queryPipeline, expressionPipeline, notDeletedPipeline,
                vertex1, vertex1Wrapper);
    }

    private class TestAtlasEntityQuery extends AtlasEntityQuery {
        private final GremlinPipeline initialPipeline;
        private final Pipe queryPipe;
        private final Pipe notDeletedPipe;
        private final TitanGraph graph;
        private final VertexWrapper vWrapper;

        public TestAtlasEntityQuery(QueryExpression queryExpression,
                                    ResourceDefinition resourceDefinition,
                                    Request request,
                                    GremlinPipeline initialPipeline,
                                    Pipe queryPipe,
                                    Pipe notDeletedPipe,
                                    TitanGraph graph,
                                    VertexWrapper vWrapper) {

            super(queryExpression, resourceDefinition, request);
            this.initialPipeline = initialPipeline;
            this.queryPipe = queryPipe;
            this.notDeletedPipe = notDeletedPipe;
            this.graph = graph;
            this.vWrapper = vWrapper;
        }

        @Override
        protected GremlinPipeline getRootVertexPipeline() {
            return initialPipeline;
        }

        @Override
        protected Pipe getQueryPipe() {
            return queryPipe;
        }

        @Override
        protected Pipe getNotDeletedPipe() {
            return notDeletedPipe;
        }

        @Override
        protected TitanGraph getGraph() {
            return graph;
        }

        @Override
        protected VertexWrapper wrapVertex(Vertex v) {
            return vWrapper;
        }
    }
}
