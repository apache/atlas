/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graph;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.atlas.repository.RepositoryException;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Iterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class GraphHelperMockTest {

    private GraphHelper graphHelperInstance;

    private TitanGraph graph;

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
        graph = mock(TitanGraph.class);
        graphHelperInstance = GraphHelper.getInstance(graph);
    }

    @Test(expectedExceptions = RepositoryException.class)
    public void testGetOrCreateEdgeLabelWithMaxRetries() throws Exception {
        final String edgeLabel = "testLabel";
        TitanVertex v1 = mock(TitanVertex.class);
        TitanVertex v2 = mock(TitanVertex.class);

        Iterable noEdgesIterable = new Iterable<Edge>() {
            @Override
            public Iterator<Edge> iterator() {
                return new Iterator<Edge>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Edge next() {
                        return null;
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
        };
        when(v2.getEdges(Direction.IN)).thenReturn(noEdgesIterable);
        when(v1.getEdges(Direction.OUT)).thenReturn(noEdgesIterable);

        when(v1.getId()).thenReturn(new String("1234"));
        when(v2.getId()).thenReturn(new String("5678"));
        when(graph.addEdge(null, v1, v2, edgeLabel)).thenThrow(new RuntimeException("Unique property constraint violated"));
        graphHelperInstance.getOrCreateEdge(v1, v2, edgeLabel);
    }

    @Test
    public void testGetOrCreateEdgeLabelWithRetries() throws Exception {
        final String edgeLabel = "testLabel";
        TitanVertex v1 = mock(TitanVertex.class);
        TitanVertex v2 = mock(TitanVertex.class);
        Edge edge = mock(Edge.class);

        Iterable noEdgesIterable = new Iterable<Edge>() {
            @Override
            public Iterator<Edge> iterator() {
                return new Iterator<Edge>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Edge next() {
                        return null;
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
        };
        when(v2.getEdges(Direction.IN)).thenReturn(noEdgesIterable);
        when(v1.getEdges(Direction.OUT)).thenReturn(noEdgesIterable);

        when(v1.getId()).thenReturn(new String("v1"));
        when(v2.getId()).thenReturn(new String("v2"));
        when(edge.getId()).thenReturn(new String("edge"));
        when(graph.addEdge(null, v1, v2, edgeLabel))
                .thenThrow(new RuntimeException("Unique property constraint violated")).thenReturn(edge);
        Edge redge = graphHelperInstance.getOrCreateEdge(v1, v2, edgeLabel);
        assertEquals(edge, redge);
    }
}
