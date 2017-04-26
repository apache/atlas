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

import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Iterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class GraphHelperMockTest {

    private GraphHelper graphHelperInstance;

    private AtlasGraph graph;

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
        graph = mock(AtlasGraph.class);
        graphHelperInstance = GraphHelper.getInstance(graph);
    }

    @Test(expectedExceptions = RepositoryException.class)
    public void testGetOrCreateEdgeLabelWithMaxRetries() throws Exception {
        final String edgeLabel = "testLabel";
        AtlasVertex v1 = mock(AtlasVertex.class);
        AtlasVertex v2 = mock(AtlasVertex.class);

        Iterable noEdgesIterable = new Iterable<AtlasEdge>() {
            @Override
            public Iterator<AtlasEdge> iterator() {
                return new Iterator<AtlasEdge>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public AtlasEdge next() {
                        return null;
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
        };
        when(v2.getEdges(AtlasEdgeDirection.IN)).thenReturn(noEdgesIterable);
        when(v1.getEdges(AtlasEdgeDirection.OUT)).thenReturn(noEdgesIterable);

        when(v1.getId()).thenReturn("1234");
        when(v2.getId()).thenReturn("5678");
        when(graph.addEdge(v1, v2, edgeLabel)).thenThrow(new RuntimeException("Unique property constraint violated"));
        graphHelperInstance.getOrCreateEdge(v1, v2, edgeLabel);
    }

    @Test
    public void testGetOrCreateEdgeLabelWithRetries() throws Exception {
        final String edgeLabel = "testLabel";
        AtlasVertex v1 = mock(AtlasVertex.class);
        AtlasVertex v2 = mock(AtlasVertex.class);
        AtlasEdge edge = mock(AtlasEdge.class);

        Iterable noEdgesIterable = new Iterable<AtlasEdge>() {
            @Override
            public Iterator<AtlasEdge> iterator() {
                return new Iterator<AtlasEdge>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public AtlasEdge next() {
                        return null;
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
        };
        when(v2.getEdges(AtlasEdgeDirection.IN)).thenReturn(noEdgesIterable);
        when(v1.getEdges(AtlasEdgeDirection.OUT)).thenReturn(noEdgesIterable);

        when(v1.getId()).thenReturn("v1");
        when(v2.getId()).thenReturn("v2");
        when(edge.getId()).thenReturn("edge");
        when(graph.addEdge(v1, v2, edgeLabel))
                .thenThrow(new RuntimeException("Unique property constraint violated")).thenReturn(edge);
        AtlasEdge redge = graphHelperInstance.getOrCreateEdge(v1, v2, edgeLabel);
        assertEquals(edge, redge);
    }
}
