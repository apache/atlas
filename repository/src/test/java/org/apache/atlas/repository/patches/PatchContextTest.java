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

package org.apache.atlas.repository.patches;

import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class PatchContextTest {
    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private GraphBackedSearchIndexer indexer;

    @Mock
    private EntityGraphMapper entityGraphMapper;

    @Mock
    private AtlasGraphQuery graphQuery;

    @Mock
    private AtlasGraphQuery childQuery;

    private PatchContext patchContext;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        // Mock the graph query chain for AtlasPatchRegistry
        when(graph.query()).thenReturn(graphQuery);
        when(graphQuery.createChildQuery()).thenReturn(childQuery);
        when(childQuery.has(any(), any(), any())).thenReturn(childQuery);
        when(graphQuery.has(any(), any())).thenReturn(graphQuery);
        when(graphQuery.or(any())).thenReturn(graphQuery);
        when(graphQuery.vertices()).thenReturn(new ArrayList<AtlasVertex>());
        // Mock vertex creation for createOrUpdatePatchVertex
        AtlasVertex mockVertex = mock(AtlasVertex.class);
        when(graph.addVertex()).thenReturn(mockVertex);
        patchContext = new PatchContext(graph, typeRegistry, indexer, entityGraphMapper);
    }

    @Test
    public void testConstructor() {
        assertNotNull(patchContext);
        assertNotNull(patchContext.getGraph());
        assertNotNull(patchContext.getTypeRegistry());
        assertNotNull(patchContext.getIndexer());
        assertNotNull(patchContext.getEntityGraphMapper());
        assertNotNull(patchContext.getPatchRegistry());
    }

    @Test
    public void testGetGraph() {
        assertEquals(patchContext.getGraph(), graph);
    }

    @Test
    public void testGetTypeRegistry() {
        assertEquals(patchContext.getTypeRegistry(), typeRegistry);
    }

    @Test
    public void testGetIndexer() {
        assertEquals(patchContext.getIndexer(), indexer);
    }

    @Test
    public void testGetEntityGraphMapper() {
        assertEquals(patchContext.getEntityGraphMapper(), entityGraphMapper);
    }

    @Test
    public void testGetPatchRegistry() {
        AtlasPatchRegistry patchRegistry = patchContext.getPatchRegistry();
        assertNotNull(patchRegistry);
    }
}
