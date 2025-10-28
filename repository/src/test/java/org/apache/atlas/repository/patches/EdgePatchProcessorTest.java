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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class EdgePatchProcessorTest {
    @Mock
    private PatchContext patchContext;

    @Mock
    private AtlasGraph graph;

    @Mock
    private GraphBackedSearchIndexer indexer;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private WorkItemManager<String, ?> workItemManager;

    @Mock
    private AtlasEdge edge;

    @Mock
    private AtlasRelationshipType relationshipType;

    private TestEdgePatchProcessor processor;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(patchContext.getGraph()).thenReturn(graph);
        when(patchContext.getIndexer()).thenReturn(indexer);
        when(patchContext.getTypeRegistry()).thenReturn(typeRegistry);

        processor = new TestEdgePatchProcessor(patchContext);
    }

    @Test
    public void testConstructor() {
        assertNotNull(processor);
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getIndexer(), indexer);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
    }

    @Test
    public void testGetters() {
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getIndexer(), indexer);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
    }

    @Test
    public void testApplySuccess() throws AtlasBaseException {
        processor.apply();

        assertTrue(processor.prepareForExecutionCalled);
        assertTrue(processor.executeCalled);
    }

    @Test
    public void testApplyWithPrepareException() throws AtlasBaseException {
        processor.shouldThrowInPrepare = true;

        try {
            processor.apply();
        } catch (AtlasBaseException e) {
            assertEquals(e.getMessage(), "Prepare failed");
        }

        assertTrue(processor.prepareForExecutionCalled);
    }

    @Test
    public void testProcessEdgesItemSuccess() throws AtlasBaseException {
        String edgeId = "edge123";
        String typeName = "TestRelationType";

        processor.processEdgesItem(edgeId, edge, typeName, relationshipType);

        assertTrue(processor.processEdgesItemCalled);
        assertEquals(processor.lastEdgeId, edgeId);
        assertEquals(processor.lastEdge, edge);
        assertEquals(processor.lastTypeName, typeName);
        assertEquals(processor.lastRelationshipType, relationshipType);
    }

    @Test
    public void testProcessEdgesItemWithException() throws AtlasBaseException {
        processor.shouldThrowInProcessEdgesItem = true;
        String edgeId = "edge123";
        String typeName = "TestRelationType";

        try {
            processor.processEdgesItem(edgeId, edge, typeName, relationshipType);
        } catch (AtlasBaseException e) {
            assertEquals(e.getMessage(), "ProcessEdgesItem failed");
        }

        assertTrue(processor.processEdgesItemCalled);
    }

    @Test
    public void testStaticFields() throws Exception {
        Field numWorkersField = EdgePatchProcessor.class.getDeclaredField("NUM_WORKERS");
        Field batchSizeField = EdgePatchProcessor.class.getDeclaredField("BATCH_SIZE");

        numWorkersField.setAccessible(true);
        batchSizeField.setAccessible(true);

        int numWorkers = (Integer) numWorkersField.get(null);
        int batchSize = (Integer) batchSizeField.get(null);

        assertTrue(numWorkers > 0);
        assertTrue(batchSize > 0);
    }

    // Test implementation of abstract EdgePatchProcessor
    private static class TestEdgePatchProcessor extends EdgePatchProcessor {
        boolean prepareForExecutionCalled;
        boolean executeCalled;
        boolean processEdgesItemCalled;
        boolean shouldThrowInPrepare;
        boolean shouldThrowInProcessEdgesItem;
        String lastEdgeId;
        AtlasEdge lastEdge;
        String lastTypeName;
        AtlasRelationshipType lastRelationshipType;

        public TestEdgePatchProcessor(PatchContext context) {
            super(context);
        }

        @Override
        protected void prepareForExecution() throws AtlasBaseException {
            prepareForExecutionCalled = true;
            if (shouldThrowInPrepare) {
                throw new AtlasBaseException("Prepare failed");
            }
        }

        @Override
        protected void submitEdgesToUpdate(WorkItemManager manager) {
            executeCalled = true;
            // Mock implementation - just add a few test edge IDs
            manager.checkProduce("edge1");
            manager.checkProduce("edge2");
            manager.checkProduce("edge3");
        }

        @Override
        protected void processEdgesItem(String edgeId, AtlasEdge edge, String typeName, AtlasRelationshipType type) throws AtlasBaseException {
            processEdgesItemCalled = true;
            lastEdgeId = edgeId;
            lastEdge = edge;
            lastTypeName = typeName;
            lastRelationshipType = type;

            if (shouldThrowInProcessEdgesItem) {
                throw new AtlasBaseException("ProcessEdgesItem failed");
            }
        }
    }
}
