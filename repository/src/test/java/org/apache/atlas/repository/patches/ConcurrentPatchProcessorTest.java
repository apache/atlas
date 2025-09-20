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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasEntityType;
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

public class ConcurrentPatchProcessorTest {
    @Mock
    private PatchContext patchContext;

    @Mock
    private AtlasGraph graph;

    @Mock
    private GraphBackedSearchIndexer indexer;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private EntityGraphMapper entityGraphMapper;

    @Mock
    private WorkItemManager<Long, ?> workItemManager;

    @Mock
    private AtlasVertex vertex;

    @Mock
    private AtlasEntityType entityType;

    private TestConcurrentPatchProcessor processor;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(patchContext.getGraph()).thenReturn(graph);
        when(patchContext.getIndexer()).thenReturn(indexer);
        when(patchContext.getTypeRegistry()).thenReturn(typeRegistry);
        when(patchContext.getEntityGraphMapper()).thenReturn(entityGraphMapper);

        processor = new TestConcurrentPatchProcessor(patchContext);
    }

    @Test
    public void testConstructor() {
        assertNotNull(processor);
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getIndexer(), indexer);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
        assertEquals(processor.getEntityGraphMapper(), entityGraphMapper);
    }

    @Test
    public void testGetters() {
        assertEquals(processor.getEntityGraphMapper(), entityGraphMapper);
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
    public void testProcessVertexItemSuccess() throws AtlasBaseException {
        Long vertexId = 123L;
        String typeName = "TestType";

        processor.processVertexItem(vertexId, vertex, typeName, entityType);

        assertTrue(processor.processVertexItemCalled);
        assertEquals(processor.lastVertexId, vertexId);
        assertEquals(processor.lastVertex, vertex);
        assertEquals(processor.lastTypeName, typeName);
        assertEquals(processor.lastEntityType, entityType);
    }

    @Test
    public void testProcessVertexItemWithException() throws AtlasBaseException {
        processor.shouldThrowInProcessVertexItem = true;
        Long vertexId = 123L;
        String typeName = "TestType";

        try {
            processor.processVertexItem(vertexId, vertex, typeName, entityType);
        } catch (AtlasBaseException e) {
            assertEquals(e.getMessage(), "ProcessVertexItem failed");
        }

        assertTrue(processor.processVertexItemCalled);
    }

    @Test
    public void testStaticFields() throws Exception {
        Field numWorkersField = ConcurrentPatchProcessor.class.getDeclaredField("NUM_WORKERS");
        Field batchSizeField = ConcurrentPatchProcessor.class.getDeclaredField("BATCH_SIZE");

        numWorkersField.setAccessible(true);
        batchSizeField.setAccessible(true);

        int numWorkers = (Integer) numWorkersField.get(null);
        int batchSize = (Integer) batchSizeField.get(null);

        assertTrue(numWorkers > 0);
        assertTrue(batchSize > 0);
    }

    // Test implementation of abstract ConcurrentPatchProcessor
    private static class TestConcurrentPatchProcessor extends ConcurrentPatchProcessor {
        boolean prepareForExecutionCalled;
        boolean executeCalled;
        boolean processVertexItemCalled;
        boolean shouldThrowInPrepare;
        boolean shouldThrowInProcessVertexItem;

        Long lastVertexId;
        AtlasVertex lastVertex;
        String lastTypeName;
        AtlasEntityType lastEntityType;

        public TestConcurrentPatchProcessor(PatchContext context) {
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
        protected void submitVerticesToUpdate(WorkItemManager manager) {
            executeCalled = true;
            // Mock implementation - just add a few test vertex IDs
            manager.checkProduce(1L);
            manager.checkProduce(2L);
            manager.checkProduce(3L);
        }

        @Override
        protected void processVertexItem(Long vertexId, AtlasVertex vertex, String typeName, AtlasEntityType entityType) throws AtlasBaseException {
            processVertexItemCalled = true;
            lastVertexId = vertexId;
            lastVertex = vertex;
            lastTypeName = typeName;
            lastEntityType = entityType;

            if (shouldThrowInProcessVertexItem) {
                throw new AtlasBaseException("ProcessVertexItem failed");
            }
        }
    }
}
