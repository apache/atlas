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
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ClassificationTextPatchTest {
    @Mock
    private PatchContext patchContext;

    @Mock
    private AtlasPatchRegistry patchRegistry;

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private GraphBackedSearchIndexer indexer;

    @Mock
    private EntityGraphMapper entityGraphMapper;

    @Mock
    private AtlasGraphQuery query;

    @Mock
    private AtlasVertex classificationVertex;

    @Mock
    private AtlasVertex entityVertex;

    @Mock
    private AtlasEdge edge;

    @Mock
    private AtlasClassificationType classificationType;

    @Mock
    private AtlasEntityType entityType;

    @Mock
    private WorkItemManager<Long, ?> workItemManager;

    private ClassificationTextPatch patch;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(patchContext.getPatchRegistry()).thenReturn(patchRegistry);
        when(patchContext.getGraph()).thenReturn(graph);
        when(patchContext.getTypeRegistry()).thenReturn(typeRegistry);
        when(patchContext.getIndexer()).thenReturn(indexer);
        when(patchContext.getEntityGraphMapper()).thenReturn(entityGraphMapper);
        when(patchRegistry.getStatus(any())).thenReturn(null);
        lenient().doNothing().when(patchRegistry).register(any(), any(), any(), any(), any());
        lenient().doNothing().when(patchRegistry).updateStatus(any(), any());
        patch = new ClassificationTextPatch(patchContext);
    }

    @Test
    public void testConstructor() {
        assertNotNull(patch);
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_002");
    }

    @Test
    public void testApply() throws AtlasBaseException {
        // Mock the processor behavior
        when(typeRegistry.getAllClassificationTypes()).thenReturn(Collections.emptyList());
        patch.apply();
        assertEquals(patch.getStatus(), APPLIED);
    }

    @Test
    public void testClassificationTextPatchProcessorConstructor() {
        ClassificationTextPatch.ClassificationTextPatchProcessor processor = new ClassificationTextPatch.ClassificationTextPatchProcessor(patchContext);
        assertNotNull(processor);
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
        assertEquals(processor.getIndexer(), indexer);
        assertEquals(processor.getEntityGraphMapper(), entityGraphMapper);
    }

    @Test
    public void testPrepareForExecution() {
        ClassificationTextPatch.ClassificationTextPatchProcessor processor = new ClassificationTextPatch.ClassificationTextPatchProcessor(patchContext);
        processor.prepareForExecution();
    }

    @Test
    public void testSubmitVerticesToUpdateWithNoClassifications() {
        when(typeRegistry.getAllClassificationTypes()).thenReturn(Collections.emptyList());
        ClassificationTextPatch.ClassificationTextPatchProcessor processor = new ClassificationTextPatch.ClassificationTextPatchProcessor(patchContext);
        processor.submitVerticesToUpdate(workItemManager);

        verify(typeRegistry, times(1)).getAllClassificationTypes();
    }

    @Test
    public void testSubmitVerticesToUpdateWithClassifications() {
        when(typeRegistry.getAllClassificationTypes()).thenReturn(Collections.singletonList(classificationType));
        when(classificationType.getTypeName()).thenReturn("TestClassification");
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("TestClassification"))).thenReturn(query);
        when(query.vertices()).thenReturn(Collections.singletonList(classificationVertex));

        when(classificationVertex.getEdges(eq(AtlasEdgeDirection.IN))).thenReturn(Collections.singletonList(edge));
        when(edge.getOutVertex()).thenReturn(entityVertex);
        when(entityVertex.getId()).thenReturn(123L);
        ClassificationTextPatch.ClassificationTextPatchProcessor processor = new ClassificationTextPatch.ClassificationTextPatchProcessor(patchContext);
        processor.submitVerticesToUpdate(workItemManager);
        verify(workItemManager, times(1)).checkProduce(123L);
    }

    @Test
    public void testSubmitVerticesToUpdateWithMultipleClassifications() {
        AtlasClassificationType classificationType2 = mock(AtlasClassificationType.class);
        when(typeRegistry.getAllClassificationTypes()).thenReturn(Arrays.asList(classificationType, classificationType2));

        when(classificationType.getTypeName()).thenReturn("TestClassification1");
        when(classificationType2.getTypeName()).thenReturn("TestClassification2");
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("TestClassification1"))).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("TestClassification2"))).thenReturn(query);
        when(query.vertices()).thenReturn(Collections.singletonList(classificationVertex));

        when(classificationVertex.getEdges(eq(AtlasEdgeDirection.IN))).thenReturn(Collections.singletonList(edge));
        when(edge.getOutVertex()).thenReturn(entityVertex);
        when(entityVertex.getId()).thenReturn(123L);

        ClassificationTextPatch.ClassificationTextPatchProcessor processor = new ClassificationTextPatch.ClassificationTextPatchProcessor(patchContext);
        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(1)).checkProduce(123L);
    }

    @Test
    public void testSubmitVerticesToUpdateWithDuplicateVertices() {
        when(typeRegistry.getAllClassificationTypes()).thenReturn(Collections.singletonList(classificationType));
        when(classificationType.getTypeName()).thenReturn("TestClassification");
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("TestClassification"))).thenReturn(query);
        AtlasVertex classificationVertex2 = mock(AtlasVertex.class);
        when(query.vertices()).thenReturn(Arrays.asList(classificationVertex, classificationVertex2));

        AtlasEdge edge2 = mock(AtlasEdge.class);
        when(classificationVertex.getEdges(eq(AtlasEdgeDirection.IN))).thenReturn(Collections.singletonList(edge));
        when(classificationVertex2.getEdges(eq(AtlasEdgeDirection.IN))).thenReturn(Collections.singletonList(edge2));
        when(edge.getOutVertex()).thenReturn(entityVertex);
        when(edge2.getOutVertex()).thenReturn(entityVertex); // Same entity vertex
        when(entityVertex.getId()).thenReturn(123L);

        ClassificationTextPatch.ClassificationTextPatchProcessor processor = new ClassificationTextPatch.ClassificationTextPatchProcessor(patchContext);

        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(1)).checkProduce(123L);
    }

    @Test
    public void testProcessVertexItem() throws AtlasBaseException {
        Long vertexId = 123L;
        String typeName = "TestEntity";

        ClassificationTextPatch.ClassificationTextPatchProcessor processor = new ClassificationTextPatch.ClassificationTextPatchProcessor(patchContext);

        processor.processVertexItem(vertexId, entityVertex, typeName, entityType);
    }
}
