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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class SuperTypesUpdatePatchTest {
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
    private AtlasVertex vertex;

    @Mock
    private AtlasEntityType entityType;

    @Mock
    private WorkItemManager<Long, ?> workItemManager;

    private SuperTypesUpdatePatch patch;

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

        patch = new SuperTypesUpdatePatch(patchContext, "TEST_TYPEDEF_001", "TestEntity");
    }

    @Test
    public void testConstructor() {
        assertNotNull(patch);
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_007_TEST_TYPEDEF_001");
    }

    @Test
    public void testApply() throws AtlasBaseException {
        // Mock the processor behavior
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.emptySet());

        patch.apply();

        assertEquals(patch.getStatus(), APPLIED);
    }

    @Test
    public void testSuperTypesUpdatePatchProcessorConstructor() {
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        Set<String> typeAndSubTypes = new HashSet<>();
        typeAndSubTypes.add("TestEntity");
        typeAndSubTypes.add("SubTestEntity");
        when(entityType.getTypeAndAllSubTypes()).thenReturn(typeAndSubTypes);

        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "TestEntity");
        assertNotNull(processor);
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
        assertEquals(processor.getIndexer(), indexer);
        assertEquals(processor.getEntityGraphMapper(), entityGraphMapper);
    }

    @Test
    public void testSuperTypesUpdatePatchProcessorConstructorWithNullEntityType() {
        when(typeRegistry.getEntityTypeByName("NonExistentEntity")).thenReturn(null);

        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "NonExistentEntity");
        assertNotNull(processor);
    }

    @Test
    public void testPrepareForExecution() {
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.emptySet());

        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "TestEntity");
        // prepareForExecution does nothing, so just verify it doesn't throw
        processor.prepareForExecution();
    }

    @Test
    public void testSubmitVerticesToUpdateWithEmptyTypes() {
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.emptySet());

        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "TestEntity");
        processor.submitVerticesToUpdate(workItemManager);

        // Should not produce any work items when no types are available
        verify(workItemManager, times(0)).checkProduce(any());
    }

    @Test
    public void testSubmitVerticesToUpdateWithTypes() {
        Set<String> typeAndSubTypes = new HashSet<>();
        typeAndSubTypes.add("TestEntity");
        typeAndSubTypes.add("SubTestEntity");

        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(typeAndSubTypes);
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("TestEntity"))).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("SubTestEntity"))).thenReturn(query);
        when(query.vertexIds()).thenReturn(Arrays.asList(1L, 2L, 3L));

        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "TestEntity");
        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(6)).checkProduce(any()); // 3 vertices * 2 types
    }

    @Test
    public void testSubmitVerticesToUpdateWithSingleType() {
        Set<String> typeAndSubTypes = new HashSet<>();
        typeAndSubTypes.add("TestEntity");

        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(typeAndSubTypes);
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("TestEntity"))).thenReturn(query);
        when(query.vertexIds()).thenReturn(Arrays.asList(1L, 2L));

        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "TestEntity");
        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(2)).checkProduce(any());
    }

    @Test
    public void testProcessVertexItem() {
        Long vertexId = 123L;
        String typeName = "TestEntity";

        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.singleton("TestEntity"));
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("SuperType"));
        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "TestEntity");

        processor.processVertexItem(vertexId, vertex, typeName, entityType);
    }

    @Test
    public void testProcessVertexItemWithNoSuperTypes() {
        Long vertexId = 123L;
        String typeName = "TestEntity";

        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.singleton("TestEntity"));
        when(entityType.getAllSuperTypes()).thenReturn(Collections.emptySet());

        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "TestEntity");
        processor.processVertexItem(vertexId, vertex, typeName, entityType);
    }

    @Test
    public void testProcessVertexItemWithMultipleSuperTypes() {
        Long vertexId = 123L;
        String typeName = "TestEntity";

        Set<String> superTypes = new HashSet<>();
        superTypes.add("SuperType1");
        superTypes.add("SuperType2");
        superTypes.add("SuperType3");

        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.singleton("TestEntity"));
        when(entityType.getAllSuperTypes()).thenReturn(superTypes);

        SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor processor = new SuperTypesUpdatePatch.SuperTypesUpdatePatchProcessor(patchContext, "TestEntity");
        processor.processVertexItem(vertexId, vertex, typeName, entityType);
    }
}
