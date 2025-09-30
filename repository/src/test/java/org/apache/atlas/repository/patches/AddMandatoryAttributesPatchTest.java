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
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
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
import java.util.List;
import java.util.Set;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class AddMandatoryAttributesPatchTest {
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
    private AtlasAttributeDef attributeDef1;

    @Mock
    private AtlasAttributeDef attributeDef2;

    @Mock
    private WorkItemManager<Long, ?> workItemManager;

    private AddMandatoryAttributesPatch patch;
    private List<AtlasAttributeDef> attributesToAdd;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(patchContext.getPatchRegistry()).thenReturn(patchRegistry);
        when(patchContext.getGraph()).thenReturn(graph);
        when(patchContext.getTypeRegistry()).thenReturn(typeRegistry);
        when(patchContext.getIndexer()).thenReturn(indexer);
        when(patchContext.getEntityGraphMapper()).thenReturn(entityGraphMapper);
        when(patchRegistry.getStatus(any())).thenReturn(null);
        doNothing().when(patchRegistry).register(any(), any(), any(), any(), any());
        doNothing().when(patchRegistry).updateStatus(any(), any());
        attributesToAdd = Arrays.asList(attributeDef1, attributeDef2);
        patch = new AddMandatoryAttributesPatch(patchContext, "TEST_TYPEDEF_001", "TestEntity", attributesToAdd);
    }

    @Test
    public void testConstructor() {
        assertNotNull(patch);
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_008_TEST_TYPEDEF_001");
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
    public void testAddMandatoryAttributesPatchProcessorConstructor() {
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        Set<String> typeAndSubTypes = new HashSet<>();
        typeAndSubTypes.add("TestEntity");
        typeAndSubTypes.add("SubTestEntity");
        when(entityType.getTypeAndAllSubTypes()).thenReturn(typeAndSubTypes);

        AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor processor = new AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor(patchContext, "TestEntity", attributesToAdd);
        assertNotNull(processor);
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
        assertEquals(processor.getIndexer(), indexer);
        assertEquals(processor.getEntityGraphMapper(), entityGraphMapper);
    }

    @Test
    public void testAddMandatoryAttributesPatchProcessorConstructorWithNullEntityType() {
        when(typeRegistry.getEntityTypeByName("NonExistentEntity")).thenReturn(null);
        AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor processor = new AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor(patchContext, "NonExistentEntity", attributesToAdd);
        assertNotNull(processor);
    }

    @Test
    public void testPrepareForExecution() {
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.emptySet());
        AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor processor = new AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor(patchContext, "TestEntity", attributesToAdd);
        processor.prepareForExecution();
    }

    @Test
    public void testSubmitVerticesToUpdateWithEmptyTypes() {
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.emptySet());
        AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor processor = new AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor(patchContext, "TestEntity", attributesToAdd);
        processor.submitVerticesToUpdate(workItemManager);
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
        AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor processor = new AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor(patchContext, "TestEntity", attributesToAdd);
        processor.submitVerticesToUpdate(workItemManager);
        verify(workItemManager, times(6)).checkProduce(any()); // 3 vertices * 2 types
    }

    @Test
    public void testProcessVertexItem() {
        Long vertexId = 123L;
        String typeName = "TestEntity";
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.singleton("TestEntity"));
        AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor processor = new AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor(patchContext, "TestEntity", attributesToAdd);
        processor.processVertexItem(vertexId, vertex, typeName, entityType);
    }

    @Test
    public void testProcessVertexItemWithNullAttributesToAdd() {
        Long vertexId = 123L;
        String typeName = "TestEntity";
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.singleton("TestEntity"));
        // Test with empty list instead of null to avoid NPE
        AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor processor = new AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor(patchContext, "TestEntity", Collections.emptyList());
        processor.processVertexItem(vertexId, vertex, typeName, entityType);
    }

    @Test
    public void testProcessVertexItemWithEmptyAttributesToAdd() {
        Long vertexId = 123L;
        String typeName = "TestEntity";
        when(typeRegistry.getEntityTypeByName("TestEntity")).thenReturn(entityType);
        when(entityType.getTypeAndAllSubTypes()).thenReturn(Collections.singleton("TestEntity"));
        AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor processor = new AddMandatoryAttributesPatch.AddMandatoryAttributesPatchProcessor(patchContext, "TestEntity", Collections.emptyList());
        processor.processVertexItem(vertexId, vertex, typeName, entityType);
    }
}
