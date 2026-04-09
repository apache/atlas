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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class RelationshipTypeNamePatchTest {
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
    private AtlasEdge edge1;

    @Mock
    private AtlasEdge edge2;

    @Mock
    private AtlasEdge edge3;

    @Mock
    private AtlasRelationshipType relationshipType;

    @Mock
    private WorkItemManager<String, ?> workItemManager;

    private RelationshipTypeNamePatch patch;
    private MockedStatic<AtlasConfiguration> atlasConfigurationMock;

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

        atlasConfigurationMock = mockStatic(AtlasConfiguration.class);
        patch = new RelationshipTypeNamePatch(patchContext);
    }

    @AfterMethod
    public void tearDown() {
        if (atlasConfigurationMock != null) {
            atlasConfigurationMock.close();
        }
    }

    @Test
    public void testConstructor() {
        assertNotNull(patch);
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_0011");
    }

    @Test
    public void testApplyWhenRelationshipSearchDisabled() throws AtlasBaseException {
        patch.apply();

        verify(graph, never()).getEdges();
    }

    @Test
    public void testRelationshipTypeNamePatchProcessorConstructor() {
        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);

        assertNotNull(processor);
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
        assertEquals(processor.getIndexer(), indexer);
    }

    @Test
    public void testPrepareForExecution() {
        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);
        processor.prepareForExecution();
    }

    @Test
    public void testSubmitEdgesToUpdateWithNoEdges() {
        when(graph.getEdges()).thenReturn(Collections.emptyList());

        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);

        processor.submitEdgesToUpdate(workItemManager);

        verify(workItemManager, times(0)).checkProduce(any());
    }

    @Test
    public void testSubmitEdgesToUpdateWithEdgesHavingTypeName() {
        when(graph.getEdges()).thenReturn(Arrays.asList(edge1, edge2, edge3));

        when(edge1.getProperty(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq(String.class))).thenReturn("relationship_type_1");
        when(edge2.getProperty(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq(String.class))).thenReturn("relationship_type_2");
        when(edge3.getProperty(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq(String.class))).thenReturn(null);

        when(edge1.getId()).thenReturn("edge1");
        when(edge2.getId()).thenReturn("edge2");

        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);

        processor.submitEdgesToUpdate(workItemManager);

        // Should only produce work items for edges with non-null typeName
        verify(workItemManager, times(2)).checkProduce(any());
        verify(workItemManager, times(1)).checkProduce("edge1");
        verify(workItemManager, times(1)).checkProduce("edge2");
    }

    @Test
    public void testSubmitEdgesToUpdateWithAllEdgesHavingNullTypeName() {
        when(graph.getEdges()).thenReturn(Arrays.asList(edge1, edge2, edge3));

        when(edge1.getProperty(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq(String.class))).thenReturn(null);
        when(edge2.getProperty(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq(String.class))).thenReturn(null);
        when(edge3.getProperty(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq(String.class))).thenReturn(null);

        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);
        processor.submitEdgesToUpdate(workItemManager);

        // Should not produce any work items when all edges have null typeName
        verify(workItemManager, times(0)).checkProduce(any());
    }

    @Test
    public void testProcessEdgesItem() {
        String edgeId = "edge123";
        String typeName = "test_relationship_type";
        doNothing().when(edge1).setProperty(eq(Constants.RELATIONSHIP_TYPE_PROPERTY_KEY), eq(typeName));
        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);
        processor.processEdgesItem(edgeId, edge1, typeName, relationshipType);
        verify(edge1, times(1)).setProperty(Constants.RELATIONSHIP_TYPE_PROPERTY_KEY, typeName);
    }

    @Test
    public void testProcessEdgesItemWithDifferentTypeName() {
        String edgeId = "edge456";
        String typeName = "another_relationship_type";

        doNothing().when(edge2).setProperty(eq(Constants.RELATIONSHIP_TYPE_PROPERTY_KEY), eq(typeName));

        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);

        processor.processEdgesItem(edgeId, edge2, typeName, relationshipType);

        verify(edge2, times(1)).setProperty(Constants.RELATIONSHIP_TYPE_PROPERTY_KEY, typeName);
    }

    @Test
    public void testProcessEdgesItemWithNullTypeName() {
        String edgeId = "edge789";
        String typeName = null;

        doNothing().when(edge3).setProperty(eq(Constants.RELATIONSHIP_TYPE_PROPERTY_KEY), eq(typeName));

        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);

        processor.processEdgesItem(edgeId, edge3, typeName, relationshipType);

        verify(edge3, times(1)).setProperty(Constants.RELATIONSHIP_TYPE_PROPERTY_KEY, null);
    }

    @Test
    public void testProcessEdgesItemWithEmptyTypeName() {
        String edgeId = "edge000";
        String typeName = "";

        doNothing().when(edge1).setProperty(eq(Constants.RELATIONSHIP_TYPE_PROPERTY_KEY), eq(typeName));

        RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor processor = new RelationshipTypeNamePatch.RelationshipTypeNamePatchProcessor(patchContext);

        processor.processEdgesItem(edgeId, edge1, typeName, relationshipType);

        verify(edge1, times(1)).setProperty(Constants.RELATIONSHIP_TYPE_PROPERTY_KEY, "");
    }
}
