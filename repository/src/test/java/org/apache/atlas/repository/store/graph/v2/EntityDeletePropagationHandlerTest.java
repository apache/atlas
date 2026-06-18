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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.DeletePropagationTarget;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class EntityDeletePropagationHandlerTest {

    @Mock private AtlasTypeRegistry typeRegistry;

    private EntityDeletePropagationHandler handler;

    private MockedStatic<GraphHelper>       graphHelperMock;
    private MockedStatic<AtlasGraphUtilsV2> graphUtilsMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new EntityDeletePropagationHandler(typeRegistry);
    }

    @AfterMethod
    public void tearDown() {
        if (graphHelperMock != null) {
            graphHelperMock.close();
            graphHelperMock = null;
        }
        if (graphUtilsMock != null) {
            graphUtilsMock.close();
            graphUtilsMock = null;
        }
    }

    @Test
    public void testNoPropagationTargets_returnsEmpty() {
        AtlasEntityType entityType = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex  = mock(AtlasVertex.class);

        when(entityType.getDeletePropagationTargets()).thenReturn(Collections.emptyList());

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, entityType, visitedGuids);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testNullRelAttr_skipped() {
        openStaticMocks();

        DeletePropagationTarget target = mock(DeletePropagationTarget.class);
        when(target.getRelAttr()).thenReturn(null);

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex  = mock(AtlasVertex.class);
        when(entityType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target));

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, entityType, visitedGuids);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testNoEdges_returnsEmpty() {
        openStaticMocks();

        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__trino_table.schema");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target = mock(DeletePropagationTarget.class);
        when(target.getRelAttr()).thenReturn(relAttr);

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex  = mock(AtlasVertex.class);
        when(entityType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target));

        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.<AtlasEdge>emptyList().iterator());

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, entityType, visitedGuids);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testDeletedEdge_skipped() {
        openStaticMocks();

        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__trino_table.schema");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target = mock(DeletePropagationTarget.class);
        when(target.getRelAttr()).thenReturn(relAttr);

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex  = mock(AtlasVertex.class);
        AtlasEdge       edge       = mock(AtlasEdge.class);
        when(entityType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target));

        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge)).thenReturn(AtlasEntity.Status.DELETED);

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, entityType, visitedGuids);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testDeletedTargetVertex_skipped() {
        openStaticMocks();

        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__trino_table.schema");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target = mock(DeletePropagationTarget.class);
        when(target.getRelAttr()).thenReturn(relAttr);

        AtlasEntityType entityType   = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex    = mock(AtlasVertex.class);
        AtlasVertex     targetVertex = mock(AtlasVertex.class);
        AtlasEdge       edge         = mock(AtlasEdge.class);

        when(entityType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target));
        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(targetVertex);

        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(targetVertex)).thenReturn(AtlasEntity.Status.DELETED);

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, entityType, visitedGuids);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testAlreadyVisitedGuid_skipped() {
        openStaticMocks();

        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__trino_table.schema");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target = mock(DeletePropagationTarget.class);
        when(target.getRelAttr()).thenReturn(relAttr);

        AtlasEntityType entityType   = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex    = mock(AtlasVertex.class);
        AtlasVertex     targetVertex = mock(AtlasVertex.class);
        AtlasEdge       edge         = mock(AtlasEdge.class);

        when(entityType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target));
        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(targetVertex);

        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(targetVertex)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(targetVertex)).thenReturn("already-visited-guid");

        Set<String> visitedGuids = new HashSet<>();
        visitedGuids.add("already-visited-guid");

        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, entityType, visitedGuids);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testSingleTargetPropagated() {
        openStaticMocks();

        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__trino_table.schema");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target = mock(DeletePropagationTarget.class);
        when(target.getRelAttr()).thenReturn(relAttr);

        AtlasEntityType sourceType   = mock(AtlasEntityType.class);
        AtlasEntityType targetType   = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex    = mock(AtlasVertex.class);
        AtlasVertex     targetVertex = mock(AtlasVertex.class);
        AtlasEdge       edge         = mock(AtlasEdge.class);

        when(sourceType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target));
        when(sourceType.getTypeName()).thenReturn("trino_schema");
        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(targetVertex);

        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(targetVertex)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getTypeName(targetVertex)).thenReturn("trino_table");
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(targetVertex)).thenReturn("target-guid-001");

        when(typeRegistry.getEntityTypeByName("trino_table")).thenReturn(targetType);
        when(targetType.getDeletePropagationTargets()).thenReturn(Collections.emptyList());

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, sourceType, visitedGuids);

        assertEquals(result.size(), 1);
        assertTrue(result.contains(targetVertex));
        assertTrue(visitedGuids.contains("target-guid-001"));
    }

    @Test
    public void testRecursivePropagation() {
        openStaticMocks();

        // Level 1: trino_schema -> trino_table
        AtlasAttribute relAttr1 = mock(AtlasAttribute.class);
        when(relAttr1.getRelationshipEdgeLabel()).thenReturn("__trino_table.schema");
        when(relAttr1.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target1 = mock(DeletePropagationTarget.class);
        when(target1.getRelAttr()).thenReturn(relAttr1);

        // Level 2: trino_table -> trino_column
        AtlasAttribute relAttr2 = mock(AtlasAttribute.class);
        when(relAttr2.getRelationshipEdgeLabel()).thenReturn("__trino_column.table");
        when(relAttr2.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target2 = mock(DeletePropagationTarget.class);
        when(target2.getRelAttr()).thenReturn(relAttr2);

        AtlasEntityType schemaType  = mock(AtlasEntityType.class);
        AtlasEntityType tableType   = mock(AtlasEntityType.class);
        AtlasEntityType columnType  = mock(AtlasEntityType.class);
        AtlasVertex     schemaVertex = mock(AtlasVertex.class);
        AtlasVertex     tableVertex  = mock(AtlasVertex.class);
        AtlasVertex     columnVertex = mock(AtlasVertex.class);
        AtlasEdge       edge1        = mock(AtlasEdge.class);
        AtlasEdge       edge2        = mock(AtlasEdge.class);

        when(schemaType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target1));
        when(schemaType.getTypeName()).thenReturn("trino_schema");
        when(tableType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target2));
        when(tableType.getTypeName()).thenReturn("trino_table");
        when(columnType.getDeletePropagationTargets()).thenReturn(Collections.emptyList());

        // Edge from schema -> table
        when(edge1.getOutVertex()).thenReturn(schemaVertex);
        when(edge1.getInVertex()).thenReturn(tableVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(schemaVertex), eq("__trino_table.schema"), any()))
                .thenReturn(Collections.singletonList(edge1).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge1)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(tableVertex)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getTypeName(tableVertex)).thenReturn("trino_table");
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(tableVertex)).thenReturn("table-guid");

        // Edge from table -> column
        when(edge2.getOutVertex()).thenReturn(tableVertex);
        when(edge2.getInVertex()).thenReturn(columnVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(tableVertex), eq("__trino_column.table"), any()))
                .thenReturn(Collections.singletonList(edge2).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge2)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(columnVertex)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getTypeName(columnVertex)).thenReturn("trino_column");
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(columnVertex)).thenReturn("column-guid");

        when(typeRegistry.getEntityTypeByName("trino_table")).thenReturn(tableType);
        when(typeRegistry.getEntityTypeByName("trino_column")).thenReturn(columnType);

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(schemaVertex, schemaType, visitedGuids);

        assertEquals(result.size(), 2);
        assertTrue(result.contains(tableVertex));
        assertTrue(result.contains(columnVertex));
    }

    @Test
    public void testCyclePrevention() {
        openStaticMocks();

        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__cyclic_edge");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target = mock(DeletePropagationTarget.class);
        when(target.getRelAttr()).thenReturn(relAttr);

        AtlasEntityType entityType = mock(AtlasEntityType.class);
        AtlasVertex     vertexA    = mock(AtlasVertex.class);
        AtlasVertex     vertexB    = mock(AtlasVertex.class);
        AtlasEdge       edgeAtoB   = mock(AtlasEdge.class);
        AtlasEdge       edgeBtoA   = mock(AtlasEdge.class);

        when(entityType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target));
        when(entityType.getTypeName()).thenReturn("cyclic_type");

        // A -> B
        when(edgeAtoB.getOutVertex()).thenReturn(vertexA);
        when(edgeAtoB.getInVertex()).thenReturn(vertexB);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(vertexA), anyString(), any()))
                .thenReturn(Collections.singletonList(edgeAtoB).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edgeAtoB)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(vertexB)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getTypeName(vertexB)).thenReturn("cyclic_type");
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(vertexB)).thenReturn("guid-B");

        // B -> A (cycle): vertexA guid is already in visitedGuids since it's the source
        when(edgeBtoA.getOutVertex()).thenReturn(vertexB);
        when(edgeBtoA.getInVertex()).thenReturn(vertexA);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(vertexB), anyString(), any()))
                .thenReturn(Collections.singletonList(edgeBtoA).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edgeBtoA)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(vertexA)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(vertexA)).thenReturn("guid-A");

        when(typeRegistry.getEntityTypeByName("cyclic_type")).thenReturn(entityType);

        Set<String> visitedGuids = new HashSet<>();
        visitedGuids.add("guid-A");

        Set<AtlasVertex> result = handler.collectPropagatedVertices(vertexA, entityType, visitedGuids);

        assertEquals(result.size(), 1);
        assertTrue(result.contains(vertexB));
    }

    @Test
    public void testMultipleTargetsFromSameSource() {
        openStaticMocks();

        AtlasAttribute relAttr1 = mock(AtlasAttribute.class);
        when(relAttr1.getRelationshipEdgeLabel()).thenReturn("__edge_label_1");
        when(relAttr1.getRelationshipEdgeDirection()).thenReturn(null);

        AtlasAttribute relAttr2 = mock(AtlasAttribute.class);
        when(relAttr2.getRelationshipEdgeLabel()).thenReturn("__edge_label_2");
        when(relAttr2.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target1 = mock(DeletePropagationTarget.class);
        when(target1.getRelAttr()).thenReturn(relAttr1);

        DeletePropagationTarget target2 = mock(DeletePropagationTarget.class);
        when(target2.getRelAttr()).thenReturn(relAttr2);

        AtlasEntityType sourceType  = mock(AtlasEntityType.class);
        AtlasEntityType targetType1 = mock(AtlasEntityType.class);
        AtlasEntityType targetType2 = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex   = mock(AtlasVertex.class);
        AtlasVertex     vertex1     = mock(AtlasVertex.class);
        AtlasVertex     vertex2     = mock(AtlasVertex.class);
        AtlasEdge       edge1       = mock(AtlasEdge.class);
        AtlasEdge       edge2       = mock(AtlasEdge.class);

        when(sourceType.getDeletePropagationTargets()).thenReturn(Arrays.asList(target1, target2));
        when(sourceType.getTypeName()).thenReturn("source_type");

        when(edge1.getOutVertex()).thenReturn(srcVertex);
        when(edge1.getInVertex()).thenReturn(vertex1);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), eq("__edge_label_1"), any()))
                .thenReturn(Collections.singletonList(edge1).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge1)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(vertex1)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getTypeName(vertex1)).thenReturn("target_type_1");
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(vertex1)).thenReturn("guid-1");

        when(edge2.getOutVertex()).thenReturn(srcVertex);
        when(edge2.getInVertex()).thenReturn(vertex2);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), eq("__edge_label_2"), any()))
                .thenReturn(Collections.singletonList(edge2).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge2)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(vertex2)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getTypeName(vertex2)).thenReturn("target_type_2");
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(vertex2)).thenReturn("guid-2");

        when(typeRegistry.getEntityTypeByName("target_type_1")).thenReturn(targetType1);
        when(typeRegistry.getEntityTypeByName("target_type_2")).thenReturn(targetType2);
        when(targetType1.getDeletePropagationTargets()).thenReturn(Collections.emptyList());
        when(targetType2.getDeletePropagationTargets()).thenReturn(Collections.emptyList());

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, sourceType, visitedGuids);

        assertEquals(result.size(), 2);
        assertTrue(result.contains(vertex1));
        assertTrue(result.contains(vertex2));
    }

    @Test
    public void testNullTargetEntityType_skipped() {
        openStaticMocks();

        AtlasAttribute relAttr = mock(AtlasAttribute.class);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__edge_label");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        DeletePropagationTarget target = mock(DeletePropagationTarget.class);
        when(target.getRelAttr()).thenReturn(relAttr);

        AtlasEntityType sourceType   = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex    = mock(AtlasVertex.class);
        AtlasVertex     targetVertex = mock(AtlasVertex.class);
        AtlasEdge       edge         = mock(AtlasEdge.class);

        when(sourceType.getDeletePropagationTargets()).thenReturn(Collections.singletonList(target));
        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(targetVertex);

        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());
        graphHelperMock.when(() -> GraphHelper.getStatus(edge)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getStatus(targetVertex)).thenReturn(AtlasEntity.Status.ACTIVE);
        graphHelperMock.when(() -> GraphHelper.getTypeName(targetVertex)).thenReturn("unknown_type");
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(targetVertex)).thenReturn("unknown-guid");

        when(typeRegistry.getEntityTypeByName("unknown_type")).thenReturn(null);

        Set<String> visitedGuids = new HashSet<>();
        Set<AtlasVertex> result = handler.collectPropagatedVertices(srcVertex, sourceType, visitedGuids);

        assertTrue(result.isEmpty());
    }

    private void openStaticMocks() {
        graphHelperMock = mockStatic(GraphHelper.class);
        graphUtilsMock  = mockStatic(AtlasGraphUtilsV2.class);
    }
}
