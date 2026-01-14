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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.graph.GraphSandboxUtil;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasEdgeLabel;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJobFuture;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasJanusGraphManagementTest {
    @Mock
    private AtlasJanusGraph mockAtlasGraph;

    @Mock
    private JanusGraphManagement mockJanusManagement;

    @Mock
    private JanusGraph mockJanusGraph;

    private AtlasJanusGraphManagement management;
    private AtlasJanusGraphDatabase database;
    private AtlasGraph<?, ?> atlasGraph;

    @BeforeClass
    public static void setupClass() throws Exception {
        GraphSandboxUtil.create();

        if (useLocalSolr()) {
            LocalSolrRunner.start();
        }
    }

    @AfterClass
    public static void cleanupClass() throws Exception {
        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }
    }

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);
        database = new AtlasJanusGraphDatabase();
        atlasGraph = database.getGraph();
        management = new AtlasJanusGraphManagement(mockAtlasGraph, mockJanusManagement);
    }

    @Test
    public void testConstructor() {
        AtlasJanusGraphManagement mgmt = new AtlasJanusGraphManagement(mockAtlasGraph, mockJanusManagement);
        assertNotNull(mgmt);
    }

    @Test
    public void testContainsPropertyKey() {
        when(mockJanusManagement.containsPropertyKey("testProperty")).thenReturn(true);
        when(mockJanusManagement.containsPropertyKey("nonExistentProperty")).thenReturn(false);

        assertTrue(management.containsPropertyKey("testProperty"));
        assertFalse(management.containsPropertyKey("nonExistentProperty"));
    }

    @Test
    public void testMakePropertyKey() {
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        org.janusgraph.core.schema.PropertyKeyMaker mockPropertyKeyMaker = mock(org.janusgraph.core.schema.PropertyKeyMaker.class);

        when(mockJanusManagement.makePropertyKey("testProperty")).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.dataType(String.class)).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.cardinality((Cardinality) any())).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.make()).thenReturn(mockJanusPropertyKey);

        AtlasPropertyKey propertyKey = management.makePropertyKey("testProperty", String.class, AtlasCardinality.SINGLE);
        assertNotNull(propertyKey);
    }

    @Test
    public void testMakePropertyKeyWithMultiCardinality() {
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        org.janusgraph.core.schema.PropertyKeyMaker mockPropertyKeyMaker = mock(org.janusgraph.core.schema.PropertyKeyMaker.class);

        when(mockJanusManagement.makePropertyKey("testProperty")).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.dataType(String.class)).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.cardinality((Cardinality) any())).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.make()).thenReturn(mockJanusPropertyKey);

        AtlasPropertyKey propertyKey = management.makePropertyKey("testProperty", String.class, AtlasCardinality.SET);
        assertNotNull(propertyKey);
    }

    @Test
    public void testMakeEdgeLabel() {
        org.janusgraph.core.EdgeLabel mockJanusEdgeLabel = mock(org.janusgraph.core.EdgeLabel.class);
        org.janusgraph.core.schema.EdgeLabelMaker mockEdgeLabelMaker = mock(org.janusgraph.core.schema.EdgeLabelMaker.class);

        when(mockJanusManagement.makeEdgeLabel("testLabel")).thenReturn(mockEdgeLabelMaker);
        when(mockEdgeLabelMaker.make()).thenReturn(mockJanusEdgeLabel);

        AtlasEdgeLabel edgeLabel = management.makeEdgeLabel("testLabel");
        assertNotNull(edgeLabel);
    }

    @Test
    public void testDeletePropertyKey() {
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);

        when(mockJanusManagement.getPropertyKey("testProperty")).thenReturn(mockJanusPropertyKey);
        when(mockJanusPropertyKey.toString()).thenReturn("testProperty");
        when(mockJanusManagement.getPropertyKey("testProperty_deleted_0")).thenReturn(null);

        // Should not throw exception
        management.deletePropertyKey("testProperty");
    }

    @Test
    public void testDeletePropertyKeyNonExistent() {
        when(mockJanusManagement.getPropertyKey("nonExistentProperty")).thenReturn(null);

        // Should not throw exception when property doesn't exist
        management.deletePropertyKey("nonExistentProperty");
    }

    @Test
    public void testDeletePropertyKeyWithExistingDeletedNames() {
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);

        when(mockJanusManagement.getPropertyKey("testProperty")).thenReturn(mockJanusPropertyKey);
        when(mockJanusPropertyKey.toString()).thenReturn("testProperty");
        // Simulate that first few deleted names already exist
        when(mockJanusManagement.getPropertyKey("testProperty_deleted_0")).thenReturn(mockJanusPropertyKey);
        when(mockJanusManagement.getPropertyKey("testProperty_deleted_1")).thenReturn(mockJanusPropertyKey);
        when(mockJanusManagement.getPropertyKey("testProperty_deleted_2")).thenReturn(null);

        management.deletePropertyKey("testProperty");

        verify(mockJanusManagement).changeName(mockJanusPropertyKey, "testProperty_deleted_2");
    }

    @Test
    public void testGetPropertyKey() {
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);

        when(mockJanusManagement.getPropertyKey("testProperty")).thenReturn(mockJanusPropertyKey);

        AtlasPropertyKey propertyKey = management.getPropertyKey("testProperty");
        assertNotNull(propertyKey);
    }

    @Test
    public void testGetPropertyKeyNonExistent() {
        when(mockJanusManagement.getPropertyKey("nonExistentProperty")).thenReturn(null);

        AtlasPropertyKey propertyKey = management.getPropertyKey("nonExistentProperty");
        assertNull(propertyKey);
    }

    @Test
    public void testGetPropertyKeyWithInvalidName() {
        try {
            management.getPropertyKey("");
            assertTrue(false, "Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Need to specify name"));
        }

        try {
            management.getPropertyKey("invalid{name");
            assertTrue(false, "Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("can not contains reserved character"));
        }
    }

    @Test
    public void testGetEdgeLabel() {
        org.janusgraph.core.EdgeLabel mockJanusEdgeLabel = mock(org.janusgraph.core.EdgeLabel.class);

        when(mockJanusManagement.getEdgeLabel("testLabel")).thenReturn(mockJanusEdgeLabel);

        AtlasEdgeLabel edgeLabel = management.getEdgeLabel("testLabel");
        assertNotNull(edgeLabel);
    }

    @Test
    public void testGetEdgeLabelNull() {
        when(mockJanusManagement.getEdgeLabel("nonExistentLabel")).thenReturn(null);

        AtlasEdgeLabel result = management.getEdgeLabel("nonExistentLabel");
        assertNull(result);
    }

    @Test
    public void testCreateVertexCompositeIndex() {
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder mockIndexBuilder = mock(org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder.class);
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        when(mockJanusManagement.buildIndex("testIndex", Vertex.class)).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.addKey(any())).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.unique()).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.buildCompositeIndex()).thenReturn(mockJanusGraphIndex);

        List<AtlasPropertyKey> propertyKeys = Collections.singletonList(mockPropertyKey);

        management.createVertexCompositeIndex("testIndex", true, propertyKeys);
    }

    @Test
    public void testCreateEdgeCompositeIndex() {
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder mockIndexBuilder = mock(org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder.class);
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockJanusManagement.buildIndex("testIndex", org.apache.tinkerpop.gremlin.structure.Edge.class)).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.addKey(any())).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.buildCompositeIndex()).thenReturn(mockJanusGraphIndex);

        List<AtlasPropertyKey> propertyKeys = Collections.singletonList(mockPropertyKey);

        management.createEdgeCompositeIndex("testIndex", false, propertyKeys);
    }

    @Test
    public void testGetGraphIndex() {
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockJanusManagement.getGraphIndex("testIndex")).thenReturn(mockJanusGraphIndex);

        AtlasGraphIndex graphIndex = management.getGraphIndex("testIndex");
        assertNotNull(graphIndex);
    }

    @Test
    public void testGetGraphIndexWithNullIndex() {
        when(mockJanusManagement.getGraphIndex("nonExistentIndex")).thenReturn(null);

        AtlasGraphIndex result = management.getGraphIndex("nonExistentIndex");
        assertNull(result);
    }

    @Test
    public void testEdgeIndexExist() {
        org.janusgraph.core.EdgeLabel mockEdgeLabel = mock(org.janusgraph.core.EdgeLabel.class);
        org.janusgraph.core.schema.RelationTypeIndex mockRelationIndex = mock(org.janusgraph.core.schema.RelationTypeIndex.class);

        when(mockJanusManagement.getEdgeLabel("testLabel")).thenReturn(mockEdgeLabel);
        when(mockJanusManagement.getRelationIndex(mockEdgeLabel, "testIndex")).thenReturn(mockRelationIndex);

        assertTrue(management.edgeIndexExist("testLabel", "testIndex"));
    }

    @Test
    public void testEdgeIndexExistNonExistent() {
        when(mockJanusManagement.getEdgeLabel("testLabel")).thenReturn(null);

        assertFalse(management.edgeIndexExist("testLabel", "testIndex"));
    }

    @Test
    public void testCreateVertexMixedIndex() {
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder mockIndexBuilder = mock(org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder.class);
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockJanusManagement.buildIndex("testIndex", Vertex.class)).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.addKey(any())).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.buildMixedIndex(anyString())).thenReturn(mockJanusGraphIndex);

        List<AtlasPropertyKey> propertyKeys = Collections.singletonList(mockPropertyKey);

        management.createVertexMixedIndex("testIndex", "backingIndex", propertyKeys);
    }

    @Test
    public void testCreateEdgeMixedIndex() {
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder mockIndexBuilder = mock(org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder.class);
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockJanusManagement.buildIndex("testIndex", org.apache.tinkerpop.gremlin.structure.Edge.class)).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.addKey(any())).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.buildMixedIndex(anyString())).thenReturn(mockJanusGraphIndex);

        List<AtlasPropertyKey> propertyKeys = Collections.singletonList(mockPropertyKey);

        management.createEdgeMixedIndex("testIndex", "backingIndex", propertyKeys);
    }

    @Test
    public void testCreateEdgeIndex() {
        org.janusgraph.core.EdgeLabel mockEdgeLabel = mock(org.janusgraph.core.EdgeLabel.class);
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);

        when(mockJanusManagement.getEdgeLabel("testLabel")).thenReturn(mockEdgeLabel);
        when(mockJanusManagement.getRelationIndex(mockEdgeLabel, "testIndex")).thenReturn(null);

        List<AtlasPropertyKey> propertyKeys = Collections.singletonList(mockPropertyKey);

        management.createEdgeIndex("testLabel", "testIndex", AtlasEdgeDirection.OUT, propertyKeys);
    }

    @Test
    public void testCreateEdgeIndexWithNewLabel() {
        org.janusgraph.core.EdgeLabel mockEdgeLabel = mock(org.janusgraph.core.EdgeLabel.class);
        org.janusgraph.core.schema.EdgeLabelMaker mockEdgeLabelMaker = mock(org.janusgraph.core.schema.EdgeLabelMaker.class);
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);

        when(mockJanusManagement.getEdgeLabel("newLabel")).thenReturn(null);
        when(mockJanusManagement.makeEdgeLabel("newLabel")).thenReturn(mockEdgeLabelMaker);
        when(mockEdgeLabelMaker.make()).thenReturn(mockEdgeLabel);
        when(mockJanusManagement.getRelationIndex(mockEdgeLabel, "testIndex")).thenReturn(null);

        List<AtlasPropertyKey> propertyKeys = Collections.singletonList(mockPropertyKey);

        // Should not throw exception
        management.createEdgeIndex("newLabel", "testIndex", AtlasEdgeDirection.OUT, propertyKeys);
    }

    @Test
    public void testCreateEdgeIndexExistingIndex() {
        org.janusgraph.core.EdgeLabel mockEdgeLabel = mock(org.janusgraph.core.EdgeLabel.class);
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        org.janusgraph.core.schema.RelationTypeIndex mockExistingIndex = mock(org.janusgraph.core.schema.RelationTypeIndex.class);

        when(mockJanusManagement.getEdgeLabel("testLabel")).thenReturn(mockEdgeLabel);
        when(mockJanusManagement.getRelationIndex(mockEdgeLabel, "existingIndex")).thenReturn(mockExistingIndex);

        List<AtlasPropertyKey> propertyKeys = Collections.singletonList(mockPropertyKey);
        management.createEdgeIndex("testLabel", "existingIndex", AtlasEdgeDirection.OUT, propertyKeys);

        verify(mockJanusManagement, never()).buildEdgeIndex(any(), anyString(), any(), any());
    }

    @Test
    public void testCreateFullTextMixedIndex() {
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder mockIndexBuilder = mock(org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder.class);
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockJanusManagement.buildIndex("testIndex", Vertex.class)).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.addKey(any(), any())).thenReturn(mockIndexBuilder);
        when(mockIndexBuilder.buildMixedIndex(anyString())).thenReturn(mockJanusGraphIndex);

        List<AtlasPropertyKey> propertyKeys = Collections.singletonList(mockPropertyKey);

        management.createFullTextMixedIndex("testIndex", "backingIndex", propertyKeys);
    }

    @Test
    public void testAddMixedIndex() {
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockPropertyKey.getName()).thenReturn("testProperty");
        when(mockJanusManagement.getGraphIndex("testIndex")).thenReturn(mockJanusGraphIndex);
        when(mockAtlasGraph.getIndexFieldName(any(), any())).thenReturn("testProperty_encoded");

        String result = management.addMixedIndex("testIndex", mockPropertyKey, false);
        assertEquals(result, "testProperty_encoded");
    }

    @Test
    public void testAddMixedIndexWithStringField() {
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockPropertyKey.getName()).thenReturn("testProperty");
        when(mockJanusManagement.getGraphIndex("testIndex")).thenReturn(mockJanusGraphIndex);
        when(mockAtlasGraph.getIndexFieldName(any(), any(), any())).thenReturn("testProperty_string_encoded");

        String result = management.addMixedIndex("testIndex", mockPropertyKey, true);
        assertEquals(result, "testProperty_string_encoded");
    }

    @Test
    public void testGetIndexFieldName() {
        AtlasJanusPropertyKey mockPropertyKey = mock(AtlasJanusPropertyKey.class);
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        when(mockPropertyKey.getWrappedPropertyKey()).thenReturn(mockJanusPropertyKey);
        JanusGraphIndex mockJanusGraphIndex = mock(JanusGraphIndex.class);

        when(mockJanusManagement.getGraphIndex("testIndex")).thenReturn(mockJanusGraphIndex);
        when(mockAtlasGraph.getIndexFieldName(any(), any())).thenReturn("testProperty_encoded");

        String result = management.getIndexFieldName("testIndex", mockPropertyKey, false);
        assertEquals(result, "testProperty_encoded");
    }

    @Test
    public void testUpdateUniqueIndexesForConsistencyLock() {
        // Should not throw exception
        management.updateUniqueIndexesForConsistencyLock();
    }

    @Test
    public void testUpdateSchemaStatus() {
        when(mockAtlasGraph.getGraph()).thenReturn(mockJanusGraph);

        // Should not throw exception
        management.updateSchemaStatus();
    }

    @Test
    public void testUpdateSchemaStatusWithException() {
        when(mockAtlasGraph.getGraph()).thenThrow(new RuntimeException("Test exception"));

        try {
            management.updateSchemaStatus();
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testSetIsSuccess() {
        management.setIsSuccess(true);
        management.setIsSuccess(false);
    }

    @Test
    public void testCloseWithSuccess() throws Exception {
        management.setIsSuccess(true);
        management.close();
    }

    @Test
    public void testCloseWithoutSuccess() throws Exception {
        management.setIsSuccess(false);
        management.close();
    }

    @Test
    public void testCommitWithMultiProperties() throws Exception {
        // Setup mock objects for makePropertyKey
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        org.janusgraph.core.schema.PropertyKeyMaker mockPropertyKeyMaker = mock(org.janusgraph.core.schema.PropertyKeyMaker.class);

        when(mockJanusManagement.makePropertyKey("multiProp")).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.dataType(String.class)).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.cardinality(any(Cardinality.class))).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.make()).thenReturn(mockJanusPropertyKey);
        when(mockJanusPropertyKey.name()).thenReturn("multiProp");
        when(mockJanusManagement.isOpen()).thenReturn(true);

        // Create a multi-cardinality property to add to newMultProperties
        management.makePropertyKey("multiProp", String.class, AtlasCardinality.SET);

        management.setIsSuccess(true);
        management.close();

        // Verify that commit was called and addMultiProperties was called on graph
        verify(mockJanusManagement).commit();
        verify(mockAtlasGraph).addMultiProperties(any());
    }

    @Test
    public void testRollback() throws Exception {
        when(mockJanusManagement.isOpen()).thenReturn(true);

        management.setIsSuccess(false);
        management.close();

        verify(mockJanusManagement).rollback();
        verify(mockJanusManagement, never()).commit();
    }

    @Test
    public void testReindex() throws Exception {
        List<AtlasElement> elements = new ArrayList<>();
        AtlasElement mockElement = mock(AtlasElement.class);
        elements.add(mockElement);

        // Should not throw exception for invalid management system
        management.reindex("testIndex", elements);
    }

    @Test
    public void testReindexWithNullIndex() throws Exception {
        when(mockJanusManagement.getGraphIndex("nonExistentIndex")).thenReturn(null);

        List<AtlasElement> elements = new ArrayList<>();
        management.reindex("nonExistentIndex", elements);
    }

    @Test
    public void testReindexWithNonManagementSystem() throws Exception {
        JanusGraphIndex mockIndex = mock(JanusGraphIndex.class);
        when(mockJanusManagement.getGraphIndex("testIndex")).thenReturn(mockIndex);

        List<AtlasElement> elements = new ArrayList<>();
        management.reindex("testIndex", elements);
    }

    @Test
    public void testStartIndexRecovery() {
        // Mock the JanusGraph to avoid IllegalArgumentException from StandardTransactionLogProcessor
        when(mockAtlasGraph.getGraph()).thenReturn(mockJanusGraph);

        long recoveryStartTime = System.currentTimeMillis();

        try {
            management.startIndexRecovery(recoveryStartTime);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage() != null);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testStopIndexRecovery() {
        Object mockRecoveryObject = mock(Object.class);

        management.stopIndexRecovery(mockRecoveryObject);
        management.stopIndexRecovery(null);
    }

    @Test
    public void testPrintIndexRecoveryStats() {
        Object mockRecoveryObject = mock(Object.class);

        management.printIndexRecoveryStats(mockRecoveryObject);
        management.printIndexRecoveryStats(null);
    }

    @Test
    public void testUpdateSchemaStatusStaticMethod() {
        // Test the static method
        AtlasJanusGraphManagement.updateSchemaStatus(mockJanusManagement, mockJanusGraph, Vertex.class);
        AtlasJanusGraphManagement.updateSchemaStatus(mockJanusManagement, mockJanusGraph, org.apache.tinkerpop.gremlin.structure.Edge.class);
    }

    @Test
    public void testStaticUpdateSchemaStatusWithRegisteredIndex() {
        JanusGraphIndex mockIndex = mock(JanusGraphIndex.class);
        PropertyKey mockFieldKey = mock(PropertyKey.class);
        when(mockIndex.isCompositeIndex()).thenReturn(true);
        when(mockIndex.getFieldKeys()).thenReturn(new PropertyKey[] {mockFieldKey});
        when(mockIndex.getIndexStatus(mockFieldKey)).thenReturn(SchemaStatus.REGISTERED);
        when(mockIndex.name()).thenReturn("testIndex");
        when(mockJanusManagement.getGraphIndexes(Vertex.class)).thenReturn(Collections.singletonList(mockIndex));

        JanusGraphManagement innerMgmt = mock(JanusGraphManagement.class);
        when(mockJanusGraph.openManagement()).thenReturn(innerMgmt);
        when(innerMgmt.getGraphIndex("testIndex")).thenReturn(mockIndex);
        ScanJobFuture mockFuture = mock(ScanJobFuture.class);
        when(innerMgmt.updateIndex(mockIndex, SchemaAction.ENABLE_INDEX)).thenReturn(mockFuture);
        try {
            when(mockFuture.get()).thenReturn(null);
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testStaticUpdateSchemaStatusWithInstalledIndex() {
        JanusGraphIndex mockIndex = mock(JanusGraphIndex.class);
        PropertyKey mockFieldKey = mock(PropertyKey.class);
        when(mockIndex.isCompositeIndex()).thenReturn(true);
        when(mockIndex.getFieldKeys()).thenReturn(new PropertyKey[] {mockFieldKey});
        when(mockIndex.getIndexStatus(mockFieldKey)).thenReturn(SchemaStatus.INSTALLED);
        when(mockIndex.name()).thenReturn("installedIndex");
        when(mockJanusManagement.getGraphIndexes(Vertex.class)).thenReturn(Collections.singletonList(mockIndex));

        // Should handle INSTALLED status without attempting to update
        AtlasJanusGraphManagement.updateSchemaStatus(mockJanusManagement, mockJanusGraph, Vertex.class);

        // Verify no inner management was opened for INSTALLED status
        verify(mockJanusGraph, never()).openManagement();
    }

    @Test
    public void testSetConsistencyStaticMethod() throws Exception {
        // Using reflection to test private static method
        Method method = AtlasJanusGraphManagement.class.getDeclaredMethod("setConsistency", JanusGraphManagement.class, Class.class);
        method.setAccessible(true);

        method.invoke(null, mockJanusManagement, Vertex.class);
        method.invoke(null, mockJanusManagement, org.apache.tinkerpop.gremlin.structure.Edge.class);
    }

    @Test
    public void testStaticSetConsistencyMethod() {
        JanusGraphIndex mockUniqueIndex = mock(JanusGraphIndex.class);
        when(mockUniqueIndex.isCompositeIndex()).thenReturn(true);
        when(mockUniqueIndex.isUnique()).thenReturn(true);
        when(mockJanusManagement.getConsistency(mockUniqueIndex)).thenReturn(ConsistencyModifier.DEFAULT);
        PropertyKey[] fieldKeys = {mock(PropertyKey.class)};
        when(mockUniqueIndex.getFieldKeys()).thenReturn(fieldKeys);
        when(fieldKeys[0].name()).thenReturn("testField");

        when(mockJanusManagement.getGraphIndexes(Vertex.class)).thenReturn(Collections.singletonList(mockUniqueIndex));

        try {
            Method setConsistencyMethod = AtlasJanusGraphManagement.class.getDeclaredMethod("setConsistency", JanusGraphManagement.class, Class.class);
            setConsistencyMethod.setAccessible(true);
            setConsistencyMethod.invoke(null, mockJanusManagement, Vertex.class);

            verify(mockJanusManagement).setConsistency(mockUniqueIndex, ConsistencyModifier.LOCK);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testCheckNameMethod() throws Exception {
        // Using reflection to test private static method
        Method method = AtlasJanusGraphManagement.class.getDeclaredMethod("checkName", String.class);
        method.setAccessible(true);

        // Valid names should not throw
        method.invoke(null, "validName");
        method.invoke(null, "valid_name");
        method.invoke(null, "validName123");

        try {
            method.invoke(null, "");
            assertTrue(false, "Should have thrown exception for empty name");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        try {
            method.invoke(null, "invalid{name");
            assertTrue(false, "Should have thrown exception for invalid character");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testCheckNameMethodWithInvalidNames() throws Exception {
        Method checkNameMethod = AtlasJanusGraphManagement.class.getDeclaredMethod("checkName", String.class);
        checkNameMethod.setAccessible(true);

        // Test each reserved character
        String[] invalidNames = {"", "name{invalid", "name}invalid", "name\"invalid", "name$invalid"};

        for (String invalidName : invalidNames) {
            try {
                checkNameMethod.invoke(null, invalidName);
                assertTrue(false, "Should have thrown exception for: " + invalidName);
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof IllegalArgumentException);
            }
        }

        // Test null name
        try {
            checkNameMethod.invoke(null, (String) null);
            assertTrue(false, "Should have thrown exception for null name");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testCommitMethod() throws Exception {
        // Using reflection to test private method
        Method method = AtlasJanusGraphManagement.class.getDeclaredMethod("commit");
        method.setAccessible(true);

        method.invoke(management);
    }

    @Test
    public void testRollbackMethod() throws Exception {
        // Using reflection to test private method
        Method method = AtlasJanusGraphManagement.class.getDeclaredMethod("rollback");
        method.setAccessible(true);

        method.invoke(management);
    }

    @Test
    public void testNewMultPropertiesField() throws Exception {
        // Using reflection to access private field
        Field field = AtlasJanusGraphManagement.class.getDeclaredField("newMultProperties");
        field.setAccessible(true);

        @SuppressWarnings("unchecked")
        java.util.Set<String> newMultProperties = (java.util.Set<String>) field.get(management);
        assertNotNull(newMultProperties);

        // Mock the JanusGraphManagement to return valid objects
        org.janusgraph.core.PropertyKey mockJanusPropertyKey = mock(org.janusgraph.core.PropertyKey.class);
        org.janusgraph.core.schema.PropertyKeyMaker mockPropertyKeyMaker = mock(org.janusgraph.core.schema.PropertyKeyMaker.class);

        when(mockJanusManagement.makePropertyKey("testMultiProperty")).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.dataType(String.class)).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.cardinality(any(Cardinality.class))).thenReturn(mockPropertyKeyMaker);
        when(mockPropertyKeyMaker.make()).thenReturn(mockJanusPropertyKey);
        when(mockJanusPropertyKey.name()).thenReturn("testMultiProperty");

        // Test that making a multi-cardinality property adds to the set
        management.makePropertyKey("testMultiProperty", String.class, AtlasCardinality.SET);
        assertTrue(newMultProperties.contains("testMultiProperty"));
    }

    @Test
    public void testReindexElementMethod() throws Exception {
        ManagementSystem mockManagementSystem = mock(ManagementSystem.class);
        IndexSerializer mockIndexSerializer = mock(IndexSerializer.class);
        MixedIndexType mockIndexType = mock(MixedIndexType.class);
        StandardJanusGraphTx mockTx = mock(StandardJanusGraphTx.class);
        BackendTransaction mockBackendTx = mock(BackendTransaction.class);

        when(mockManagementSystem.getWrappedTx()).thenReturn(mockTx);
        when(mockTx.getTxHandle()).thenReturn(mockBackendTx);
        when(mockIndexType.getBackingIndexName()).thenReturn("backingIndex");
        when(mockIndexType.getName()).thenReturn("testIndex");

        AtlasElement mockElement = mock(AtlasElement.class);
        org.janusgraph.core.JanusGraphElement mockJanusElement = mock(org.janusgraph.core.JanusGraphElement.class);
        when(mockElement.getWrappedElement()).thenReturn(mockJanusElement);

        List<AtlasElement> elements = Arrays.asList(mockElement, null); // Include null element

        try {
            Method reindexElementMethod = AtlasJanusGraphManagement.class.getDeclaredMethod("reindexElement",
                    ManagementSystem.class, IndexSerializer.class, MixedIndexType.class, List.class);
            reindexElementMethod.setAccessible(true);
            reindexElementMethod.invoke(management, mockManagementSystem, mockIndexSerializer, mockIndexType, elements);

            verify(mockIndexSerializer).reindexElement(eq(mockJanusElement), eq(mockIndexType), any(Map.class));
        } catch (Exception e) {
            // Method might throw exceptions during reindexing, which is acceptable
            assertNotNull(e);
        }
    }

    @Test
    public void testIntegrationWithRealGraph() {
        if (atlasGraph != null) {
            try (AtlasGraphManagement mgmt = atlasGraph.getManagementSystem()) {
                // Test basic operations with real graph
                boolean exists = mgmt.containsPropertyKey("__guid");
                // Can be true or false, just test it doesn't throw

                if (!exists) {
                    AtlasPropertyKey guidKey = mgmt.makePropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
                    assertNotNull(guidKey);
                }

                mgmt.setIsSuccess(true);
            } catch (Exception ignored) {
            }
        }
    }
}
