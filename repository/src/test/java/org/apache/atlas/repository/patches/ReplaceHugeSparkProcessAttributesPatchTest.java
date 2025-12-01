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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ReplaceHugeSparkProcessAttributesPatchTest {
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

    @Mock
    private Iterator<Object> iterator;

    private ReplaceHugeSparkProcessAttributesPatch patch;
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
        patch = new ReplaceHugeSparkProcessAttributesPatch(patchContext);
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
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_015");
    }

    @Test
    public void testApplyWhenReplaceHugeSparkProcessAttributesPatchDisabled() throws AtlasBaseException {
        patch.apply();
        verify(graph, never()).query();
    }

    @Test
    public void testReplaceHugeSparkProcessAttributesPatchProcessorConstructor() {
        ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor processor = new ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor(patchContext);
        assertNotNull(processor);
        assertEquals(processor.getGraph(), graph);
        assertEquals(processor.getTypeRegistry(), typeRegistry);
        assertEquals(processor.getIndexer(), indexer);
        assertEquals(processor.getEntityGraphMapper(), entityGraphMapper);
    }

    @Test
    public void testPrepareForExecution() {
        ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor processor = new ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor(patchContext);
        processor.prepareForExecution();
    }

    @Test
    public void testSubmitVerticesToUpdate() {
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("spark_process"))).thenReturn(query);
        when(query.vertexIds()).thenReturn(Arrays.asList(1L, 2L, 3L));

        ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor processor = new ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor(patchContext);
        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(3)).checkProduce(any());
    }

    @Test
    public void testSubmitVerticesToUpdateWithEmptyResults() {
        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("spark_process"))).thenReturn(query);
        when(query.vertexIds()).thenReturn(Arrays.asList());

        ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor processor = new ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor(patchContext);

        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(0)).checkProduce(any());
    }

    @Test
    public void testSubmitVerticesToUpdateWithIterator() {
        // Test the iterator-based approach used in the actual implementation
        Iterable<Object> mockIterable = mock(Iterable.class);
        when(mockIterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(1L, 2L, 3L);

        when(graph.query()).thenReturn(query);
        when(query.has(eq(Constants.ENTITY_TYPE_PROPERTY_KEY), eq("spark_process"))).thenReturn(query);
        when(query.vertexIds()).thenReturn(mockIterable);

        ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor processor = new ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor(patchContext);

        processor.submitVerticesToUpdate(workItemManager);

        verify(workItemManager, times(3)).checkProduce(any());
    }

    @Test
    public void testProcessVertexItem() throws AtlasBaseException {
        Long vertexId = 123L;
        String typeName = "spark_process";

        when(entityType.getVertexPropertyName("details")).thenReturn("spark_process.details");
        when(entityType.getVertexPropertyName("sparkPlanDescription")).thenReturn("spark_process.sparkPlanDescription");
        doNothing().when(vertex).removeProperty(anyString());

        ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor processor = new ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor(patchContext);

        processor.processVertexItem(vertexId, vertex, typeName, entityType);

        verify(vertex, times(1)).removeProperty("spark_process.details");
        verify(vertex, times(1)).removeProperty("spark_process.sparkPlanDescription");
    }

    @Test
    public void testProcessVertexItemWithException() throws AtlasBaseException {
        Long vertexId = 123L;
        String typeName = "spark_process";

        when(entityType.getVertexPropertyName("details")).thenReturn("spark_process.details");
        when(entityType.getVertexPropertyName("sparkPlanDescription")).thenReturn("spark_process.sparkPlanDescription");
        doNothing().when(vertex).removeProperty("spark_process.details");
        doNothing().when(vertex).removeProperty("spark_process.sparkPlanDescription");

        ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor processor = new ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor(patchContext);

        processor.processVertexItem(vertexId, vertex, typeName, entityType);

        verify(vertex, times(1)).removeProperty("spark_process.details");
        verify(vertex, times(1)).removeProperty("spark_process.sparkPlanDescription");
    }

    @Test
    public void testProcessVertexItemWithNullEntityType() {
        Long vertexId = 123L;
        String typeName = "spark_process";

        ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor processor = new ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor(patchContext);
        processor.processVertexItem(vertexId, vertex, typeName, null);

        // Should not attempt to remove properties when entityType is null
        verify(vertex, never()).removeProperty(anyString());
    }

    @Test
    public void testTypeNameConstant() throws Exception {
        Field typeNameField = ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor.class.getDeclaredField("TYPE_NAME_SPARK_PROCESS");
        typeNameField.setAccessible(true);

        String typeName = (String) typeNameField.get(null);

        assertEquals(typeName, "spark_process");
    }

    @Test
    public void testAttributeNameConstants() throws Exception {
        Field detailsField = ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor.class.getDeclaredField("ATTR_NAME_DETAILS");
        Field sparkPlanField = ReplaceHugeSparkProcessAttributesPatch.ReplaceHugeSparkProcessAttributesPatchProcessor.class.getDeclaredField("ATTR_NAME_SPARKPLANDESCRIPTION");

        detailsField.setAccessible(true);
        sparkPlanField.setAccessible(true);

        String detailsConstant = (String) detailsField.get(null);
        String sparkPlanConstant = (String) sparkPlanField.get(null);

        assertEquals(detailsConstant, "details");
        assertEquals(sparkPlanConstant, "sparkPlanDescription");
    }
}
