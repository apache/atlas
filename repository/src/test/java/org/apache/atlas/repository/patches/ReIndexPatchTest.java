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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ReIndexPatchTest {
    @Mock
    private PatchContext patchContext;

    @Mock
    private AtlasPatchRegistry patchRegistry;

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasGraphManagement management;

    private ReIndexPatch patch;
    private MockedStatic<AtlasConfiguration> atlasConfigurationMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(patchContext.getPatchRegistry()).thenReturn(patchRegistry);
        when(patchContext.getGraph()).thenReturn(graph);
        when(patchRegistry.getStatus(any())).thenReturn(null);
        lenient().doNothing().when(patchRegistry).register(any(), any(), any(), any(), any());
        lenient().doNothing().when(patchRegistry).updateStatus(any(), any());
        when(graph.getManagementSystem()).thenReturn(management);

        atlasConfigurationMock = mockStatic(AtlasConfiguration.class);
        patch = new ReIndexPatch(patchContext);
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
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_006");
    }

    @Test
    public void testApplyWhenRebuildIndexDisabled() throws AtlasBaseException {
        patch.apply();

        verify(management, never()).updateUniqueIndexesForConsistencyLock();
    }

    @Test
    public void testReindexPatchProcessorConstructor() {
        ReIndexPatch.ReindexPatchProcessor processor = new ReIndexPatch.ReindexPatchProcessor(patchContext);

        assertNotNull(processor);
    }

    @Test
    public void testReindexPatchProcessorRepairVertices() throws Exception {
        ReIndexPatch.ReindexPatchProcessor processor = new ReIndexPatch.ReindexPatchProcessor(patchContext);

        // Use reflection to test the repairVertices method
        Method repairVerticesMethod = ReIndexPatch.ReindexPatchProcessor.class.getDeclaredMethod("repairVertices");
        repairVerticesMethod.setAccessible(true);

        repairVerticesMethod.invoke(processor);
    }

    @Test
    public void testReindexPatchProcessorRepairEdges() throws Exception {
        ReIndexPatch.ReindexPatchProcessor processor = new ReIndexPatch.ReindexPatchProcessor(patchContext);

        // Use reflection to test the repairEdges method
        Method repairEdgesMethod = ReIndexPatch.ReindexPatchProcessor.class.getDeclaredMethod("repairEdges");
        repairEdgesMethod.setAccessible(true);

        repairEdgesMethod.invoke(processor);
    }

    @Test
    public void testReindexPatchProcessorStaticFields() throws Exception {
        Field vertexIndexNamesField = ReIndexPatch.ReindexPatchProcessor.class.getDeclaredField("VERTEX_INDEX_NAMES");
        Field edgeIndexNamesField = ReIndexPatch.ReindexPatchProcessor.class.getDeclaredField("EDGE_INDEX_NAMES");
        Field workerPrefixField = ReIndexPatch.ReindexPatchProcessor.class.getDeclaredField("WORKER_PREFIX");

        vertexIndexNamesField.setAccessible(true);
        edgeIndexNamesField.setAccessible(true);
        workerPrefixField.setAccessible(true);

        String[] vertexIndexNames = (String[]) vertexIndexNamesField.get(null);
        String[] edgeIndexNames = (String[]) edgeIndexNamesField.get(null);
        String workerPrefix = (String) workerPrefixField.get(null);

        assertNotNull(vertexIndexNames);
        assertNotNull(edgeIndexNames);
        assertNotNull(workerPrefix);
        assertEquals(workerPrefix, "reindex");
    }

    @Test
    public void testReindexPatchProcessorRepairElementsMethod() throws Exception {
        ReIndexPatch.ReindexPatchProcessor processor = new ReIndexPatch.ReindexPatchProcessor(patchContext);

        // Use reflection to access the private repairElements method
        Method repairElementsMethod = ReIndexPatch.ReindexPatchProcessor.class.getDeclaredMethod("repairElements", java.util.function.BiConsumer.class, String[].class);
        repairElementsMethod.setAccessible(true);
        java.util.function.BiConsumer<WorkItemManager, AtlasGraph> mockConsumer = (manager, graph) -> {};
        String[] testIndexNames = {"test_index"};

        repairElementsMethod.invoke(processor, mockConsumer, testIndexNames);
    }

    @Test
    public void testReindexPatchProcessorWithManagementException() throws Exception {
        when(graph.getManagementSystem()).thenThrow(new RuntimeException("Management system error"));

        ReIndexPatch.ReindexPatchProcessor processor = new ReIndexPatch.ReindexPatchProcessor(patchContext);

        Method repairVerticesMethod = ReIndexPatch.ReindexPatchProcessor.class.getDeclaredMethod("repairVertices");
        repairVerticesMethod.setAccessible(true);
        repairVerticesMethod.invoke(processor);

        Method repairEdgesMethod = ReIndexPatch.ReindexPatchProcessor.class.getDeclaredMethod("repairEdges");
        repairEdgesMethod.setAccessible(true);
        repairEdgesMethod.invoke(processor);
    }
}
