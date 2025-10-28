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
import org.apache.atlas.model.patches.AtlasPatch.AtlasPatches;
import org.apache.atlas.model.patches.AtlasPatch.PatchStatus;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.FAILED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.SKIPPED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.UNKNOWN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class AtlasPatchManagerTest {
    @Mock
    private AtlasGraph atlasGraph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private GraphBackedSearchIndexer indexer;

    @Mock
    private EntityGraphMapper entityGraphMapper;

    @Mock
    private AtlasPatchRegistry patchRegistry;

    @Mock
    private AtlasPatches atlasPatches;

    @Mock
    private AtlasGraphQuery graphQuery;

    @Mock
    private AtlasGraphQuery childQuery;

    private AtlasPatchManager patchManager;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        // Mock the graph query chain for AtlasPatchRegistry
        when(atlasGraph.query()).thenReturn(graphQuery);
        when(graphQuery.createChildQuery()).thenReturn(childQuery);
        when(childQuery.has(any(), any(), any())).thenReturn(childQuery);
        when(graphQuery.has(any(), any())).thenReturn(graphQuery);
        when(graphQuery.or(any())).thenReturn(graphQuery);
        when(graphQuery.vertices()).thenReturn(new ArrayList<AtlasVertex>());
        // Mock vertex creation for createOrUpdatePatchVertex
        AtlasVertex mockVertex = mock(AtlasVertex.class);
        when(atlasGraph.addVertex()).thenReturn(mockVertex);
        patchManager = new AtlasPatchManager(atlasGraph, typeRegistry, indexer, entityGraphMapper);
    }

    @Test
    public void testConstructor() {
        assertNotNull(patchManager);
    }

    @Test
    public void testApplyAllWithAppliedPatch() throws Exception {
        AtlasPatchHandler mockHandler = createMockHandler(APPLIED);
        invokeInitMethod();
        addHandlerToManager(mockHandler);
        patchManager.applyAll();
        verify(mockHandler, times(0)).apply();
    }

    @Test
    public void testApplyAllWithSkippedPatch() throws Exception {
        AtlasPatchHandler mockHandler = createMockHandler(SKIPPED);
        invokeInitMethod();
        addHandlerToManager(mockHandler);
        patchManager.applyAll();
        verify(mockHandler, times(0)).apply();
    }

    @Test
    public void testApplyAllWithUnknownPatch() throws Exception {
        AtlasPatchHandler mockHandler = createMockHandler(UNKNOWN);
        doNothing().when(mockHandler).apply();
        invokeInitMethod();
        addHandlerToManager(mockHandler);
        patchManager.applyAll();
        verify(mockHandler, times(1)).apply();
    }

    @Test
    public void testApplyAllWithFailedPatch() throws Exception {
        AtlasPatchHandler mockHandler = createMockHandler(FAILED);
        doNothing().when(mockHandler).apply();
        invokeInitMethod();
        addHandlerToManager(mockHandler);
        patchManager.applyAll();
        verify(mockHandler, times(1)).apply();
    }

    @Test
    public void testApplyAllWithException() throws Exception {
        AtlasPatchHandler mockHandler = createMockHandler(UNKNOWN);
        doThrow(new AtlasBaseException("Test exception")).when(mockHandler).apply();
        invokeInitMethod();
        addHandlerToManager(mockHandler);
        patchManager.applyAll();
        verify(mockHandler, times(1)).apply();
    }

    @Test
    public void testAddPatchHandler() throws Exception {
        AtlasPatchHandler mockHandler = mock(AtlasPatchHandler.class);
        patchManager.addPatchHandler(mockHandler);
        List<AtlasPatchHandler> handlers = getHandlersFromManager();
        assertEquals(handlers.size(), 1);
        assertEquals(handlers.get(0), mockHandler);
    }

    @Test
    public void testGetContext() throws Exception {
        invokeInitMethod();
        PatchContext context = patchManager.getContext();
        assertNotNull(context);
    }

    @Test
    public void testInitMethod() throws Exception {
        invokeInitMethod();
        PatchContext context = patchManager.getContext();
        assertNotNull(context);
        List<AtlasPatchHandler> handlers = getHandlersFromManager();
        assertEquals(handlers.size(), 11); // Based on the init() method in AtlasPatchManager
    }

    private AtlasPatchHandler createMockHandler(PatchStatus status) {
        AtlasPatchHandler mockHandler = mock(AtlasPatchHandler.class);
        when(mockHandler.getStatusFromRegistry()).thenReturn(status);
        when(mockHandler.getPatchId()).thenReturn("TEST_PATCH");
        return mockHandler;
    }

    private void invokeInitMethod() throws Exception {
        Method initMethod = AtlasPatchManager.class.getDeclaredMethod("init");
        initMethod.setAccessible(true);
        initMethod.invoke(patchManager);
    }

    @SuppressWarnings("unchecked")
    private List<AtlasPatchHandler> getHandlersFromManager() throws Exception {
        Field handlersField = AtlasPatchManager.class.getDeclaredField("handlers");
        handlersField.setAccessible(true);
        return (List<AtlasPatchHandler>) handlersField.get(patchManager);
    }

    private void addHandlerToManager(AtlasPatchHandler handler) throws Exception {
        List<AtlasPatchHandler> handlers = getHandlersFromManager();
        if (handlers == null) {
            Field handlersField = AtlasPatchManager.class.getDeclaredField("handlers");
            handlersField.setAccessible(true);
            handlersField.set(patchManager, new ArrayList<>());
            handlers = getHandlersFromManager();
        }
        handlers.clear(); // Clear default handlers for test
        handlers.add(handler);
    }
}
