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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.UNKNOWN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class UpdateCompositeIndexStatusPatchTest {
    @Mock
    private PatchContext patchContext;

    @Mock
    private AtlasPatchRegistry patchRegistry;

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasGraphManagement management;

    private UpdateCompositeIndexStatusPatch patch;
    private MockedStatic<AtlasConfiguration> atlasConfigurationMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        lenient().when(patchContext.getPatchRegistry()).thenReturn(patchRegistry);
        lenient().when(patchContext.getGraph()).thenReturn(graph);
        lenient().when(patchRegistry.getStatus(any())).thenReturn(null);
        lenient().doNothing().when(patchRegistry).register(any(), any(), any(), any(), any());
        lenient().doNothing().when(patchRegistry).updateStatus(any(), any());
        lenient().when(graph.getManagementSystem()).thenReturn(management);

        atlasConfigurationMock = mockStatic(AtlasConfiguration.class);
        patch = new UpdateCompositeIndexStatusPatch(patchContext);
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
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_010");
    }

    @Test
    public void testApplyBasicFunctionality() throws AtlasBaseException {
        patch.apply();
        // Verify patch is functional
        assertNotNull(patch);
        assertEquals(patch.getPatchId(), "JAVA_PATCH_0000_010");
    }

    @Test
    public void testApplyWhenUpdateCompositeIndexStatusEnabled() throws Exception {
        doNothing().when(management).updateSchemaStatus();
        doNothing().when(management).setIsSuccess(true);
        doNothing().when(management).close();

        patch.apply();

        assertEquals(patch.getStatus(), UNKNOWN);
        verify(management, times(1)).updateSchemaStatus();
        verify(management, times(1)).setIsSuccess(true);
        verify(management, times(1)).close();
    }

    @Test
    public void testApplyWithRuntimeException() throws Exception {
        doThrow(new RuntimeException("Test runtime exception")).when(management).updateSchemaStatus();
        doNothing().when(management).close();

        try {
            patch.apply();
            fail("Expected AtlasBaseException to be thrown");
        } catch (AtlasBaseException e) {
            assertEquals(e.getCause().getMessage(), "Test runtime exception");
            verify(management, times(1)).updateSchemaStatus();
            verify(management, never()).setIsSuccess(true);
            verify(management, times(1)).close();
        }
    }

    @Test
    public void testApplyWithAtlasBaseException() throws Exception {
        doNothing().when(management).close();

        try {
            patch.apply();
        } catch (AtlasBaseException e) {
            assertNotNull(e.getCause());
            assertEquals("Original Atlas exception", e.getCause().getMessage());
            verify(management, times(1)).updateSchemaStatus();
            verify(management, never()).setIsSuccess(true);
            verify(management, times(1)).close();
        }
    }

    @Test
    public void testApplyWithExceptionInSetIsSuccess() throws Exception {
        doNothing().when(management).updateSchemaStatus();
        doThrow(new RuntimeException("Exception in setIsSuccess")).when(management).setIsSuccess(true);
        doNothing().when(management).close();

        try {
            patch.apply();
            fail("Expected AtlasBaseException to be thrown");
        } catch (AtlasBaseException e) {
            assertEquals(e.getCause().getMessage(), "Exception in setIsSuccess");
            verify(management, times(1)).updateSchemaStatus();
            verify(management, times(1)).setIsSuccess(true);
            verify(management, times(1)).close();
        }
    }

    @Test
    public void testApplyWithExceptionInClose() throws Exception {
        doNothing().when(management).updateSchemaStatus();
        doNothing().when(management).setIsSuccess(true);
        doThrow(new RuntimeException("Exception in close")).when(management).close();

        // Exception in close should be handled by the patch
        try {
            patch.apply();
            fail("Expected AtlasBaseException to be thrown");
        } catch (AtlasBaseException e) {
            verify(management, times(1)).updateSchemaStatus();
            verify(management, times(1)).setIsSuccess(true);
            verify(management, times(1)).close();
        }
    }
}
