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
import org.apache.atlas.model.patches.AtlasPatch.PatchStatus;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.UNKNOWN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class AtlasPatchHandlerTest {
    @Mock
    private AtlasPatchRegistry patchRegistry;

    private TestAtlasPatchHandler patchHandler;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(patchRegistry.getStatus("TEST_PATCH_001")).thenReturn(null);
        patchHandler = new TestAtlasPatchHandler(patchRegistry, "TEST_PATCH_001", "Test patch description");
    }

    @Test
    public void testConstructorWithNullStatus() {
        when(patchRegistry.getStatus("TEST_PATCH_002")).thenReturn(null);
        TestAtlasPatchHandler handler = new TestAtlasPatchHandler(patchRegistry, "TEST_PATCH_002", "Test description");

        assertEquals(handler.getPatchId(), "TEST_PATCH_002");
        verify(patchRegistry, times(1)).register(eq("TEST_PATCH_002"), eq("Test description"), eq(AtlasPatchHandler.JAVA_PATCH_TYPE), eq("apply"), eq(UNKNOWN));
    }

    @Test
    public void testConstructorWithUnknownStatus() {
        when(patchRegistry.getStatus("TEST_PATCH_003")).thenReturn(UNKNOWN);
        TestAtlasPatchHandler handler = new TestAtlasPatchHandler(patchRegistry, "TEST_PATCH_003", "Test description");

        assertEquals(handler.getPatchId(), "TEST_PATCH_003");
        verify(patchRegistry, times(1)).register(eq("TEST_PATCH_003"), eq("Test description"),
                eq(AtlasPatchHandler.JAVA_PATCH_TYPE), eq("apply"), eq(UNKNOWN));
    }

    @Test
    public void testConstructorWithAppliedStatus() {
        when(patchRegistry.getStatus("TEST_PATCH_004")).thenReturn(APPLIED);
        doNothing().when(patchRegistry).register(any(), any(), any(), any(), any());
        TestAtlasPatchHandler handler = new TestAtlasPatchHandler(patchRegistry, "TEST_PATCH_004", "Test description");
        assertEquals(handler.getPatchId(), "TEST_PATCH_004");
        // Constructor always calls register() regardless of status
        verify(patchRegistry, times(1)).register(any(), any(), any(), any(), any());
    }

    @Test
    public void testGetStatusFromRegistry() {
        // Reset the mock to clear previous interactions
        reset(patchRegistry);
        when(patchRegistry.getStatus("TEST_PATCH_001")).thenReturn(APPLIED);

        PatchStatus status = patchHandler.getStatusFromRegistry();

        assertEquals(status, APPLIED);
        verify(patchRegistry, times(1)).getStatus("TEST_PATCH_001");
    }

    @Test
    public void testGetStatus() {
        when(patchRegistry.getStatus("TEST_PATCH_001")).thenReturn(APPLIED);
        patchHandler.setStatus(APPLIED);

        PatchStatus status = patchHandler.getStatus();

        assertEquals(status, APPLIED);
    }

    @Test
    public void testSetStatus() {
        patchHandler.setStatus(APPLIED);

        assertEquals(patchHandler.getStatus(), APPLIED);
        verify(patchRegistry, times(1)).updateStatus("TEST_PATCH_001", APPLIED);
    }

    @Test
    public void testGetPatchId() {
        assertEquals(patchHandler.getPatchId(), "TEST_PATCH_001");
    }

    @Test
    public void testJavaPatchTypeConstant() {
        assertEquals(AtlasPatchHandler.JAVA_PATCH_TYPE, "JAVA_PATCH");
    }

    // Test implementation of abstract AtlasPatchHandler
    private static class TestAtlasPatchHandler extends AtlasPatchHandler {
        public TestAtlasPatchHandler(AtlasPatchRegistry patchRegistry, String patchId, String patchDescription) {
            super(patchRegistry, patchId, patchDescription);
        }

        @Override
        public void apply() throws AtlasBaseException {
            // Test implementation - does nothing
        }
    }
}
