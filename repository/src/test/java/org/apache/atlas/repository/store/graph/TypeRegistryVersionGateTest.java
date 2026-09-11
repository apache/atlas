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
 * See the License for the specific language regarding permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TypeRegistryVersionGateTest {
    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeDefStore typeDefStore;

    private TypeRegistryVersionGate gate;
    private int                     storeLookups;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        storeLookups = 0;
        gate         = new TypeRegistryVersionGate(graph, () -> {
            storeLookups++;

            return typeDefStore;
        });
    }

    @Test
    public void constructionDoesNotResolveTheStore() {
        assertEquals(storeLookups, 0);
    }

    @Test
    public void unchangedVersionDoesNotReload() throws AtlasBaseException {
        try (MockedStatic<AtlasGraphUtilsV2> utils = mockStatic(AtlasGraphUtilsV2.class)) {
            utils.when(() -> AtlasGraphUtilsV2.getTypeDefRegistryVersion(graph)).thenReturn(0L);

            assertFalse(gate.ensureUpToDate());
            assertFalse(gate.ensureUpToDate());
        }

        verify(typeDefStore, never()).init();
        assertEquals(storeLookups, 0);
    }

    @Test
    public void newerVersionReloadsOnce() throws AtlasBaseException {
        try (MockedStatic<AtlasGraphUtilsV2> utils = mockStatic(AtlasGraphUtilsV2.class)) {
            utils.when(() -> AtlasGraphUtilsV2.getTypeDefRegistryVersion(graph)).thenReturn(3L);

            assertTrue(gate.ensureUpToDate());
            assertFalse(gate.ensureUpToDate());
        }

        verify(typeDefStore, times(1)).init();
    }

    @Test
    public void markSeenSkipsReloadForThatVersion() throws AtlasBaseException {
        gate.markSeen(3L);

        try (MockedStatic<AtlasGraphUtilsV2> utils = mockStatic(AtlasGraphUtilsV2.class)) {
            utils.when(() -> AtlasGraphUtilsV2.getTypeDefRegistryVersion(graph)).thenReturn(3L);

            assertFalse(gate.ensureUpToDate());
            assertTrue(gate.isCurrent());
        }

        verify(typeDefStore, never()).init();
    }

    @Test
    public void subsequentBumpReloadsAgain() throws AtlasBaseException {
        try (MockedStatic<AtlasGraphUtilsV2> utils = mockStatic(AtlasGraphUtilsV2.class)) {
            utils.when(() -> AtlasGraphUtilsV2.getTypeDefRegistryVersion(graph)).thenReturn(1L, 2L);

            assertTrue(gate.ensureUpToDate());
            assertTrue(gate.ensureUpToDate());
        }

        verify(typeDefStore, times(2)).init();
    }

    @Test
    public void failedReloadReturnsFalseAndLeavesVersionUnseen() throws AtlasBaseException {
        doThrow(new AtlasBaseException("reload failed")).when(typeDefStore).init();

        try (MockedStatic<AtlasGraphUtilsV2> utils = mockStatic(AtlasGraphUtilsV2.class)) {
            utils.when(() -> AtlasGraphUtilsV2.getTypeDefRegistryVersion(graph)).thenReturn(1L);

            assertFalse(gate.ensureUpToDate());
            assertFalse(gate.isCurrent());
        }
    }
}
