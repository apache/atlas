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

package org.apache.atlas.web.rest;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasRelationship.AtlasRelationshipWithExtInfo;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

public class RelationshipRESTTest {
    @Mock
    private AtlasRelationshipStore relationshipStore;

    @Mock
    private AtlasRelationship mockRelationship;

    @Mock
    private AtlasRelationshipWithExtInfo mockRelationshipWithExtInfo;

    private RelationshipREST relationshipREST;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        relationshipREST = new RelationshipREST(relationshipStore);
    }

    @Test
    public void testCreate_Success() throws AtlasBaseException {
        // Setup
        when(relationshipStore.create(mockRelationship)).thenReturn(mockRelationship);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute
            AtlasRelationship result = relationshipREST.create(mockRelationship);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockRelationship);
            verify(relationshipStore).create(mockRelationship);
        }
    }

    @Test
    public void testCreate_WithPerfTracerEnabled() throws AtlasBaseException {
        // Setup
        when(relationshipStore.create(mockRelationship)).thenReturn(mockRelationship);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(true);
            mockedPerfTracer.when(() -> AtlasPerfTracer.getPerfTracer(any(), anyString())).thenReturn(null);

            // Execute
            AtlasRelationship result = relationshipREST.create(mockRelationship);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockRelationship);
            verify(relationshipStore).create(mockRelationship);
            mockedPerfTracer.verify(() -> AtlasPerfTracer.isPerfTraceEnabled(any()));
            mockedPerfTracer.verify(() -> AtlasPerfTracer.getPerfTracer(any(), anyString()));
        }
    }

    @Test
    public void testCreate_WithException() throws AtlasBaseException {
        // Setup
        AtlasBaseException exception = new AtlasBaseException("Test exception");
        when(relationshipStore.create(mockRelationship)).thenThrow(exception);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute & Verify
            AtlasBaseException thrownException = expectThrows(AtlasBaseException.class, () -> {
                relationshipREST.create(mockRelationship);
            });

            assertEquals(thrownException.getMessage(), "Test exception");
            verify(relationshipStore).create(mockRelationship);
        }
    }

    @Test
    public void testUpdate_Success() throws AtlasBaseException {
        // Setup
        when(relationshipStore.update(mockRelationship)).thenReturn(mockRelationship);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute
            AtlasRelationship result = relationshipREST.update(mockRelationship);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockRelationship);
            verify(relationshipStore).update(mockRelationship);
        }
    }

    @Test
    public void testUpdate_WithPerfTracerEnabled() throws AtlasBaseException {
        // Setup
        when(relationshipStore.update(mockRelationship)).thenReturn(mockRelationship);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(true);
            mockedPerfTracer.when(() -> AtlasPerfTracer.getPerfTracer(any(), anyString())).thenReturn(null);

            // Execute
            AtlasRelationship result = relationshipREST.update(mockRelationship);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockRelationship);
            verify(relationshipStore).update(mockRelationship);
            mockedPerfTracer.verify(() -> AtlasPerfTracer.isPerfTraceEnabled(any()));
            mockedPerfTracer.verify(() -> AtlasPerfTracer.getPerfTracer(any(), anyString()));
        }
    }

    @Test
    public void testUpdate_WithException() throws AtlasBaseException {
        // Setup
        AtlasBaseException exception = new AtlasBaseException("Update failed");
        when(relationshipStore.update(mockRelationship)).thenThrow(exception);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute & Verify
            AtlasBaseException thrownException = expectThrows(AtlasBaseException.class, () -> {
                relationshipREST.update(mockRelationship);
            });

            assertEquals(thrownException.getMessage(), "Update failed");
            verify(relationshipStore).update(mockRelationship);
        }
    }

    @Test
    public void testGetById_WithExtendedInfoFalse() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        AtlasRelationship mockRel = mock(AtlasRelationship.class);
        when(relationshipStore.getById(guid)).thenReturn(mockRel);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute
            AtlasRelationshipWithExtInfo result = relationshipREST.getById(guid, false);

            // Verify
            assertNotNull(result);
            verify(relationshipStore).getById(guid);
            verify(relationshipStore, never()).getExtInfoById(anyString());
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetById_WithExtendedInfoTrue() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-456";
        when(relationshipStore.getExtInfoById(guid)).thenReturn(mockRelationshipWithExtInfo);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute
            AtlasRelationshipWithExtInfo result = relationshipREST.getById(guid, true);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockRelationshipWithExtInfo);
            verify(relationshipStore).getExtInfoById(guid);
            verify(relationshipStore, never()).getById(anyString());
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetById_WithPerfTracerEnabled() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-789";
        AtlasRelationship mockRel = mock(AtlasRelationship.class);
        when(relationshipStore.getById(guid)).thenReturn(mockRel);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(true);
            mockedPerfTracer.when(() -> AtlasPerfTracer.getPerfTracer(any(), anyString())).thenReturn(null);

            // Execute
            AtlasRelationshipWithExtInfo result = relationshipREST.getById(guid, false);

            // Verify
            assertNotNull(result);
            verify(relationshipStore).getById(guid);
            mockedPerfTracer.verify(() -> AtlasPerfTracer.isPerfTraceEnabled(any()));
            mockedPerfTracer.verify(() -> AtlasPerfTracer.getPerfTracer(any(), anyString()));
        }
    }

    @Test
    public void testGetById_WithException() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-error";
        AtlasBaseException exception = new AtlasBaseException("Get failed");
        when(relationshipStore.getById(guid)).thenThrow(exception);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute & Verify
            AtlasBaseException thrownException = expectThrows(AtlasBaseException.class, () -> {
                relationshipREST.getById(guid, false);
            });

            assertEquals(thrownException.getMessage(), "Get failed");
            verify(relationshipStore).getById(guid);
        }
    }

    @Test
    public void testDeleteById_Success() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-delete";
        doNothing().when(relationshipStore).deleteById(guid);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute
            relationshipREST.deleteById(guid);

            // Verify
            verify(relationshipStore).deleteById(guid);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testDeleteById_WithPerfTracerEnabled() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-delete-perf";
        doNothing().when(relationshipStore).deleteById(guid);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(true);
            mockedPerfTracer.when(() -> AtlasPerfTracer.getPerfTracer(any(), anyString())).thenReturn(null);

            // Execute
            relationshipREST.deleteById(guid);

            // Verify
            verify(relationshipStore).deleteById(guid);
            mockedPerfTracer.verify(() -> AtlasPerfTracer.isPerfTraceEnabled(any()));
            mockedPerfTracer.verify(() -> AtlasPerfTracer.getPerfTracer(any(), anyString()));
        }
    }

    @Test
    public void testDeleteById_WithException() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-delete-error";
        AtlasBaseException exception = new AtlasBaseException("Delete failed");
        doThrow(exception).when(relationshipStore).deleteById(guid);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute & Verify
            AtlasBaseException thrownException = expectThrows(AtlasBaseException.class, () -> {
                relationshipREST.deleteById(guid);
            });

            assertEquals(thrownException.getMessage(), "Delete failed");
            verify(relationshipStore).deleteById(guid);
        }
    }
}
