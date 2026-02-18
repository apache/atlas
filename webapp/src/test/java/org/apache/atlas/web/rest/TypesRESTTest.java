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
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypeDefHeader;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

public class TypesRESTTest {
    @Mock
    private AtlasTypeDefStore typeDefStore;

    @Mock
    private AtlasBaseTypeDef mockBaseTypeDef;

    @Mock
    private AtlasEnumDef mockEnumDef;

    @Mock
    private AtlasStructDef mockStructDef;

    @Mock
    private AtlasClassificationDef mockClassificationDef;

    @Mock
    private AtlasEntityDef mockEntityDef;

    @Mock
    private AtlasRelationshipDef mockRelationshipDef;

    @Mock
    private AtlasBusinessMetadataDef mockBusinessMetadataDef;

    @Mock
    private AtlasTypesDef mockTypesDef;

    @Mock
    private AtlasTypeDefHeader mockTypeDefHeader;

    @Mock
    private HttpServletRequest httpServletRequest;

    private TypesREST typesREST;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Manually create the TypesREST instance with mocked dependencies
        typesREST = new TypesREST(typeDefStore);
    }

    @Test
    public void testGetTypeDefByName_Success() throws AtlasBaseException {
        // Setup
        String name = "test_type";
        when(typeDefStore.getByName(name)).thenReturn(mockBaseTypeDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasBaseTypeDef result = typesREST.getTypeDefByName(name);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockBaseTypeDef);
            verify(typeDefStore).getByName(name);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("name", name));
        }
    }

    @Test
    public void testGetTypeDefByGuid_Success() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        when(typeDefStore.getByGuid(guid)).thenReturn(mockBaseTypeDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasBaseTypeDef result = typesREST.getTypeDefByGuid(guid);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockBaseTypeDef);
            verify(typeDefStore).getByGuid(guid);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetTypeDefHeaders_Success() throws AtlasBaseException {
        // Setup
        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("excludeInternalTypesAndReferences", new String[] {"true"});
        parameterMap.put("type", new String[] {"entity"});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeDefStore.searchTypesDef(any(SearchFilter.class))).thenReturn(mockTypesDef);

        List<AtlasTypeDefHeader> mockHeaders = Collections.singletonList(mockTypeDefHeader);

        try (MockedStatic<FilterUtil> mockedFilterUtil = mockStatic(FilterUtil.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toTypeDefHeader(mockTypesDef)).thenReturn(mockHeaders);

            // Execute
            List<AtlasTypeDefHeader> result = typesREST.getTypeDefHeaders(httpServletRequest);

            // Verify
            assertNotNull(result);
            assertEquals(result.size(), 1);
            assertEquals(result.get(0), mockTypeDefHeader);
            verify(typeDefStore).searchTypesDef(any(SearchFilter.class));
            mockedAtlasTypeUtil.verify(() -> AtlasTypeUtil.toTypeDefHeader(mockTypesDef));
        }
    }

    @Test
    public void testGetAllTypeDefs_Success() throws AtlasBaseException {
        // Setup
        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("type", new String[] {"entity"});
        parameterMap.put("name", new String[] {"test"});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeDefStore.searchTypesDef(any(SearchFilter.class))).thenReturn(mockTypesDef);

        // Execute
        AtlasTypesDef result = typesREST.getAllTypeDefs(httpServletRequest);

        // Verify
        assertNotNull(result);
        assertEquals(result, mockTypesDef);
        verify(typeDefStore).searchTypesDef(any(SearchFilter.class));
    }

    @Test
    public void testGetEnumDefByName_Success() throws AtlasBaseException {
        // Setup
        String name = "test_enum";
        when(typeDefStore.getEnumDefByName(name)).thenReturn(mockEnumDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasEnumDef result = typesREST.getEnumDefByName(name);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockEnumDef);
            verify(typeDefStore).getEnumDefByName(name);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("name", name));
        }
    }

    @Test
    public void testGetEnumDefByGuid_Success() throws AtlasBaseException {
        // Setup
        String guid = "enum-guid-123";
        when(typeDefStore.getEnumDefByGuid(guid)).thenReturn(mockEnumDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasEnumDef result = typesREST.getEnumDefByGuid(guid);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockEnumDef);
            verify(typeDefStore).getEnumDefByGuid(guid);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetStructDefByName_Success() throws AtlasBaseException {
        // Setup
        String name = "test_struct";
        when(typeDefStore.getStructDefByName(name)).thenReturn(mockStructDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasStructDef result = typesREST.getStructDefByName(name);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockStructDef);
            verify(typeDefStore).getStructDefByName(name);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("name", name));
        }
    }

    @Test
    public void testGetStructDefByGuid_Success() throws AtlasBaseException {
        // Setup
        String guid = "struct-guid-123";
        when(typeDefStore.getStructDefByGuid(guid)).thenReturn(mockStructDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasStructDef result = typesREST.getStructDefByGuid(guid);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockStructDef);
            verify(typeDefStore).getStructDefByGuid(guid);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetClassificationDefByName_Success() throws AtlasBaseException {
        // Setup
        String name = "test_classification";
        when(typeDefStore.getClassificationDefByName(name)).thenReturn(mockClassificationDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasClassificationDef result = typesREST.getClassificationDefByName(name);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockClassificationDef);
            verify(typeDefStore).getClassificationDefByName(name);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("name", name));
        }
    }

    @Test
    public void testGetClassificationDefByGuid_Success() throws AtlasBaseException {
        // Setup
        String guid = "classification-guid-123";
        when(typeDefStore.getClassificationDefByGuid(guid)).thenReturn(mockClassificationDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasClassificationDef result = typesREST.getClassificationDefByGuid(guid);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockClassificationDef);
            verify(typeDefStore).getClassificationDefByGuid(guid);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetEntityDefByName_Success() throws AtlasBaseException {
        // Setup
        String name = "test_entity";
        when(typeDefStore.getEntityDefByName(name)).thenReturn(mockEntityDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasEntityDef result = typesREST.getEntityDefByName(name);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockEntityDef);
            verify(typeDefStore).getEntityDefByName(name);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("name", name));
        }
    }

    @Test
    public void testGetEntityDefByGuid_Success() throws AtlasBaseException {
        // Setup
        String guid = "entity-guid-123";
        when(typeDefStore.getEntityDefByGuid(guid)).thenReturn(mockEntityDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasEntityDef result = typesREST.getEntityDefByGuid(guid);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockEntityDef);
            verify(typeDefStore).getEntityDefByGuid(guid);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetRelationshipDefByName_Success() throws AtlasBaseException {
        // Setup
        String name = "test_relationship";
        when(typeDefStore.getRelationshipDefByName(name)).thenReturn(mockRelationshipDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasRelationshipDef result = typesREST.getRelationshipDefByName(name);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockRelationshipDef);
            verify(typeDefStore).getRelationshipDefByName(name);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("name", name));
        }
    }

    @Test
    public void testGetRelationshipDefByGuid_Success() throws AtlasBaseException {
        // Setup
        String guid = "relationship-guid-123";
        when(typeDefStore.getRelationshipDefByGuid(guid)).thenReturn(mockRelationshipDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasRelationshipDef result = typesREST.getRelationshipDefByGuid(guid);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockRelationshipDef);
            verify(typeDefStore).getRelationshipDefByGuid(guid);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetBusinessMetadataDefByGuid_Success() throws AtlasBaseException {
        // Setup
        String guid = "business-metadata-guid-123";
        when(typeDefStore.getBusinessMetadataDefByGuid(guid)).thenReturn(mockBusinessMetadataDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasBusinessMetadataDef result = typesREST.getBusinessMetadataDefByGuid(guid);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockBusinessMetadataDef);
            verify(typeDefStore).getBusinessMetadataDefByGuid(guid);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("guid", guid));
        }
    }

    @Test
    public void testGetBusinessMetadataDefByName_Success() throws AtlasBaseException {
        // Setup
        String name = "test_business_metadata";
        when(typeDefStore.getBusinessMetadataDefByName(name)).thenReturn(mockBusinessMetadataDef);

        try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            // Execute
            AtlasBusinessMetadataDef result = typesREST.getBusinessMetadataDefByName(name);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockBusinessMetadataDef);
            verify(typeDefStore).getBusinessMetadataDefByName(name);
            mockedServlets.verify(() -> Servlets.validateQueryParamLength("name", name));
        }
    }

    @Test
    public void testCreateAtlasTypeDefs_Success() throws AtlasBaseException {
        // Setup
        when(typeDefStore.createTypesDef(mockTypesDef)).thenReturn(mockTypesDef);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute
            AtlasTypesDef result = typesREST.createAtlasTypeDefs(mockTypesDef);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockTypesDef);
            verify(typeDefStore).createTypesDef(mockTypesDef);
        }
    }

    @Test
    public void testCreateAtlasTypeDefs_WithPerfTracerEnabled() throws AtlasBaseException {
        // Setup
        when(typeDefStore.createTypesDef(mockTypesDef)).thenReturn(mockTypesDef);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(true);
            mockedPerfTracer.when(() -> AtlasPerfTracer.getPerfTracer(any(), anyString())).thenReturn(null);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute
            AtlasTypesDef result = typesREST.createAtlasTypeDefs(mockTypesDef);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockTypesDef);
            verify(typeDefStore).createTypesDef(mockTypesDef);
            mockedPerfTracer.verify(() -> AtlasPerfTracer.isPerfTraceEnabled(any()));
            mockedPerfTracer.verify(() -> AtlasPerfTracer.getPerfTracer(any(), anyString()));
        }
    }

    @Test
    public void testCreateAtlasTypeDefs_WithException() throws AtlasBaseException {
        // Setup
        AtlasBaseException exception = new AtlasBaseException("Create failed");
        when(typeDefStore.createTypesDef(mockTypesDef)).thenThrow(exception);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute & Verify
            AtlasBaseException thrownException = expectThrows(AtlasBaseException.class, () -> {
                typesREST.createAtlasTypeDefs(mockTypesDef);
            });

            assertEquals(thrownException.getMessage(), "Create failed");
            verify(typeDefStore).createTypesDef(mockTypesDef);
        }
    }

    @Test
    public void testUpdateAtlasTypeDefs_Success() throws AtlasBaseException {
        // Setup
        when(typeDefStore.updateTypesDef(mockTypesDef)).thenReturn(mockTypesDef);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute
            AtlasTypesDef result = typesREST.updateAtlasTypeDefs(mockTypesDef);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockTypesDef);
            verify(typeDefStore).updateTypesDef(mockTypesDef);
        }
    }

    @Test
    public void testUpdateAtlasTypeDefs_WithPerfTracerEnabled() throws AtlasBaseException {
        // Setup
        when(typeDefStore.updateTypesDef(mockTypesDef)).thenReturn(mockTypesDef);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(true);
            mockedPerfTracer.when(() -> AtlasPerfTracer.getPerfTracer(any(), anyString())).thenReturn(null);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute
            AtlasTypesDef result = typesREST.updateAtlasTypeDefs(mockTypesDef);

            // Verify
            assertNotNull(result);
            assertEquals(result, mockTypesDef);
            verify(typeDefStore).updateTypesDef(mockTypesDef);
            mockedPerfTracer.verify(() -> AtlasPerfTracer.isPerfTraceEnabled(any()));
            mockedPerfTracer.verify(() -> AtlasPerfTracer.getPerfTracer(any(), anyString()));
        }
    }

    @Test
    public void testUpdateAtlasTypeDefs_WithException() throws AtlasBaseException {
        // Setup
        AtlasBaseException exception = new AtlasBaseException("Update failed");
        when(typeDefStore.updateTypesDef(mockTypesDef)).thenThrow(exception);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute & Verify
            AtlasBaseException thrownException = expectThrows(AtlasBaseException.class, () -> {
                typesREST.updateAtlasTypeDefs(mockTypesDef);
            });

            assertEquals(thrownException.getMessage(), "Update failed");
            verify(typeDefStore).updateTypesDef(mockTypesDef);
        }
    }

    @Test
    public void testDeleteAtlasTypeDefs_Success() throws AtlasBaseException {
        // Setup
        doNothing().when(typeDefStore).deleteTypesDef(mockTypesDef);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute
            typesREST.deleteAtlasTypeDefs(mockTypesDef);

            // Verify
            verify(typeDefStore).deleteTypesDef(mockTypesDef);
        }
    }

    @Test
    public void testDeleteAtlasTypeDefs_WithPerfTracerEnabled() throws AtlasBaseException {
        // Setup
        doNothing().when(typeDefStore).deleteTypesDef(mockTypesDef);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(true);
            mockedPerfTracer.when(() -> AtlasPerfTracer.getPerfTracer(any(), anyString())).thenReturn(null);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute
            typesREST.deleteAtlasTypeDefs(mockTypesDef);

            // Verify
            verify(typeDefStore).deleteTypesDef(mockTypesDef);
            mockedPerfTracer.verify(() -> AtlasPerfTracer.isPerfTraceEnabled(any()));
            mockedPerfTracer.verify(() -> AtlasPerfTracer.getPerfTracer(any(), anyString()));
        }
    }

    @Test
    public void testDeleteAtlasTypeDefs_WithException() throws AtlasBaseException {
        // Setup
        AtlasBaseException exception = new AtlasBaseException("Delete failed");
        doThrow(exception).when(typeDefStore).deleteTypesDef(mockTypesDef);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class);
                MockedStatic<AtlasTypeUtil> mockedAtlasTypeUtil = mockStatic(AtlasTypeUtil.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);
            mockedAtlasTypeUtil.when(() -> AtlasTypeUtil.toDebugString(mockTypesDef)).thenReturn("debug_string");

            // Execute & Verify
            AtlasBaseException thrownException = expectThrows(AtlasBaseException.class, () -> {
                typesREST.deleteAtlasTypeDefs(mockTypesDef);
            });

            assertEquals(thrownException.getMessage(), "Delete failed");
            verify(typeDefStore).deleteTypesDef(mockTypesDef);
        }
    }

    @Test
    public void testDeleteAtlasTypeByName_Success() throws AtlasBaseException {
        // Setup
        String typeName = "test_type";
        doNothing().when(typeDefStore).deleteTypeByName(typeName, false);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute
            typesREST.deleteAtlasTypeByName(typeName, false);

            // Verify
            verify(typeDefStore).deleteTypeByName(typeName, false);
        }
    }

    @Test
    public void testDeleteAtlasTypeByName_WithPerfTracerEnabled() throws AtlasBaseException {
        // Setup
        String typeName = "test_type_perf";
        doNothing().when(typeDefStore).deleteTypeByName(typeName, false);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(true);
            mockedPerfTracer.when(() -> AtlasPerfTracer.getPerfTracer(any(), anyString())).thenReturn(null);

            // Execute
            typesREST.deleteAtlasTypeByName(typeName, false);

            // Verify
            verify(typeDefStore).deleteTypeByName(typeName, false);
            mockedPerfTracer.verify(() -> AtlasPerfTracer.isPerfTraceEnabled(any()));
            mockedPerfTracer.verify(() -> AtlasPerfTracer.getPerfTracer(any(), anyString()));
        }
    }

    @Test
    public void testDeleteAtlasTypeByName_WithException() throws AtlasBaseException {
        // Setup
        String typeName = "test_type_error";
        AtlasBaseException exception = new AtlasBaseException("Delete by name failed");
        doThrow(exception).when(typeDefStore).deleteTypeByName(typeName, false);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute & Verify
            AtlasBaseException thrownException = expectThrows(AtlasBaseException.class, () -> {
                typesREST.deleteAtlasTypeByName(typeName, false);
            });

            assertEquals(thrownException.getMessage(), "Delete by name failed");
            verify(typeDefStore).deleteTypeByName(typeName, false);
        }
    }

    @Test
    public void testDeleteAtlasTypeByName_WithForceDelete() throws AtlasBaseException {
        // Setup
        String typeName = "test_type_force";
        doNothing().when(typeDefStore).deleteTypeByName(typeName, true);

        try (MockedStatic<AtlasPerfTracer> mockedPerfTracer = mockStatic(AtlasPerfTracer.class)) {
            mockedPerfTracer.when(() -> AtlasPerfTracer.isPerfTraceEnabled(any())).thenReturn(false);

            // Execute with force=true
            typesREST.deleteAtlasTypeByName(typeName, true);

            // Verify
            verify(typeDefStore).deleteTypeByName(typeName, true);
        }
    }

    @Test
    public void testGetSearchFilter_WithExcludeInternalTypes() throws Exception {
        // Setup
        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("excludeInternalTypesAndReferences", new String[] {"true"});
        parameterMap.put("type", new String[] {"entity"});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);

        try (MockedStatic<FilterUtil> mockedFilterUtil = mockStatic(FilterUtil.class)) {
            // Use reflection to access private method
            java.lang.reflect.Method getSearchFilterMethod = TypesREST.class.getDeclaredMethod("getSearchFilter", HttpServletRequest.class);
            getSearchFilterMethod.setAccessible(true);

            SearchFilter result = (SearchFilter) getSearchFilterMethod.invoke(typesREST, httpServletRequest);

            // Verify
            assertNotNull(result);
        }
    }

    @Test
    public void testGetSearchFilter_WithoutExcludeInternalTypes() throws Exception {
        // Setup
        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("type", new String[] {"entity"});
        parameterMap.put("name", new String[] {"test"});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);

        // Use reflection to access private method
        java.lang.reflect.Method getSearchFilterMethod = TypesREST.class.getDeclaredMethod("getSearchFilter", HttpServletRequest.class);
        getSearchFilterMethod.setAccessible(true);

        SearchFilter result = (SearchFilter) getSearchFilterMethod.invoke(typesREST, httpServletRequest);

        // Verify
        assertNotNull(result);
    }

    @Test
    public void testGetSearchFilter_WithNullParameters() throws Exception {
        // Setup
        Map<String, String[]> parameterMap = new HashMap<>();
        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);

        // Use reflection to access private method
        java.lang.reflect.Method getSearchFilterMethod = TypesREST.class.getDeclaredMethod("getSearchFilter", HttpServletRequest.class);
        getSearchFilterMethod.setAccessible(true);

        SearchFilter result = (SearchFilter) getSearchFilterMethod.invoke(typesREST, httpServletRequest);

        // Verify
        assertNotNull(result);
    }
}
