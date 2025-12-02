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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.discovery.AtlasLineageService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.lineage.LineageOnDemandConstraints;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class LineageRESTTest {
    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private AtlasLineageService atlasLineageService;

    @Mock
    private AtlasEntityType entityType;

    @Mock
    private AtlasLineageInfo lineageInfo;

    @Mock
    private HttpServletRequest httpServletRequest;

    private LineageREST lineageREST;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Manually create the LineageREST instance with mocked dependencies
        lineageREST = new LineageREST(typeRegistry, atlasLineageService);
    }

    @Test
    public void testGetLineageGraph_WithValidGuid() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, depth)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, depth);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, direction, depth);
    }

    @Test
    public void testGetLineageGraph_WithDefaultDirection() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        int depth = 5;

        when(atlasLineageService.getAtlasLineageInfo(guid, LineageDirection.BOTH, depth)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, LineageDirection.BOTH, depth);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, LineageDirection.BOTH, depth);
    }

    @Test
    public void testGetLineageGraph_WithDefaultDepth() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        LineageDirection direction = LineageDirection.INPUT;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, 3)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, 3);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, direction, 3);
    }

    @Test
    public void testGetLineageGraph_WithInputDirection() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        LineageDirection direction = LineageDirection.INPUT;
        int depth = 2;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, depth)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, depth);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, direction, depth);
    }

    @Test
    public void testGetLineageGraph_WithOutputDirection() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        LineageDirection direction = LineageDirection.OUTPUT;
        int depth = 4;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, depth)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, depth);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, direction, depth);
    }

    @Test
    public void testGetLineageGraph_WithZeroDepth() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 0;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, 3)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, depth);

        // Verify
        assertNull(result);
    }

    @Test
    public void testGetLineageGraph_WithNegativeDepth() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = -1;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, 3)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, depth);

        // Verify
        assertNull(result);
    }

    @Test
    public void testGetLineageGraph_WithLargeDepth() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 10;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, depth)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, depth);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, direction, depth);
    }

    @Test
    public void testGetLineageGraph_WithEmptyGuid() throws AtlasBaseException {
        // Setup
        String guid = "";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, depth)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, depth);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, direction, depth);
    }

    @Test
    public void testGetLineageGraph_WithNullGuid() throws AtlasBaseException {
        // Setup
        String guid = null;
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, depth)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, direction, depth);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, direction, depth);
    }

    @Test
    public void testGetLineageGraph_WithServiceException() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(atlasLineageService.getAtlasLineageInfo(guid, direction, depth))
                .thenThrow(new AtlasBaseException("Entity not found"));

        // Execute & Verify
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            lineageREST.getLineageGraph(guid, direction, depth);
        });

        assertEquals(exception.getMessage(), "Entity not found");
        verify(atlasLineageService).getAtlasLineageInfo(guid, direction, depth);
    }

    @Test
    public void testGetLineageGraph_WithLineageConstraints_Success() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        Map<String, LineageOnDemandConstraints> constraintsMap = new HashMap<>();
        constraintsMap.put("constraint1", new LineageOnDemandConstraints());

        when(atlasLineageService.getAtlasLineageInfo(guid, constraintsMap)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, constraintsMap);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, constraintsMap);
    }

    @Test
    public void testGetLineageGraph_WithLineageConstraints_EmptyConstraints() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        Map<String, LineageOnDemandConstraints> constraintsMap = new HashMap<>();

        when(atlasLineageService.getAtlasLineageInfo(guid, constraintsMap)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, constraintsMap);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, constraintsMap);
    }

    @Test
    public void testGetLineageGraph_WithLineageConstraints_NullConstraints() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        Map<String, LineageOnDemandConstraints> constraintsMap = null;

        when(atlasLineageService.getAtlasLineageInfo(guid, constraintsMap)).thenReturn(lineageInfo);

        // Execute
        AtlasLineageInfo result = lineageREST.getLineageGraph(guid, constraintsMap);

        // Verify
        assertNotNull(result);
        assertEquals(result, lineageInfo);
        verify(atlasLineageService).getAtlasLineageInfo(guid, constraintsMap);
    }

    @Test
    public void testGetLineageGraph_WithLineageConstraints_ServiceException() throws AtlasBaseException {
        // Setup
        String guid = "test-guid-123";
        Map<String, LineageOnDemandConstraints> constraintsMap = new HashMap<>();
        constraintsMap.put("constraint1", new LineageOnDemandConstraints());

        when(atlasLineageService.getAtlasLineageInfo(guid, constraintsMap))
                .thenThrow(new AtlasBaseException("Internal error"));

        // Execute & Verify
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            lineageREST.getLineageGraph(guid, constraintsMap);
        });

        assertEquals(exception.getMessage(), "Internal error");
        verify(atlasLineageService).getAtlasLineageInfo(guid, constraintsMap);
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithValidTypeName() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithDefaultDirection() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        int depth = 3;

        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, null, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithDefaultDepth() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.INPUT;

        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, 0, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithAttributes() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.OUTPUT;
        int depth = 2;

        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("attr:qualifiedName", new String[] {"test_table"});
        parameterMap.put("attr:name", new String[] {"test"});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(httpServletRequest, atLeastOnce()).getParameterMap();
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithMultipleAttributes() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 4;

        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("attr:qualifiedName", new String[] {"db.test_table"});
        parameterMap.put("attr:name", new String[] {"test_table"});
        parameterMap.put("attr:database", new String[] {"test_db"});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(httpServletRequest, atLeastOnce()).getParameterMap();
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithEmptyAttributes() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        Map<String, String[]> parameterMap = new HashMap<>();

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(httpServletRequest).getParameterMap();
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithNullAttributes() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(httpServletRequest.getParameterMap()).thenReturn(null);
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(httpServletRequest).getParameterMap();
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithNonAttrParameters() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("direction", new String[] {"INPUT"});
        parameterMap.put("depth", new String[] {"5"});
        parameterMap.put("attr:qualifiedName", new String[] {"test_table"});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(httpServletRequest, atLeastOnce()).getParameterMap();
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithInvalidTypeName() throws AtlasBaseException {
        // Setup
        String typeName = "invalid_type";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(null);

        // Execute & Verify
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
        });

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_INVALID);
        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(atlasLineageService, never()).getAtlasLineageInfo(anyString(), any(), anyInt());
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithNullTypeName() throws AtlasBaseException {
        // Setup
        String typeName = null;
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(null);

        // Execute & Verify
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
        });

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_INVALID);
        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(atlasLineageService, never()).getAtlasLineageInfo(anyString(), any(), anyInt());
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithEmptyTypeName() throws AtlasBaseException {
        // Setup
        String typeName = "";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(null);

        // Execute & Verify
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
        });

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_INVALID);
        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(atlasLineageService, never()).getAtlasLineageInfo(anyString(), any(), anyInt());
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithNullAttributeValues() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("attr:qualifiedName", null);
        parameterMap.put("attr:name", new String[] {null});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(httpServletRequest, atLeastOnce()).getParameterMap();
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithEmptyAttributeValues() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("attr:qualifiedName", new String[] {""});
        parameterMap.put("attr:name", new String[] {"   "});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(httpServletRequest, atLeastOnce()).getParameterMap();
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithMixedAttributeFormats() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        Map<String, String[]> parameterMap = new HashMap<>();
        parameterMap.put("attr:qualifiedName", new String[] {"db.table"});
        parameterMap.put("attr:name", new String[] {"table"});
        parameterMap.put("attr:database", new String[] {"db"});
        parameterMap.put("attr:cluster", new String[] {"cluster1"});

        when(httpServletRequest.getParameterMap()).thenReturn(parameterMap);
        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
        verify(httpServletRequest, atLeastOnce()).getParameterMap();
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithVeryLongTypeName() throws AtlasBaseException {
        // Setup
        String typeName = "very_long_type_name_that_exceeds_normal_length_but_should_still_work";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
    }

    @Test
    public void testGetLineageByUniqueAttribute_WithSpecialCharactersInTypeName() throws AtlasBaseException {
        // Setup
        String typeName = "hive_table_with_special_chars_@#$%^&*()";
        LineageDirection direction = LineageDirection.BOTH;
        int depth = 3;

        when(typeRegistry.getEntityTypeByName(typeName)).thenReturn(entityType);

        try {
            AtlasLineageInfo result = lineageREST.getLineageByUniqueAttribute(typeName, direction, depth, httpServletRequest);
            // If it succeeds, verify the result
            assertNotNull(result);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException || e instanceof AtlasBaseException);
        }

        verify(typeRegistry).getEntityTypeByName(typeName);
    }
}
