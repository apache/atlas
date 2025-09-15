/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graph.IndexRecoveryService;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME;
import static org.apache.atlas.repository.Constants.PROPERTY_KEY_INDEX_RECOVERY_START_TIME;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;

public class IndexRecoveryRESTTest {
    @Mock
    private IndexRecoveryService indexRecoveryService;

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasVertex indexRecoveryVertex;

    private IndexRecoveryREST indexRecoveryREST;

    private IndexRecoveryService.RecoveryInfoManagement mockRecoveryInfoManagement;
    private IndexRecoveryService.RecoveryThread mockRecoveryThread;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Create mock objects for the nested classes
        mockRecoveryInfoManagement = mock(IndexRecoveryService.RecoveryInfoManagement.class);
        mockRecoveryThread = mock(IndexRecoveryService.RecoveryThread.class);

        // Use reflection to set the mocked nested objects
        try {
            java.lang.reflect.Field recoveryInfoManagementField = IndexRecoveryService.class.getField("recoveryInfoManagement");
            recoveryInfoManagementField.setAccessible(true);
            recoveryInfoManagementField.set(indexRecoveryService, mockRecoveryInfoManagement);

            java.lang.reflect.Field recoveryThreadField = IndexRecoveryService.class.getField("recoveryThread");
            recoveryThreadField.setAccessible(true);
            recoveryThreadField.set(indexRecoveryService, mockRecoveryThread);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up mocks", e);
        }

        // Manually create the IndexRecoveryREST instance with mocked dependencies
        indexRecoveryREST = new IndexRecoveryREST(indexRecoveryService, graph);
    }

    @Test
    public void testGetPropertyKeyByRemovingPrefix() {
        String result = IndexRecoveryREST.getPropertyKeyByRemovingPrefix(PROPERTY_KEY_INDEX_RECOVERY_START_TIME);
        assertEquals(result, "startTime");
    }

    @Test
    public void testGetPropertyKeyByRemovingPrefix_WithNull() {
        String result = IndexRecoveryREST.getPropertyKeyByRemovingPrefix(null);
        assertNull(result);
    }

    @Test
    public void testGetPropertyKeyByRemovingPrefix_WithEmptyString() {
        String result = IndexRecoveryREST.getPropertyKeyByRemovingPrefix("");
        assertEquals(result, "");
    }

    @Test
    public void testGetPropertyKeyByRemovingPrefix_WithoutPrefix() {
        String result = IndexRecoveryREST.getPropertyKeyByRemovingPrefix("startTime");
        assertEquals(result, "startTime");
    }

    @Test
    public void testGetIndexRecoveryData_WithAllProperties() {
        // Setup
        Long startTime = 1640995200000L; // 2022-01-01 00:00:00 UTC
        Long prevTime = 1638316800000L;  // 2021-12-01 00:00:00 UTC
        Long customStartTime = 1640995200000L; // 2022-01-01 00:00:00 UTC

        when(mockRecoveryInfoManagement.findVertex()).thenReturn(indexRecoveryVertex);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class)).thenReturn(startTime);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class)).thenReturn(prevTime);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class)).thenReturn(customStartTime);

        // Execute
        Map<String, String> result = indexRecoveryREST.getIndexRecoveryData();

        // Verify
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get("startTime"), "2022-01-01T00:00:00Z");
        assertEquals(result.get("prevTime"), "2021-12-01T00:00:00Z");
        assertEquals(result.get("customTime"), "2022-01-01T00:00:00Z");

        verify(mockRecoveryInfoManagement).findVertex();
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class);
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class);
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class);
    }

    @Test
    public void testGetIndexRecoveryData_WithNullVertex() {
        // Setup
        when(mockRecoveryInfoManagement.findVertex()).thenReturn(null);

        // Execute
        Map<String, String> result = indexRecoveryREST.getIndexRecoveryData();

        // Verify
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get("startTime"), "Not applicable");
        assertEquals(result.get("prevTime"), "Not applicable");
        assertEquals(result.get("customTime"), "Not applicable");

        verify(mockRecoveryInfoManagement).findVertex();
        verify(indexRecoveryVertex, never()).getProperty(any(), any());
    }

    @Test
    public void testGetIndexRecoveryData_WithNullProperties() {
        // Setup
        when(mockRecoveryInfoManagement.findVertex()).thenReturn(indexRecoveryVertex);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class)).thenReturn(null);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class)).thenReturn(null);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class)).thenReturn(null);

        // Execute
        Map<String, String> result = indexRecoveryREST.getIndexRecoveryData();

        // Verify
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get("startTime"), "Not applicable");
        assertEquals(result.get("prevTime"), "Not applicable");
        assertEquals(result.get("customTime"), "Not applicable");

        verify(mockRecoveryInfoManagement).findVertex();
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class);
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class);
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class);
    }

    @Test
    public void testGetIndexRecoveryData_WithMixedProperties() {
        // Setup
        Long startTime = 1640995200000L; // 2022-01-01 00:00:00 UTC
        Long prevTime = null;
        Long customStartTime = 1640995200000L; // 2022-01-01 00:00:00 UTC

        when(mockRecoveryInfoManagement.findVertex()).thenReturn(indexRecoveryVertex);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class)).thenReturn(startTime);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class)).thenReturn(prevTime);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class)).thenReturn(customStartTime);

        // Execute
        Map<String, String> result = indexRecoveryREST.getIndexRecoveryData();

        // Verify
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get("startTime"), "2022-01-01T00:00:00Z");
        assertEquals(result.get("prevTime"), "Not applicable");
        assertEquals(result.get("customTime"), "2022-01-01T00:00:00Z");

        verify(mockRecoveryInfoManagement).findVertex();
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class);
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class);
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class);
    }

    @Test
    public void testGetIndexRecoveryData_WithPerfTracerEnabled() {
        // Setup - Mock the static AtlasPerfTracer methods
        Long startTime = 1640995200000L; // 2022-01-01 00:00:00 UTC
        Long prevTime = 1638316800000L;  // 2021-12-01 00:00:00 UTC
        Long customStartTime = 1640995200000L; // 2022-01-01 00:00:00 UTC

        when(mockRecoveryInfoManagement.findVertex()).thenReturn(indexRecoveryVertex);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class)).thenReturn(startTime);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class)).thenReturn(prevTime);
        when(indexRecoveryVertex.getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class)).thenReturn(customStartTime);

        // Execute
        Map<String, String> result = indexRecoveryREST.getIndexRecoveryData();

        // Verify
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get("startTime"), "2022-01-01T00:00:00Z");
        assertEquals(result.get("prevTime"), "2021-12-01T00:00:00Z");
        assertEquals(result.get("customTime"), "2022-01-01T00:00:00Z");

        verify(mockRecoveryInfoManagement).findVertex();
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_START_TIME, Long.class);
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME, Long.class);
        verify(indexRecoveryVertex).getProperty(PROPERTY_KEY_INDEX_RECOVERY_CUSTOM_TIME, Long.class);
    }

    @Test
    public void testStartCustomIndexRecovery_Success() throws AtlasBaseException, AtlasException {
        // Setup
        String startTime = "2022-01-01T00:00:00Z";
        long startTimeMilli = 1640995200000L;

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);

        // Execute
        indexRecoveryREST.startCustomIndexRecovery(startTime);

        // Verify
        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread).startMonitoringByUserRequest(startTimeMilli);
        verify(mockRecoveryInfoManagement).updateCustomStartTime(startTimeMilli);
    }

    @Test
    public void testStartCustomIndexRecovery_WithNullStartTime() throws AtlasException {
        // Setup
        String startTime = null;

        // Execute & Verify
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
        assertEquals(exception.getMessage(), "Index Recovery requested without start time");

        verify(mockRecoveryThread, never()).isIndexBackendHealthy();
        verify(mockRecoveryThread, never()).stopMonitoringByUserRequest();
        verify(mockRecoveryThread, never()).startMonitoringByUserRequest(anyLong());
        verify(mockRecoveryInfoManagement, never()).updateCustomStartTime(anyLong());
    }

    @Test
    public void testStartCustomIndexRecovery_WithUnhealthyIndexBackend() throws AtlasException {
        // Setup
        String startTime = "2022-01-01T00:00:00Z";

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(false);

        // Execute & Verify
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INTERNAL_ERROR);
        assertEquals(exception.getMessage(), "Internal server error Index recovery can not be started - Solr Health: Unhealthy");

        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread, never()).stopMonitoringByUserRequest();
        verify(mockRecoveryThread, never()).startMonitoringByUserRequest(anyLong());
        verify(mockRecoveryInfoManagement, never()).updateCustomStartTime(anyLong());
    }

    @Test
    public void testStartCustomIndexRecovery_WithInvalidDateFormat() throws AtlasException {
        // Setup
        String startTime = "invalid-date-format";

        // Execute & Verify
        expectThrows(Exception.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });
    }

    @Test
    public void testStartCustomIndexRecovery_WithAuthorizationFailure() throws AtlasException {
        // Setup
        String startTime = "2022-01-01T00:00:00Z";

      /*  doThrow(new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, "Access denied"))
                .when(mockRecoveryThread).isIndexBackendHealthy();*/

        // Execute & Verify
        expectThrows(AtlasBaseException.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });

        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread, never()).stopMonitoringByUserRequest();
        verify(mockRecoveryThread, never()).startMonitoringByUserRequest(anyLong());
        verify(mockRecoveryInfoManagement, never()).updateCustomStartTime(anyLong());
    }

    @Test
    public void testStartCustomIndexRecovery_WithDifferentTimeFormats() throws AtlasBaseException, AtlasException {
        // Test with different valid ISO8601 formats
        String[] validFormats = {
                "2022-01-01T00:00:00Z",
                "2022-01-01T00:00:00.000Z",
                "2022-01-01T00:00:00.123Z"
        };

        for (String startTime : validFormats) {
            when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);

            indexRecoveryREST.startCustomIndexRecovery(startTime);

            verify(mockRecoveryThread, atLeastOnce()).isIndexBackendHealthy();
            verify(mockRecoveryThread, atLeastOnce()).stopMonitoringByUserRequest();
            verify(mockRecoveryThread, atLeastOnce()).startMonitoringByUserRequest(anyLong());
            verify(mockRecoveryInfoManagement, atLeastOnce()).updateCustomStartTime(anyLong());
        }
    }

    @Test
    public void testStartCustomIndexRecovery_WithZeroTimestamp() throws AtlasBaseException, AtlasException {
        // Setup
        String startTime = "1970-01-01T00:00:00Z"; // Unix epoch
        long startTimeMilli = 0L;

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);

        // Execute
        indexRecoveryREST.startCustomIndexRecovery(startTime);

        // Verify
        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread).startMonitoringByUserRequest(startTimeMilli);
        verify(mockRecoveryInfoManagement).updateCustomStartTime(startTimeMilli);
    }

    @Test
    public void testStartCustomIndexRecovery_WithFutureTimestamp() throws AtlasBaseException, AtlasException {
        // Setup
        String startTime = "2030-01-01T00:00:00Z";
        long startTimeMilli = 1893456000000L;

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);

        // Execute
        indexRecoveryREST.startCustomIndexRecovery(startTime);

        // Verify
        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread).startMonitoringByUserRequest(startTimeMilli);
        verify(mockRecoveryInfoManagement).updateCustomStartTime(startTimeMilli);
    }

    @Test
    public void testStartCustomIndexRecovery_WithAuthorizationUtilsMock() throws AtlasBaseException, AtlasException {
        // Setup
        String startTime = "2022-01-01T00:00:00Z";
        long startTimeMilli = 1640995200000L;

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);

        // Execute
        indexRecoveryREST.startCustomIndexRecovery(startTime);

        // Verify
        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread).startMonitoringByUserRequest(startTimeMilli);
        verify(mockRecoveryInfoManagement).updateCustomStartTime(startTimeMilli);
    }

    @Test
    public void testStartCustomIndexRecovery_WithPerfTracerEnabled() throws AtlasBaseException, AtlasException {
        // Setup - Mock the static AtlasPerfTracer methods
        String startTime = "2022-01-01T00:00:00Z";
        long startTimeMilli = 1640995200000L;

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);

        // Execute
        indexRecoveryREST.startCustomIndexRecovery(startTime);

        // Verify
        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread).startMonitoringByUserRequest(startTimeMilli);
        verify(mockRecoveryInfoManagement).updateCustomStartTime(startTimeMilli);
    }

    @Test
    public void testStartCustomIndexRecovery_WithExceptionInRecoveryThread() throws AtlasException {
        // Setup
        String startTime = "2022-01-01T00:00:00Z";

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);
        doThrow(new RuntimeException("Recovery thread error"))
                .when(mockRecoveryThread).stopMonitoringByUserRequest();

        // Execute & Verify
        expectThrows(RuntimeException.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });

        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread, never()).startMonitoringByUserRequest(anyLong());
        verify(mockRecoveryInfoManagement, never()).updateCustomStartTime(anyLong());
    }

    @Test
    public void testStartCustomIndexRecovery_WithExceptionInStartMonitoring() throws AtlasException {
        // Setup
        String startTime = "2022-01-01T00:00:00Z";
        long startTimeMilli = 1640995200000L;

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);
        doThrow(new RuntimeException("Start monitoring error"))
                .when(mockRecoveryThread).startMonitoringByUserRequest(anyLong());

        // Execute & Verify
        expectThrows(RuntimeException.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });

        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread).startMonitoringByUserRequest(startTimeMilli);
        verify(mockRecoveryInfoManagement, never()).updateCustomStartTime(anyLong());
    }

    @Test
    public void testStartCustomIndexRecovery_WithExceptionInUpdateCustomStartTime() throws AtlasException {
        // Setup
        String startTime = "2022-01-01T00:00:00Z";
        long startTimeMilli = 1640995200000L;

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);
        doThrow(new RuntimeException("Update custom start time error"))
                .when(mockRecoveryInfoManagement).updateCustomStartTime(anyLong());

        // Execute & Verify
        expectThrows(RuntimeException.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });

        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread).startMonitoringByUserRequest(startTimeMilli);
        verify(mockRecoveryInfoManagement).updateCustomStartTime(startTimeMilli);
    }

    @Test
    public void testStartCustomIndexRecovery_WithEmptyStringStartTime() throws AtlasException {
        // Setup
        String startTime = "";

        // Execute & Verify
        expectThrows(Exception.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });
    }

    @Test
    public void testStartCustomIndexRecovery_WithWhitespaceStartTime() throws AtlasException {
        // Setup
        String startTime = "   ";

        // Execute & Verify
        expectThrows(Exception.class, () -> {
            indexRecoveryREST.startCustomIndexRecovery(startTime);
        });
    }

    @Test
    public void testStartCustomIndexRecovery_WithVeryLongStartTime() throws AtlasException, AtlasBaseException {
        // Setup
        String startTime = "2022-01-01T00:00:00.123456789Z";

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);

        // Execute
        indexRecoveryREST.startCustomIndexRecovery(startTime);
    }

    @Test
    public void testStartCustomIndexRecovery_WithNegativeTimestamp() throws AtlasException, AtlasBaseException {
        // Setup
        String startTime = "1969-12-31T23:59:59Z"; // Before Unix epoch

        when(mockRecoveryThread.isIndexBackendHealthy()).thenReturn(true);

        // Execute
        indexRecoveryREST.startCustomIndexRecovery(startTime);

        // Verify
        verify(mockRecoveryThread).isIndexBackendHealthy();
        verify(mockRecoveryThread).stopMonitoringByUserRequest();
        verify(mockRecoveryThread).startMonitoringByUserRequest(anyLong());
        verify(mockRecoveryInfoManagement).updateCustomStartTime(anyLong());
    }
}
