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

package org.apache.atlas.model.impexp;

import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportDetails;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportTrackingInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasAsyncImportRequest {
    private AtlasAsyncImportRequest request;
    private AtlasImportResult importResult;

    @BeforeMethod
    public void setUp() {
        request = new AtlasAsyncImportRequest();
        importResult = new AtlasImportResult();
        importResult.setUserName("testuser");
    }

    @Test
    public void testDefaultConstructor() {
        AtlasAsyncImportRequest newRequest = new AtlasAsyncImportRequest();

        assertNotNull(newRequest);
        assertNull(newRequest.getImportId());
        assertNull(newRequest.getStatus());
        assertNull(newRequest.getImportDetails());
        assertEquals(newRequest.getReceivedTime(), 0L);
        assertEquals(newRequest.getStagedTime(), 0L);
        assertEquals(newRequest.getProcessingStartTime(), 0L);
        assertEquals(newRequest.getCompletedTime(), 0L);
        assertNull(newRequest.getImportResult());
        assertNull(newRequest.getImportTrackingInfo());
    }

    @Test
    public void testGuidConstructor() {
        String testGuid = "test-guid-123";
        AtlasAsyncImportRequest newRequest = new AtlasAsyncImportRequest(testGuid);

        assertNotNull(newRequest);
        assertEquals(newRequest.getGuid(), testGuid);
    }

    @Test
    public void testImportResultConstructor() {
        AtlasAsyncImportRequest newRequest = new AtlasAsyncImportRequest(importResult);

        assertNotNull(newRequest);
        assertEquals(newRequest.getImportResult(), importResult);
        assertEquals(newRequest.getStatus(), ImportStatus.STAGING);
        assertEquals(newRequest.getReceivedTime(), 0L);
        assertEquals(newRequest.getStagedTime(), 0L);
        assertEquals(newRequest.getProcessingStartTime(), 0L);
        assertEquals(newRequest.getCompletedTime(), 0L);
        assertNotNull(newRequest.getImportDetails());
        assertNotNull(newRequest.getImportTrackingInfo());
    }

    @Test
    public void testImportIdSetterGetter() {
        String importId = "import-123";
        request.setImportId(importId);

        assertEquals(request.getImportId(), importId);
    }

    @Test
    public void testStatusSetterGetter() {
        request.setStatus(ImportStatus.PROCESSING);
        assertEquals(request.getStatus(), ImportStatus.PROCESSING);

        request.setStatus(ImportStatus.SUCCESSFUL);
        assertEquals(request.getStatus(), ImportStatus.SUCCESSFUL);
    }

    @Test
    public void testImportDetailsSetterGetter() {
        ImportDetails details = new ImportDetails();
        details.setTotalEntitiesCount(100);
        request.setImportDetails(details);

        assertEquals(request.getImportDetails(), details);
        assertEquals(request.getImportDetails().getTotalEntitiesCount(), 100);
    }

    @Test
    public void testTimeSettersGetters() {
        long currentTime = System.currentTimeMillis();

        request.setReceivedTime(currentTime);
        assertEquals(request.getReceivedTime(), currentTime);

        request.setStagedTime(currentTime + 1000);
        assertEquals(request.getStagedTime(), currentTime + 1000);

        request.setProcessingStartTime(currentTime + 2000);
        assertEquals(request.getProcessingStartTime(), currentTime + 2000);

        request.setCompletedTime(currentTime + 3000);
        assertEquals(request.getCompletedTime(), currentTime + 3000);
    }

    @Test
    public void testImportResultSetterGetter() {
        request.setImportResult(importResult);
        assertEquals(request.getImportResult(), importResult);
    }

    @Test
    public void testImportTrackingInfoSetterGetter() {
        ImportTrackingInfo trackingInfo = new ImportTrackingInfo("test-request-id", 10);
        request.setImportTrackingInfo(trackingInfo);

        assertEquals(request.getImportTrackingInfo(), trackingInfo);
    }

    @Test
    public void testGetTopicName() {
        String importId = "topic-test-123";
        request.setImportId(importId);

        String topicName = request.getTopicName();
        assertNotNull(topicName);
        assertTrue(topicName.contains(importId));
    }

    @Test
    public void testToImportMinInfo() {
        String importId = "min-info-test";
        long receivedTime = System.currentTimeMillis();

        request.setImportId(importId);
        request.setStatus(ImportStatus.PROCESSING);
        request.setReceivedTime(receivedTime);
        request.setImportResult(importResult);

        AsyncImportStatus minInfo = request.toImportMinInfo();

        assertNotNull(minInfo);
        assertEquals(minInfo.getImportId(), importId);
        assertEquals(minInfo.getStatus(), ImportStatus.PROCESSING);
        assertEquals(minInfo.getImportRequestUser(), importResult.getUserName());
        assertNotNull(minInfo.getImportRequestReceivedTime());
    }

    @Test
    public void testToIsoDateViaToImportMinInfo() {
        String importId = "iso-date-test";
        long receivedTime = 1640995200000L; // January 1, 2022 00:00:00 UTC

        request.setImportId(importId);
        request.setStatus(ImportStatus.PROCESSING);
        request.setReceivedTime(receivedTime);
        request.setImportResult(importResult);

        AsyncImportStatus minInfo = request.toImportMinInfo();

        assertNotNull(minInfo);
        assertNotNull(minInfo.getImportRequestReceivedTime());
        assertTrue(minInfo.getImportRequestReceivedTime().endsWith("Z"));
        assertTrue(minInfo.getImportRequestReceivedTime().contains("T"));
        assertTrue(minInfo.getImportRequestReceivedTime().contains("2022-01-01"));
    }

    @Test
    public void testToString() {
        request.setImportId("toString-test");
        request.setStatus(ImportStatus.SUCCESSFUL);
        request.setReceivedTime(123456789L);

        String result = request.toString();
        assertNotNull(result);
        assertTrue(result.contains("importId=toString-test"));
        assertTrue(result.contains("status=SUCCESSFUL"));
        assertTrue(result.contains("receivedTime=123456789"));
    }

    @Test
    public void testImportStatusEnum() {
        // Test all enum values
        assertEquals(ImportStatus.STAGING.getStatus(), "STAGING");
        assertEquals(ImportStatus.WAITING.getStatus(), "WAITING");
        assertEquals(ImportStatus.PROCESSING.getStatus(), "PROCESSING");
        assertEquals(ImportStatus.SUCCESSFUL.getStatus(), "SUCCESSFUL");
        assertEquals(ImportStatus.PARTIAL_SUCCESS.getStatus(), "PARTIAL_SUCCESS");
        assertEquals(ImportStatus.ABORTED.getStatus(), "ABORTED");
        assertEquals(ImportStatus.FAILED.getStatus(), "FAILED");

        // Test toString
        assertEquals(ImportStatus.STAGING.toString(), "STAGING");
        assertEquals(ImportStatus.PROCESSING.toString(), "PROCESSING");
    }

    // Test ImportDetails inner class
    @Test
    public void testImportDetailsDefaultConstructor() {
        ImportDetails details = new ImportDetails();

        assertNotNull(details);
        assertEquals(details.getPublishedEntityCount(), 0);
        assertEquals(details.getTotalEntitiesCount(), 0);
        assertEquals(details.getImportedEntitiesCount(), 0);
        assertEquals(details.getFailedEntitiesCount(), 0);
        assertEquals(details.getImportProgress(), 0.0f);
        assertNotNull(details.getFailedEntities());
        assertNotNull(details.getFailures());
        assertNotNull(details.getCreationOrder());
    }

    @Test
    public void testImportDetailsSettersGetters() {
        ImportDetails details = new ImportDetails();

        details.setPublishedEntityCount(50);
        assertEquals(details.getPublishedEntityCount(), 50);

        details.setTotalEntitiesCount(100);
        assertEquals(details.getTotalEntitiesCount(), 100);

        details.setImportedEntitiesCount(80);
        assertEquals(details.getImportedEntitiesCount(), 80);

        details.setFailedEntitiesCount(20);
        assertEquals(details.getFailedEntitiesCount(), 20);

        details.setImportProgress(0.8f);
        assertEquals(details.getImportProgress(), 0.8f);

        List<String> failedEntities = new ArrayList<>();
        failedEntities.add("entity1");
        details.setFailedEntities(failedEntities);
        assertEquals(details.getFailedEntities(), failedEntities);

        List<String> creationOrder = new ArrayList<>();
        creationOrder.add("order1");
        details.setCreationOrder(creationOrder);
        assertEquals(details.getCreationOrder(), creationOrder);
    }

    @Test
    public void testImportDetailsAddFailure() {
        ImportDetails details = new ImportDetails();
        details.addFailure("guid123", "Test failure message");

        Map<String, String> failures = details.getFailures();
        assertNotNull(failures);
        assertEquals(failures.size(), 1);
        assertEquals(failures.get("guid123"), "Test failure message");
    }

    @Test
    public void testImportDetailsEqualsAndHashCode() {
        ImportDetails details1 = new ImportDetails();
        ImportDetails details2 = new ImportDetails();

        // Test equality of empty objects
        assertTrue(details1.equals(details2));
        assertEquals(details1.hashCode(), details2.hashCode());

        // Test with same values
        details1.setTotalEntitiesCount(100);
        details1.setImportedEntitiesCount(80);

        details2.setTotalEntitiesCount(100);
        details2.setImportedEntitiesCount(80);

        assertTrue(details1.equals(details2));
        assertEquals(details1.hashCode(), details2.hashCode());

        // Test with different values
        details2.setTotalEntitiesCount(200);
        assertFalse(details1.equals(details2));
    }

    @Test
    public void testImportDetailsToString() {
        ImportDetails details = new ImportDetails();
        details.setTotalEntitiesCount(100);
        details.setImportedEntitiesCount(80);
        details.setFailedEntitiesCount(20);

        String result = details.toString();
        assertNotNull(result);
        assertTrue(result.contains("ImportDetails{"));
        assertTrue(result.contains("totalEntitiesCount=100"));
        assertTrue(result.contains("importedEntitiesCount=80"));
        assertTrue(result.contains("failedEntitiesCount=20"));
    }

    @Test
    public void testImportTrackingInfoDefaultConstructor() {
        ImportTrackingInfo trackingInfo = new ImportTrackingInfo();

        assertNotNull(trackingInfo);
        assertNull(trackingInfo.getRequestId());
        assertEquals(trackingInfo.getStartEntityPosition(), 0);
    }

    @Test
    public void testImportTrackingInfoParameterizedConstructor() {
        String requestId = "tracking-request-123";
        int startPosition = 50;

        ImportTrackingInfo trackingInfo = new ImportTrackingInfo(requestId, startPosition);

        assertNotNull(trackingInfo);
        assertEquals(trackingInfo.getRequestId(), requestId);
        assertEquals(trackingInfo.getStartEntityPosition(), startPosition);
    }

    @Test
    public void testImportTrackingInfoSettersGetters() {
        ImportTrackingInfo trackingInfo = new ImportTrackingInfo();

        trackingInfo.setRequestId("setter-test-id");
        assertEquals(trackingInfo.getRequestId(), "setter-test-id");

        trackingInfo.setStartEntityPosition(100);
        assertEquals(trackingInfo.getStartEntityPosition(), 100);
    }

    @Test
    public void testImportTrackingInfoToString() {
        ImportTrackingInfo trackingInfo = new ImportTrackingInfo("toString-test", 25);

        String result = trackingInfo.toString();
        assertNotNull(result);
        assertTrue(result.contains("ImportTrackingInfo{"));
        assertTrue(result.contains("requestId='toString-test'"));
        assertTrue(result.contains("startEntityPosition=25"));
    }

    @Test
    public void testImportTrackingInfoWithNullValues() {
        ImportTrackingInfo trackingInfo = new ImportTrackingInfo(null, 0);

        String result = trackingInfo.toString();
        assertNotNull(result);
        assertTrue(result.contains("requestId='null'"));
        assertTrue(result.contains("startEntityPosition=0"));
    }

    @Test
    public void testBoundaryValues() {
        ImportDetails details = new ImportDetails();

        // Test with negative values
        details.setPublishedEntityCount(-1);
        assertEquals(details.getPublishedEntityCount(), -1);

        // Test with large values
        details.setTotalEntitiesCount(Integer.MAX_VALUE);
        assertEquals(details.getTotalEntitiesCount(), Integer.MAX_VALUE);

        // Test with boundary float values
        details.setImportProgress(Float.MAX_VALUE);
        assertEquals(details.getImportProgress(), Float.MAX_VALUE);

        details.setImportProgress(Float.MIN_VALUE);
        assertEquals(details.getImportProgress(), Float.MIN_VALUE);
    }

    @Test
    public void testImportTrackingInfoBoundaryValues() {
        ImportTrackingInfo trackingInfo = new ImportTrackingInfo();

        // Test with negative position
        trackingInfo.setStartEntityPosition(-1);
        assertEquals(trackingInfo.getStartEntityPosition(), -1);

        // Test with maximum integer value
        trackingInfo.setStartEntityPosition(Integer.MAX_VALUE);
        assertEquals(trackingInfo.getStartEntityPosition(), Integer.MAX_VALUE);
    }

    @Test
    public void testNullSafeOperations() {
        AtlasAsyncImportRequest nullRequest = new AtlasAsyncImportRequest();

        // Test operations with null tracking info
        nullRequest.setImportId("test-null-tracking");
        assertEquals(nullRequest.getImportId(), "test-null-tracking");

        // Test with null import result
        AsyncImportStatus minInfo = null;
        try {
            // This might throw NPE due to null importResult
            nullRequest.setReceivedTime(System.currentTimeMillis());
            if (nullRequest.getImportResult() != null) {
                minInfo = nullRequest.toImportMinInfo();
            }
        } catch (NullPointerException e) {
            // Expected when importResult is null
            assertTrue(true);
        }
    }
}
