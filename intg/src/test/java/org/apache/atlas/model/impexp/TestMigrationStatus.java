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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestMigrationStatus {
    private MigrationStatus migrationStatus;

    @BeforeMethod
    public void setUp() {
        migrationStatus = new MigrationStatus();
    }

    @Test
    public void testDefaultConstructor() {
        MigrationStatus status = new MigrationStatus();

        assertNotNull(status);
        assertNull(status.getOperationStatus());
        assertNull(status.getStartTime());
        assertNull(status.getEndTime());
        assertEquals(status.getCurrentIndex(), 0L);
        assertEquals(status.getCurrentCounter(), Long.valueOf(0L));
        assertEquals(status.getTotalCount(), 0L);
    }

    @Test
    public void testOperationStatusSetterGetter() {
        String operationStatus = "IN_PROGRESS";
        migrationStatus.setOperationStatus(operationStatus);
        assertEquals(migrationStatus.getOperationStatus(), operationStatus);

        migrationStatus.setOperationStatus(null);
        assertNull(migrationStatus.getOperationStatus());
    }

    @Test
    public void testOperationStatusWithDifferentValues() {
        String[] statuses = {"STARTED", "IN_PROGRESS", "COMPLETED", "FAILED", "CANCELLED", "PAUSED"};
        for (String status : statuses) {
            migrationStatus.setOperationStatus(status);
            assertEquals(migrationStatus.getOperationStatus(), status);
        }
    }

    @Test
    public void testStartTimeSetterGetter() {
        Date startTime = new Date();
        migrationStatus.setStartTime(startTime);
        assertEquals(migrationStatus.getStartTime(), startTime);

        migrationStatus.setStartTime(null);
        assertNull(migrationStatus.getStartTime());
    }

    @Test
    public void testEndTimeSetterGetter() {
        Date endTime = new Date();
        migrationStatus.setEndTime(endTime);
        assertEquals(migrationStatus.getEndTime(), endTime);

        migrationStatus.setEndTime(null);
        assertNull(migrationStatus.getEndTime());
    }

    @Test
    public void testCurrentIndexSetterGetter() {
        long currentIndex = 150L;
        migrationStatus.setCurrentIndex(currentIndex);
        assertEquals(migrationStatus.getCurrentIndex(), currentIndex);

        migrationStatus.setCurrentIndex(0L);
        assertEquals(migrationStatus.getCurrentIndex(), 0L);

        migrationStatus.setCurrentIndex(Long.MAX_VALUE);
        assertEquals(migrationStatus.getCurrentIndex(), Long.MAX_VALUE);
    }

    @Test
    public void testTotalCountSetterGetter() {
        long totalCount = 1000L;
        migrationStatus.setTotalCount(totalCount);
        assertEquals(migrationStatus.getTotalCount(), totalCount);

        migrationStatus.setTotalCount(0L);
        assertEquals(migrationStatus.getTotalCount(), 0L);

        migrationStatus.setTotalCount(Long.MAX_VALUE);
        assertEquals(migrationStatus.getTotalCount(), Long.MAX_VALUE);
    }

    @Test
    public void testCurrentCounterSetterGetter() {
        long currentCounter = 500L;
        migrationStatus.setCurrentCounter(currentCounter);
        assertEquals(migrationStatus.getCurrentCounter(), Long.valueOf(currentCounter));

        migrationStatus.setCurrentCounter(0L);
        assertEquals(migrationStatus.getCurrentCounter(), Long.valueOf(0L));

        migrationStatus.setCurrentCounter(Long.MAX_VALUE);
        assertEquals(migrationStatus.getCurrentCounter(), Long.valueOf(Long.MAX_VALUE));
    }

    @Test
    public void testTimeSequence() {
        Date startTime = new Date();
        // Sleep for a small amount to ensure end time is after start time
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Date endTime = new Date();

        migrationStatus.setStartTime(startTime);
        migrationStatus.setEndTime(endTime);

        assertEquals(migrationStatus.getStartTime(), startTime);
        assertEquals(migrationStatus.getEndTime(), endTime);
        assertTrue(migrationStatus.getEndTime().getTime() >= migrationStatus.getStartTime().getTime());
    }

    @Test
    public void testProgressCalculation() {
        // Set up a scenario where we can calculate progress
        migrationStatus.setTotalCount(1000L);
        migrationStatus.setCurrentIndex(250L);
        migrationStatus.setCurrentCounter(250L);

        assertEquals(migrationStatus.getTotalCount(), 1000L);
        assertEquals(migrationStatus.getCurrentIndex(), 250L);
        assertEquals(migrationStatus.getCurrentCounter(), Long.valueOf(250L));

        // Verify progress makes sense (25% complete)
        double progress = (double) migrationStatus.getCurrentIndex() / migrationStatus.getTotalCount();
        assertEquals(progress, 0.25, 0.001);
    }

    @Test
    public void testBoundaryValues() {
        // Test with maximum values
        migrationStatus.setCurrentIndex(Long.MAX_VALUE);
        migrationStatus.setCurrentCounter(Long.MAX_VALUE);
        migrationStatus.setTotalCount(Long.MAX_VALUE);

        assertEquals(migrationStatus.getCurrentIndex(), Long.MAX_VALUE);
        assertEquals(migrationStatus.getCurrentCounter(), Long.valueOf(Long.MAX_VALUE));
        assertEquals(migrationStatus.getTotalCount(), Long.MAX_VALUE);

        // Test with minimum values
        migrationStatus.setCurrentIndex(Long.MIN_VALUE);
        migrationStatus.setCurrentCounter(Long.MIN_VALUE);
        migrationStatus.setTotalCount(Long.MIN_VALUE);

        assertEquals(migrationStatus.getCurrentIndex(), Long.MIN_VALUE);
        assertEquals(migrationStatus.getCurrentCounter(), Long.valueOf(Long.MIN_VALUE));
        assertEquals(migrationStatus.getTotalCount(), Long.MIN_VALUE);
    }

    @Test
    public void testNegativeValues() {
        // Test with negative values (edge case)
        migrationStatus.setCurrentIndex(-1L);
        migrationStatus.setCurrentCounter(-1L);
        migrationStatus.setTotalCount(-1L);

        assertEquals(migrationStatus.getCurrentIndex(), -1L);
        assertEquals(migrationStatus.getCurrentCounter(), Long.valueOf(-1L));
        assertEquals(migrationStatus.getTotalCount(), -1L);
    }

    @Test
    public void testToString() {
        migrationStatus.setOperationStatus("IN_PROGRESS");
        Date startTime = new Date(1640995200000L); // January 1, 2022 00:00:00 GMT
        Date endTime = new Date(1640995260000L);   // January 1, 2022 00:01:00 GMT
        migrationStatus.setStartTime(startTime);
        migrationStatus.setEndTime(endTime);
        migrationStatus.setCurrentIndex(500L);
        migrationStatus.setCurrentCounter(500L);
        migrationStatus.setTotalCount(1000L);

        StringBuilder result = migrationStatus.toString(new StringBuilder());
        assertNotNull(result);
        String resultString = result.toString();
        assertTrue(resultString.contains("operationStatus=IN_PROGRESS"));
        assertTrue(resultString.contains("startTime=" + startTime));
        assertTrue(resultString.contains("endTime=" + endTime));
        assertTrue(resultString.contains("currentIndex=500"));
        assertTrue(resultString.contains("currentCounter=500"));
        assertTrue(resultString.contains("totalCount=1000"));
    }

    @Test
    public void testToStringWithNullValues() {
        migrationStatus.setOperationStatus(null);
        migrationStatus.setStartTime(null);
        migrationStatus.setEndTime(null);

        StringBuilder result = migrationStatus.toString(new StringBuilder());
        assertNotNull(result);
        String resultString = result.toString();
        assertTrue(resultString.contains("operationStatus=null"));
        assertTrue(resultString.contains("startTime=null"));
        assertTrue(resultString.contains("endTime=null"));
        assertTrue(resultString.contains("currentIndex=0"));
        assertTrue(resultString.contains("currentCounter=0"));
        assertTrue(resultString.contains("totalCount=0"));
    }

    @Test
    public void testToStringWithEmptyStatus() {
        migrationStatus.setOperationStatus("");
        StringBuilder result = migrationStatus.toString(new StringBuilder());
        assertNotNull(result);
        String resultString = result.toString();
        assertTrue(resultString.contains("operationStatus="));
    }

    @Test
    public void testSpecialCharactersInOperationStatus() {
        String specialStatus = "STATUS-с-кириллицей-字符-!@#$%^&*()";
        migrationStatus.setOperationStatus(specialStatus);
        assertEquals(migrationStatus.getOperationStatus(), specialStatus);

        StringBuilder result = migrationStatus.toString(new StringBuilder());
        assertTrue(result.toString().contains("operationStatus=" + specialStatus));
    }

    @Test
    public void testLongOperationStatus() {
        StringBuilder longStatusBuilder = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            longStatusBuilder.append("VERY_LONG_OPERATION_STATUS_");
        }
        String longStatus = longStatusBuilder.toString();
        migrationStatus.setOperationStatus(longStatus);
        assertEquals(migrationStatus.getOperationStatus(), longStatus);

        StringBuilder result = migrationStatus.toString(new StringBuilder());
        assertTrue(result.toString().contains("operationStatus=" + longStatus));
    }

    @Test
    public void testHistoricalDates() {
        // Test with dates from different eras
        Date veryOldDate = new Date(0L); // January 1, 1970 00:00:00 GMT
        Date futureDate = new Date(Long.MAX_VALUE); // Far future date

        migrationStatus.setStartTime(veryOldDate);
        migrationStatus.setEndTime(futureDate);

        assertEquals(migrationStatus.getStartTime(), veryOldDate);
        assertEquals(migrationStatus.getEndTime(), futureDate);
    }

    @Test
    public void testComplexMigrationWorkflow() {
        // Simulate a complete migration workflow
        // Phase 1: Migration started
        migrationStatus.setOperationStatus("STARTED");
        migrationStatus.setStartTime(new Date());
        migrationStatus.setTotalCount(10000L);
        migrationStatus.setCurrentIndex(0L);
        migrationStatus.setCurrentCounter(0L);

        assertEquals(migrationStatus.getOperationStatus(), "STARTED");
        assertEquals(migrationStatus.getCurrentIndex(), 0L);
        // Phase 2: Migration in progress
        migrationStatus.setOperationStatus("IN_PROGRESS");
        migrationStatus.setCurrentIndex(2500L);
        migrationStatus.setCurrentCounter(2500L);

        assertEquals(migrationStatus.getOperationStatus(), "IN_PROGRESS");
        assertEquals(migrationStatus.getCurrentIndex(), 2500L);
        assertTrue(migrationStatus.getCurrentIndex() < migrationStatus.getTotalCount());
        // Phase 3: Migration halfway
        migrationStatus.setCurrentIndex(5000L);
        migrationStatus.setCurrentCounter(5000L);

        assertEquals(migrationStatus.getCurrentIndex(), 5000L);
        assertEquals(migrationStatus.getCurrentIndex(), migrationStatus.getTotalCount() / 2);
        // Phase 4: Migration nearly complete
        migrationStatus.setCurrentIndex(9900L);
        migrationStatus.setCurrentCounter(9900L);

        assertTrue(migrationStatus.getCurrentIndex() > migrationStatus.getTotalCount() * 0.9);
        // Phase 5: Migration completed
        migrationStatus.setOperationStatus("COMPLETED");
        migrationStatus.setCurrentIndex(10000L);
        migrationStatus.setCurrentCounter(10000L);
        migrationStatus.setEndTime(new Date());

        assertEquals(migrationStatus.getOperationStatus(), "COMPLETED");
        assertEquals(migrationStatus.getCurrentIndex(), migrationStatus.getTotalCount());
        assertNotNull(migrationStatus.getEndTime());
        assertTrue(migrationStatus.getEndTime().getTime() >= migrationStatus.getStartTime().getTime());
    }

    @Test
    public void testMigrationWithFailure() {
        // Simulate a migration that fails partway through
        migrationStatus.setOperationStatus("STARTED");
        migrationStatus.setStartTime(new Date());
        migrationStatus.setTotalCount(5000L);
        migrationStatus.setCurrentIndex(0L);
        migrationStatus.setCurrentCounter(0L);

        // Progress partway
        migrationStatus.setOperationStatus("IN_PROGRESS");
        migrationStatus.setCurrentIndex(1200L);
        migrationStatus.setCurrentCounter(1200L);

        // Failure occurs
        migrationStatus.setOperationStatus("FAILED");
        migrationStatus.setEndTime(new Date());

        assertEquals(migrationStatus.getOperationStatus(), "FAILED");
        assertTrue(migrationStatus.getCurrentIndex() < migrationStatus.getTotalCount());
        assertNotNull(migrationStatus.getEndTime());
    }

    @Test
    public void testMigrationPauseResume() {
        // Simulate a migration that gets paused and resumed
        migrationStatus.setOperationStatus("STARTED");
        migrationStatus.setStartTime(new Date());
        migrationStatus.setTotalCount(8000L);
        // Progress to partway
        migrationStatus.setOperationStatus("IN_PROGRESS");
        migrationStatus.setCurrentIndex(3000L);
        migrationStatus.setCurrentCounter(3000L);

        // Pause migration
        migrationStatus.setOperationStatus("PAUSED");
        assertEquals(migrationStatus.getOperationStatus(), "PAUSED");
        assertEquals(migrationStatus.getCurrentIndex(), 3000L);

        // Resume migration
        migrationStatus.setOperationStatus("IN_PROGRESS");
        migrationStatus.setCurrentIndex(6000L);
        migrationStatus.setCurrentCounter(6000L);

        assertEquals(migrationStatus.getOperationStatus(), "IN_PROGRESS");
        assertEquals(migrationStatus.getCurrentIndex(), 6000L);
        // Complete migration
        migrationStatus.setOperationStatus("COMPLETED");
        migrationStatus.setCurrentIndex(8000L);
        migrationStatus.setCurrentCounter(8000L);
        migrationStatus.setEndTime(new Date());

        assertEquals(migrationStatus.getOperationStatus(), "COMPLETED");
        assertEquals(migrationStatus.getCurrentIndex(), migrationStatus.getTotalCount());
    }

    @Test
    public void testZeroTotalCount() {
        // Edge case: migration with zero total count
        migrationStatus.setOperationStatus("COMPLETED");
        migrationStatus.setTotalCount(0L);
        migrationStatus.setCurrentIndex(0L);
        migrationStatus.setCurrentCounter(0L);

        assertEquals(migrationStatus.getTotalCount(), 0L);
        assertEquals(migrationStatus.getCurrentIndex(), 0L);
        assertEquals(migrationStatus.getCurrentCounter(), Long.valueOf(0L));
    }

    @Test
    public void testCurrentIndexExceedsTotalCount() {
        // Edge case: current index exceeds total count
        migrationStatus.setTotalCount(1000L);
        migrationStatus.setCurrentIndex(1500L); // More than total
        migrationStatus.setCurrentCounter(1500L);

        assertEquals(migrationStatus.getTotalCount(), 1000L);
        assertEquals(migrationStatus.getCurrentIndex(), 1500L);
        assertTrue(migrationStatus.getCurrentIndex() > migrationStatus.getTotalCount());
    }

    @Test
    public void testWhitespaceInOperationStatus() {
        String statusWithSpaces = "  IN_PROGRESS  ";
        migrationStatus.setOperationStatus(statusWithSpaces);
        assertEquals(migrationStatus.getOperationStatus(), statusWithSpaces);

        StringBuilder result = migrationStatus.toString(new StringBuilder());
        assertTrue(result.toString().contains("operationStatus=" + statusWithSpaces));
    }

    @Test
    public void testMultilineOperationStatus() {
        String multilineStatus = "COMPLEX_OPERATION\nWITH_DETAILS\nAND_PROGRESS";
        migrationStatus.setOperationStatus(multilineStatus);
        assertEquals(migrationStatus.getOperationStatus(), multilineStatus);

        StringBuilder result = migrationStatus.toString(new StringBuilder());
        assertTrue(result.toString().contains(multilineStatus));
    }

    @Test
    public void testEmptyStringOperationStatus() {
        migrationStatus.setOperationStatus("");
        assertEquals(migrationStatus.getOperationStatus(), "");

        StringBuilder result = migrationStatus.toString(new StringBuilder());
        assertTrue(result.toString().contains("operationStatus="));
    }
}
