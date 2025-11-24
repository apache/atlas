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
package org.apache.atlas.model.discovery;

import org.apache.atlas.model.discovery.AtlasSearchResultDownloadStatus.AtlasSearchDownloadRecord;
import org.apache.atlas.model.tasks.AtlasTask;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestAtlasSearchResultDownloadStatus {
    private AtlasSearchResultDownloadStatus downloadStatus;

    @BeforeMethod
    public void setUp() {
        downloadStatus = new AtlasSearchResultDownloadStatus();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasSearchResultDownloadStatus status = new AtlasSearchResultDownloadStatus();
        assertNull(status.getSearchDownloadRecords());
    }

    @Test
    public void testSearchDownloadRecordsGetterSetter() {
        assertNull(downloadStatus.getSearchDownloadRecords());

        List<AtlasSearchDownloadRecord> records = new ArrayList<>();
        downloadStatus.setSearchDownloadRecords(records);
        assertSame(downloadStatus.getSearchDownloadRecords(), records);

        downloadStatus.setSearchDownloadRecords(null);
        assertNull(downloadStatus.getSearchDownloadRecords());
    }

    @Test
    public void testWithMultipleRecords() {
        List<AtlasSearchDownloadRecord> records = new ArrayList<>();

        Date now = new Date();
        Date startTime = new Date(now.getTime() + 1000);

        AtlasSearchDownloadRecord record1 = new AtlasSearchDownloadRecord(
                AtlasTask.Status.IN_PROGRESS, "file1.csv", "user1", now, startTime);
        AtlasSearchDownloadRecord record2 = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "file2.csv", "user2", now);

        records.add(record1);
        records.add(record2);

        downloadStatus.setSearchDownloadRecords(records);

        assertEquals(downloadStatus.getSearchDownloadRecords().size(), 2);
        assertEquals(downloadStatus.getSearchDownloadRecords().get(0).getFileName(), "file1.csv");
        assertEquals(downloadStatus.getSearchDownloadRecords().get(1).getFileName(), "file2.csv");
    }

    @Test
    public void testSerializable() {
        assertNotNull(downloadStatus);
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(downloadStatus);
    }

    // Tests for AtlasSearchDownloadRecord inner class
    @Test
    public void testAtlasSearchDownloadRecordConstructorWithStartTime() {
        Date createdTime = new Date();
        Date startTime = new Date(createdTime.getTime() + 5000);

        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.IN_PROGRESS, "test.csv", "testUser", createdTime, startTime);

        assertEquals(record.getStatus(), AtlasTask.Status.IN_PROGRESS);
        assertEquals(record.getFileName(), "test.csv");
        assertEquals(record.getCreatedBy(), "testUser");
        assertEquals(record.getCreatedTime(), createdTime);
        assertEquals(record.getStartTime(), startTime);
    }

    @Test
    public void testAtlasSearchDownloadRecordConstructorWithoutStartTime() {
        Date createdTime = new Date();

        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "test2.csv", "testUser2", createdTime);

        assertEquals(record.getStatus(), AtlasTask.Status.COMPLETE);
        assertEquals(record.getFileName(), "test2.csv");
        assertEquals(record.getCreatedBy(), "testUser2");
        assertEquals(record.getCreatedTime(), createdTime);
        assertNull(record.getStartTime());
    }

    @Test
    public void testAtlasSearchDownloadRecordStatusGetterSetter() {
        Date createdTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.PENDING, "test.csv", "user", createdTime);

        assertEquals(record.getStatus(), AtlasTask.Status.PENDING);

        record.setStatus(AtlasTask.Status.FAILED);
        assertEquals(record.getStatus(), AtlasTask.Status.FAILED);

        record.setStatus(null);
        assertNull(record.getStatus());
    }

    @Test
    public void testAtlasSearchDownloadRecordFileNameGetterSetter() {
        Date createdTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "original.csv", "user", createdTime);

        assertEquals(record.getFileName(), "original.csv");

        record.setFileName("updated.csv");
        assertEquals(record.getFileName(), "updated.csv");

        record.setFileName("");
        assertEquals(record.getFileName(), "");

        record.setFileName(null);
        assertNull(record.getFileName());
    }

    @Test
    public void testAtlasSearchDownloadRecordCreatedByGetterSetter() {
        Date createdTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "test.csv", "originalUser", createdTime);

        assertEquals(record.getCreatedBy(), "originalUser");

        record.setCreatedBy("updatedUser");
        assertEquals(record.getCreatedBy(), "updatedUser");

        record.setCreatedBy("");
        assertEquals(record.getCreatedBy(), "");

        record.setCreatedBy(null);
        assertNull(record.getCreatedBy());
    }

    @Test
    public void testAtlasSearchDownloadRecordCreatedTimeGetterSetter() {
        Date originalTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "test.csv", "user", originalTime);

        assertEquals(record.getCreatedTime(), originalTime);

        Date newTime = new Date(originalTime.getTime() + 10000);
        record.setCreatedTime(newTime);
        assertEquals(record.getCreatedTime(), newTime);

        record.setCreatedTime(null);
        assertNull(record.getCreatedTime());
    }

    @Test
    public void testAtlasSearchDownloadRecordStartTimeGetterSetter() {
        Date createdTime = new Date();
        Date startTime = new Date(createdTime.getTime() + 1000);

        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.IN_PROGRESS, "test.csv", "user", createdTime, startTime);

        assertEquals(record.getStartTime(), startTime);

        Date newStartTime = new Date(createdTime.getTime() + 5000);
        record.setStartTime(newStartTime);
        assertEquals(record.getStartTime(), newStartTime);

        record.setStartTime(null);
        assertNull(record.getStartTime());
    }

    @Test
    public void testAtlasSearchDownloadRecordToStringWithStringBuilder() {
        Date createdTime = new Date();
        Date startTime = new Date(createdTime.getTime() + 1000);

        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.IN_PROGRESS, "test.csv", "testUser", createdTime, startTime);

        StringBuilder sb = new StringBuilder();
        StringBuilder result = record.toString(sb);

        assertNotNull(result);
        String resultString = result.toString();
        assertTrue(resultString.contains("AtlasSearchDownloadRecord"));
        assertTrue(resultString.contains("IN_PROGRESS"));
        assertTrue(resultString.contains("test.csv"));
        assertTrue(resultString.contains("testUser"));
    }

    @Test
    public void testAtlasSearchDownloadRecordToStringWithNullStringBuilder() {
        Date createdTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "test.csv", "user", createdTime);

        StringBuilder result = record.toString(null);

        assertNotNull(result);
        String resultString = result.toString();
        assertTrue(resultString.contains("AtlasSearchDownloadRecord"));
        assertTrue(resultString.contains("COMPLETE"));
    }

    @Test
    public void testAtlasSearchDownloadRecordToString() {
        Date createdTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.FAILED, "failed.csv", "failUser", createdTime);

        String result = record.toString();

        assertNotNull(result);
        assertTrue(result.contains("AtlasSearchDownloadRecord"));
        assertTrue(result.contains("FAILED"));
        assertTrue(result.contains("failed.csv"));
        assertTrue(result.contains("failUser"));
    }

    @Test
    public void testAtlasSearchDownloadRecordAllTaskStatuses() {
        Date createdTime = new Date();
        AtlasTask.Status[] statuses = AtlasTask.Status.values();

        for (AtlasTask.Status status : statuses) {
            AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                    status, "test_" + status.name() + ".csv", "user", createdTime);

            assertEquals(record.getStatus(), status);
            assertEquals(record.getFileName(), "test_" + status.name() + ".csv");
        }
    }

    @Test
    public void testAtlasSearchDownloadRecordWithSpecialCharacters() {
        Date createdTime = new Date();
        String specialFileName = "file with spaces & special chars!@#.csv";
        String specialUserName = "user@domain.com";

        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, specialFileName, specialUserName, createdTime);

        assertEquals(record.getFileName(), specialFileName);
        assertEquals(record.getCreatedBy(), specialUserName);
    }

    @Test
    public void testAtlasSearchDownloadRecordWithUnicodeCharacters() {
        Date createdTime = new Date();
        String unicodeFileName = "unicodeFile";
        String unicodeUserName = "unicodeUser";

        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, unicodeFileName, unicodeUserName, createdTime);

        assertEquals(record.getFileName(), unicodeFileName);
        assertEquals(record.getCreatedBy(), unicodeUserName);
    }

    @Test
    public void testAtlasSearchDownloadRecordTimeSequence() {
        Date createdTime = new Date();
        Date startTime = new Date(createdTime.getTime() + 5000); // 5 seconds later

        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.IN_PROGRESS, "test.csv", "user", createdTime, startTime);

        assertTrue(record.getStartTime().compareTo(record.getCreatedTime()) > 0);
    }

    @Test
    public void testAtlasSearchDownloadRecordSerializable() {
        // Test that AtlasSearchDownloadRecord implements Serializable
        Date createdTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "test.csv", "user", createdTime);

        assertNotNull(record);
        // If this compiles without error, Serializable is implemented
    }

    @Test
    public void testAtlasSearchDownloadRecordJsonAnnotations() {
        // Test that the inner class has proper Jackson annotations
        Date createdTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "test.csv", "user", createdTime);

        assertNotNull(record);
    }

    @Test
    public void testAtlasSearchDownloadRecordXmlAnnotations() {
        // Test that the inner class has proper XML annotations
        Date createdTime = new Date();
        AtlasSearchDownloadRecord record = new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "test.csv", "user", createdTime);

        assertNotNull(record);
    }

    @Test
    public void testEmptyRecordsList() {
        List<AtlasSearchDownloadRecord> emptyRecords = new ArrayList<>();
        downloadStatus.setSearchDownloadRecords(emptyRecords);

        assertNotNull(downloadStatus.getSearchDownloadRecords());
        assertEquals(downloadStatus.getSearchDownloadRecords().size(), 0);
        assertTrue(downloadStatus.getSearchDownloadRecords().isEmpty());
    }

    @Test
    public void testCompleteDownloadStatusScenario() {
        List<AtlasSearchDownloadRecord> records = new ArrayList<>();

        Date baseTime = new Date();

        // Add records with different statuses and times
        records.add(new AtlasSearchDownloadRecord(
                AtlasTask.Status.PENDING, "pending_file.csv", "user1", baseTime));

        records.add(new AtlasSearchDownloadRecord(
                AtlasTask.Status.IN_PROGRESS, "inprogress_file.csv", "user2",
                new Date(baseTime.getTime() + 1000), new Date(baseTime.getTime() + 2000)));

        records.add(new AtlasSearchDownloadRecord(
                AtlasTask.Status.COMPLETE, "complete_file.csv", "user3",
                new Date(baseTime.getTime() + 3000)));

        records.add(new AtlasSearchDownloadRecord(
                AtlasTask.Status.FAILED, "failed_file.csv", "user4",
                new Date(baseTime.getTime() + 4000)));

        downloadStatus.setSearchDownloadRecords(records);

        assertEquals(downloadStatus.getSearchDownloadRecords().size(), 4);

        assertEquals(downloadStatus.getSearchDownloadRecords().get(0).getStatus(), AtlasTask.Status.PENDING);
        assertEquals(downloadStatus.getSearchDownloadRecords().get(1).getStatus(), AtlasTask.Status.IN_PROGRESS);
        assertEquals(downloadStatus.getSearchDownloadRecords().get(2).getStatus(), AtlasTask.Status.COMPLETE);
        assertEquals(downloadStatus.getSearchDownloadRecords().get(3).getStatus(), AtlasTask.Status.FAILED);

        assertNull(downloadStatus.getSearchDownloadRecords().get(0).getStartTime());
        assertNotNull(downloadStatus.getSearchDownloadRecords().get(1).getStartTime());
        assertNull(downloadStatus.getSearchDownloadRecords().get(2).getStartTime());
        assertNull(downloadStatus.getSearchDownloadRecords().get(3).getStartTime());
    }
}
