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

import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAsyncImportStatus {
    @Test
    public void testDefaultConstructor() {
        AsyncImportStatus status = new AsyncImportStatus();

        assertNotNull(status);
        assertNull(status.getImportId());
        assertNull(status.getStatus());
        assertNull(status.getImportRequestReceivedTime());
        assertNull(status.getImportRequestUser());
    }

    @Test
    public void testParameterizedConstructor() {
        String importId = "test-import-123";
        ImportStatus status = ImportStatus.PROCESSING;
        String receivedTime = "2023-12-01T10:30:00.000Z";
        String user = "testuser";

        AsyncImportStatus asyncStatus = new AsyncImportStatus(importId, status, receivedTime, user);

        assertNotNull(asyncStatus);
        assertEquals(asyncStatus.getImportId(), importId);
        assertEquals(asyncStatus.getStatus(), status);
        assertEquals(asyncStatus.getImportRequestReceivedTime(), receivedTime);
        assertEquals(asyncStatus.getImportRequestUser(), user);
    }

    @Test
    public void testParameterizedConstructorWithNulls() {
        AsyncImportStatus asyncStatus = new AsyncImportStatus(null, null, null, null);

        assertNotNull(asyncStatus);
        assertNull(asyncStatus.getImportId());
        assertNull(asyncStatus.getStatus());
        assertNull(asyncStatus.getImportRequestReceivedTime());
        assertNull(asyncStatus.getImportRequestUser());
    }

    @Test
    public void testParameterizedConstructorWithAllImportStatuses() {
        String importId = "test-import-456";
        String receivedTime = "2023-12-01T11:00:00.000Z";
        String user = "admin";

        for (ImportStatus status : ImportStatus.values()) {
            AsyncImportStatus asyncStatus = new AsyncImportStatus(importId, status, receivedTime, user);
            assertEquals(asyncStatus.getStatus(), status);
        }
    }

    @Test
    public void testGetters() {
        String importId = "getter-test-789";
        ImportStatus status = ImportStatus.SUCCESSFUL;
        String receivedTime = "2023-12-01T12:15:00.000Z";
        String user = "getteruser";

        AsyncImportStatus asyncStatus = new AsyncImportStatus(importId, status, receivedTime, user);

        assertEquals(asyncStatus.getImportId(), importId);
        assertEquals(asyncStatus.getStatus(), status);
        assertEquals(asyncStatus.getImportRequestReceivedTime(), receivedTime);
        assertEquals(asyncStatus.getImportRequestUser(), user);
    }

    @Test
    public void testToString() {
        String importId = "toString-test-101";
        ImportStatus status = ImportStatus.FAILED;
        String receivedTime = "2023-12-01T13:45:00.000Z";
        String user = "stringuser";

        AsyncImportStatus asyncStatus = new AsyncImportStatus(importId, status, receivedTime, user);
        String result = asyncStatus.toString();

        assertNotNull(result);
        assertTrue(result.contains("AsyncImportStatus{"));
        assertTrue(result.contains("importId='toString-test-101'"));
        assertTrue(result.contains("status='FAILED'"));
        assertTrue(result.contains("importRequestReceivedTime='2023-12-01T13:45:00.000Z'"));
        assertTrue(result.contains("importRequestUser='stringuser'"));
    }

    @Test
    public void testToStringWithNullValues() {
        AsyncImportStatus asyncStatus = new AsyncImportStatus(null, null, null, null);
        String result = asyncStatus.toString();

        assertNotNull(result);
        assertTrue(result.contains("AsyncImportStatus{"));
        assertTrue(result.contains("importId='null'"));
        assertTrue(result.contains("status='null'"));
        assertTrue(result.contains("importRequestReceivedTime='null'"));
        assertTrue(result.contains("importRequestUser='null'"));
    }

    @Test
    public void testToStringWithEmptyStrings() {
        AsyncImportStatus asyncStatus = new AsyncImportStatus("", ImportStatus.STAGING, "", "");
        String result = asyncStatus.toString();

        assertNotNull(result);
        assertTrue(result.contains("AsyncImportStatus{"));
        assertTrue(result.contains("importId=''"));
        assertTrue(result.contains("status='STAGING'"));
        assertTrue(result.contains("importRequestReceivedTime=''"));
        assertTrue(result.contains("importRequestUser=''"));
    }

    @Test
    public void testBoundaryValues() {
        StringBuilder longImportIdBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longImportIdBuilder.append("a");
        }
        String longImportId = longImportIdBuilder.toString();
        StringBuilder longUserBuilder = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            longUserBuilder.append("u");
        }
        String longUser = longUserBuilder.toString();
        StringBuilder longTimeBuilder = new StringBuilder("2023-12-01T14:30:00.000Z");
        for (int i = 0; i < 100; i++) {
            longTimeBuilder.append("x");
        }
        String longTime = longTimeBuilder.toString();

        AsyncImportStatus asyncStatus = new AsyncImportStatus(longImportId, ImportStatus.PARTIAL_SUCCESS, longTime, longUser);

        assertEquals(asyncStatus.getImportId(), longImportId);
        assertEquals(asyncStatus.getStatus(), ImportStatus.PARTIAL_SUCCESS);
        assertEquals(asyncStatus.getImportRequestReceivedTime(), longTime);
        assertEquals(asyncStatus.getImportRequestUser(), longUser);
    }

    @Test
    public void testSpecialCharacters() {
        String specialImportId = "spcImpId";
        String specialUser = "spcUser";
        String specialTime = "spcTim";

        AsyncImportStatus asyncStatus = new AsyncImportStatus(specialImportId, ImportStatus.ABORTED, specialTime, specialUser);

        assertEquals(asyncStatus.getImportId(), specialImportId);
        assertEquals(asyncStatus.getStatus(), ImportStatus.ABORTED);
        assertEquals(asyncStatus.getImportRequestReceivedTime(), specialTime);
        assertEquals(asyncStatus.getImportRequestUser(), specialUser);
    }
}
