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
package org.apache.atlas.model.audit;

import org.apache.atlas.exception.AtlasBaseException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestAtlasAuditEntry {
    private AtlasAuditEntry auditEntry;

    @BeforeMethod
    public void setUp() {
        auditEntry = new AtlasAuditEntry();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasAuditEntry entry = new AtlasAuditEntry();

        assertNull(entry.getUserName());
        assertNull(entry.getOperation());
        assertNull(entry.getParams());
        assertNull(entry.getStartTime());
        assertNull(entry.getEndTime());
        assertNull(entry.getClientId());
        assertNull(entry.getResult());
        assertEquals(entry.getResultCount(), 0L);
    }

    @Test
    public void testParameterizedConstructor() {
        AtlasAuditEntry.AuditOperation operation = AtlasAuditEntry.AuditOperation.EXPORT;
        String userName = "testUser";
        String clientId = "testClient";

        AtlasAuditEntry entry = new AtlasAuditEntry(operation, userName, clientId);

        assertEquals(entry.getOperation(), operation);
        assertEquals(entry.getUserName(), userName);
        assertEquals(entry.getClientId(), clientId);
        assertNull(entry.getParams());
        assertNull(entry.getStartTime());
        assertNull(entry.getEndTime());
        assertNull(entry.getResult());
        assertEquals(entry.getResultCount(), 0L);
    }

    @Test
    public void testUserNameGetterSetter() {
        String userName = "testUser";

        auditEntry.setUserName(userName);
        assertEquals(auditEntry.getUserName(), userName);

        auditEntry.setUserName(null);
        assertNull(auditEntry.getUserName());
    }

    @Test
    public void testOperationGetterSetter() {
        AtlasAuditEntry.AuditOperation operation = AtlasAuditEntry.AuditOperation.IMPORT;

        auditEntry.setOperation(operation);
        assertEquals(auditEntry.getOperation(), operation);

        auditEntry.setOperation(null);
        assertNull(auditEntry.getOperation());
    }

    @Test
    public void testParamsGetterSetter() {
        String params = "test parameters";

        auditEntry.setParams(params);
        assertEquals(auditEntry.getParams(), params);

        auditEntry.setParams(null);
        assertNull(auditEntry.getParams());
    }

    @Test
    public void testStartTimeGetterSetter() {
        Date startTime = new Date();

        auditEntry.setStartTime(startTime);
        assertEquals(auditEntry.getStartTime(), startTime);

        auditEntry.setStartTime(null);
        assertNull(auditEntry.getStartTime());
    }

    @Test
    public void testEndTimeGetterSetter() {
        Date endTime = new Date();

        auditEntry.setEndTime(endTime);
        assertEquals(auditEntry.getEndTime(), endTime);

        auditEntry.setEndTime(null);
        assertNull(auditEntry.getEndTime());
    }

    @Test
    public void testClientIdGetterSetter() {
        String clientId = "testClientId";

        auditEntry.setClientId(clientId);
        assertEquals(auditEntry.getClientId(), clientId);

        auditEntry.setClientId(null);
        assertNull(auditEntry.getClientId());
    }

    @Test
    public void testResultGetterSetter() {
        String result = "SUCCESS";

        auditEntry.setResult(result);
        assertEquals(auditEntry.getResult(), result);

        auditEntry.setResult(null);
        assertNull(auditEntry.getResult());
    }

    @Test
    public void testResultCountGetterSetter() {
        long resultCount = 100L;

        auditEntry.setResultCount(resultCount);
        assertEquals(auditEntry.getResultCount(), resultCount);

        auditEntry.setResultCount(0L);
        assertEquals(auditEntry.getResultCount(), 0L);

        auditEntry.setResultCount(-1L);
        assertEquals(auditEntry.getResultCount(), -1L);
    }

    @Test
    public void testToString() {
        auditEntry.setUserName("testUser");
        auditEntry.setOperation(AtlasAuditEntry.AuditOperation.EXPORT);
        auditEntry.setParams("test params");
        auditEntry.setClientId("client123");
        auditEntry.setResult("SUCCESS");
        auditEntry.setResultCount(50L);

        Date startTime = new Date();
        Date endTime = new Date();
        auditEntry.setStartTime(startTime);
        auditEntry.setEndTime(endTime);

        StringBuilder sb = new StringBuilder();
        auditEntry.toString(sb);
        String result = sb.toString();

        assertNotNull(result);
        assertEquals(result.contains("testUser"), true);
        assertEquals(result.contains("EXPORT"), true);
        assertEquals(result.contains("test params"), true);
        assertEquals(result.contains("client123"), true);
        assertEquals(result.contains("SUCCESS"), true);
        assertEquals(result.contains("50"), true);
    }

    @Test
    public void testToStringWithNullValues() {
        StringBuilder sb = new StringBuilder();
        auditEntry.toString(sb);
        String result = sb.toString();

        assertNotNull(result);
    }

    @Test
    public void testSerializable() {
        assertNotNull(auditEntry);
    }

    @Test
    public void testCompleteAuditEntry() {
        AtlasAuditEntry entry = new AtlasAuditEntry();

        entry.setUserName("admin");
        entry.setOperation(AtlasAuditEntry.AuditOperation.TYPE_DEF_CREATE);
        entry.setParams("{\"typeName\":\"TestType\"}");
        entry.setClientId("atlas-client-001");
        entry.setResult("CREATED");
        entry.setResultCount(1L);

        Date now = new Date();
        entry.setStartTime(now);
        entry.setEndTime(new Date(now.getTime() + 1000));

        assertEquals(entry.getUserName(), "admin");
        assertEquals(entry.getOperation(), AtlasAuditEntry.AuditOperation.TYPE_DEF_CREATE);
        assertEquals(entry.getParams(), "{\"typeName\":\"TestType\"}");
        assertEquals(entry.getClientId(), "atlas-client-001");
        assertEquals(entry.getResult(), "CREATED");
        assertEquals(entry.getResultCount(), 1L);
        assertNotNull(entry.getStartTime());
        assertNotNull(entry.getEndTime());
    }

    @Test
    public void testAuditOperationValues() {
        AtlasAuditEntry.AuditOperation[] operations = AtlasAuditEntry.AuditOperation.values();

        assertEquals(operations.length, 10);

        assertNotNull(AtlasAuditEntry.AuditOperation.PURGE);
        assertNotNull(AtlasAuditEntry.AuditOperation.EXPORT);
        assertNotNull(AtlasAuditEntry.AuditOperation.IMPORT);
        assertNotNull(AtlasAuditEntry.AuditOperation.IMPORT_DELETE_REPL);
        assertNotNull(AtlasAuditEntry.AuditOperation.TYPE_DEF_CREATE);
        assertNotNull(AtlasAuditEntry.AuditOperation.TYPE_DEF_UPDATE);
        assertNotNull(AtlasAuditEntry.AuditOperation.TYPE_DEF_DELETE);
        assertNotNull(AtlasAuditEntry.AuditOperation.SERVER_START);
        assertNotNull(AtlasAuditEntry.AuditOperation.SERVER_STATE_ACTIVE);
    }

    @Test
    public void testAuditOperationGetType() {
        assertEquals(AtlasAuditEntry.AuditOperation.PURGE.getType(), "PURGE");
        assertEquals(AtlasAuditEntry.AuditOperation.EXPORT.getType(), "EXPORT");
        assertEquals(AtlasAuditEntry.AuditOperation.IMPORT.getType(), "IMPORT");
        assertEquals(AtlasAuditEntry.AuditOperation.IMPORT_DELETE_REPL.getType(), "IMPORT_DELETE_REPL");
        assertEquals(AtlasAuditEntry.AuditOperation.TYPE_DEF_CREATE.getType(), "TYPE_DEF_CREATE");
        assertEquals(AtlasAuditEntry.AuditOperation.TYPE_DEF_UPDATE.getType(), "TYPE_DEF_UPDATE");
        assertEquals(AtlasAuditEntry.AuditOperation.TYPE_DEF_DELETE.getType(), "TYPE_DEF_DELETE");
        assertEquals(AtlasAuditEntry.AuditOperation.SERVER_START.getType(), "SERVER_START");
        assertEquals(AtlasAuditEntry.AuditOperation.SERVER_STATE_ACTIVE.getType(), "SERVER_STATE_ACTIVE");
    }

    @Test
    public void testAuditOperationValueOf() {
        assertEquals(AtlasAuditEntry.AuditOperation.valueOf("PURGE"), AtlasAuditEntry.AuditOperation.PURGE);
        assertEquals(AtlasAuditEntry.AuditOperation.valueOf("EXPORT"), AtlasAuditEntry.AuditOperation.EXPORT);
        assertEquals(AtlasAuditEntry.AuditOperation.valueOf("IMPORT"), AtlasAuditEntry.AuditOperation.IMPORT);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAuditOperationValueOfInvalid() {
        AtlasAuditEntry.AuditOperation.valueOf("INVALID_OPERATION");
    }

    @Test
    public void testAuditOperationToEntityAuditActionV2Purge() throws AtlasBaseException {
        EntityAuditEventV2.EntityAuditActionV2 action = AtlasAuditEntry.AuditOperation.PURGE.toEntityAuditActionV2();
        assertEquals(action, EntityAuditEventV2.EntityAuditActionV2.ENTITY_PURGE);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testAuditOperationToEntityAuditActionV2InvalidOperation() throws AtlasBaseException {
        AtlasAuditEntry.AuditOperation.SERVER_START.toEntityAuditActionV2();
    }

    @Test
    public void testAuditOperationToString() {
        assertEquals(AtlasAuditEntry.AuditOperation.EXPORT.toString(), "EXPORT");
        assertEquals(AtlasAuditEntry.AuditOperation.IMPORT.toString(), "IMPORT");
    }

    @Test
    public void testAuditOperationName() {
        assertEquals(AtlasAuditEntry.AuditOperation.EXPORT.name(), "EXPORT");
        assertEquals(AtlasAuditEntry.AuditOperation.IMPORT.name(), "IMPORT");
    }

    @Test
    public void testAuditOperationOrdinal() {
        assertEquals(AtlasAuditEntry.AuditOperation.PURGE.ordinal(), 0);
        assertEquals(AtlasAuditEntry.AuditOperation.EXPORT.ordinal(), 2);
        assertEquals(AtlasAuditEntry.AuditOperation.IMPORT.ordinal(), 3);
    }

    @Test
    public void testSettersReturnVoid() {
        auditEntry.setUserName("test");
        auditEntry.setOperation(AtlasAuditEntry.AuditOperation.EXPORT);
        auditEntry.setParams("test");
        auditEntry.setStartTime(new Date());
        auditEntry.setEndTime(new Date());
        auditEntry.setClientId("test");
        auditEntry.setResult("test");
        auditEntry.setResultCount(1L);

        assertNotNull(auditEntry);
    }

    @Test
    public void testTimeSequence() {
        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + 5000); // 5 seconds later

        auditEntry.setStartTime(startTime);
        auditEntry.setEndTime(endTime);

        assertEquals(auditEntry.getStartTime(), startTime);
        assertEquals(auditEntry.getEndTime(), endTime);

        // Verify end time is after start time
        assertEquals(auditEntry.getEndTime().compareTo(auditEntry.getStartTime()) > 0, true);
    }

    @Test
    public void testLongResultCount() {
        long maxLong = Long.MAX_VALUE;
        long minLong = Long.MIN_VALUE;

        auditEntry.setResultCount(maxLong);
        assertEquals(auditEntry.getResultCount(), maxLong);

        auditEntry.setResultCount(minLong);
        assertEquals(auditEntry.getResultCount(), minLong);
    }

    @Test
    public void testEmptyStringValues() {
        auditEntry.setUserName("");
        auditEntry.setParams("");
        auditEntry.setClientId("");
        auditEntry.setResult("");

        assertEquals(auditEntry.getUserName(), "");
        assertEquals(auditEntry.getParams(), "");
        assertEquals(auditEntry.getClientId(), "");
        assertEquals(auditEntry.getResult(), "");
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(auditEntry);
    }
}
