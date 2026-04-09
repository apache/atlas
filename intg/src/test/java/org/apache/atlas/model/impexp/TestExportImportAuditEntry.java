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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestExportImportAuditEntry {
    private ExportImportAuditEntry auditEntry;

    @BeforeMethod
    public void setUp() {
        auditEntry = new ExportImportAuditEntry();
    }

    @Test
    public void testDefaultConstructor() {
        ExportImportAuditEntry entry = new ExportImportAuditEntry();

        assertNotNull(entry);
        assertNull(entry.getUserName());
        assertNull(entry.getOperation());
        assertNull(entry.getOperationParams());
        assertEquals(entry.getStartTime(), 0L);
        assertEquals(entry.getEndTime(), 0L);
        assertNull(entry.getResultSummary());
        assertNull(entry.getSourceServerName());
        assertNull(entry.getTargetServerName());
    }

    @Test
    public void testParameterizedConstructor() {
        String sourceClusterName = "source-cluster";
        String operation = ExportImportAuditEntry.OPERATION_EXPORT;

        ExportImportAuditEntry entry = new ExportImportAuditEntry(sourceClusterName, operation);

        assertNotNull(entry);
        assertEquals(entry.getSourceServerName(), sourceClusterName);
        assertEquals(entry.getOperation(), operation);
        assertNull(entry.getUserName());
        assertNull(entry.getOperationParams());
        assertEquals(entry.getStartTime(), 0L);
        assertEquals(entry.getEndTime(), 0L);
        assertNull(entry.getResultSummary());
        assertNull(entry.getTargetServerName());
    }

    @Test
    public void testParameterizedConstructorWithNulls() {
        ExportImportAuditEntry entry = new ExportImportAuditEntry(null, null);

        assertNotNull(entry);
        assertNull(entry.getSourceServerName());
        assertNull(entry.getOperation());
    }

    @Test
    public void testParameterizedConstructorWithAllOperations() {
        String sourceCluster = "test-cluster";
        String[] operations = {
            ExportImportAuditEntry.OPERATION_EXPORT,
            ExportImportAuditEntry.OPERATION_IMPORT,
            ExportImportAuditEntry.OPERATION_IMPORT_DELETE_REPL
        };

        for (String operation : operations) {
            ExportImportAuditEntry entry = new ExportImportAuditEntry(sourceCluster, operation);
            assertEquals(entry.getOperation(), operation);
            assertEquals(entry.getSourceServerName(), sourceCluster);
        }
    }

    @Test
    public void testOperationSetterGetter() {
        auditEntry.setOperation(ExportImportAuditEntry.OPERATION_IMPORT);
        assertEquals(auditEntry.getOperation(), ExportImportAuditEntry.OPERATION_IMPORT);

        auditEntry.setOperation(ExportImportAuditEntry.OPERATION_EXPORT);
        assertEquals(auditEntry.getOperation(), ExportImportAuditEntry.OPERATION_EXPORT);

        auditEntry.setOperation(ExportImportAuditEntry.OPERATION_IMPORT_DELETE_REPL);
        assertEquals(auditEntry.getOperation(), ExportImportAuditEntry.OPERATION_IMPORT_DELETE_REPL);

        auditEntry.setOperation(null);
        assertNull(auditEntry.getOperation());
    }

    @Test
    public void testUserNameSetterGetter() {
        String userName = "testuser";
        auditEntry.setUserName(userName);
        assertEquals(auditEntry.getUserName(), userName);

        auditEntry.setUserName(null);
        assertNull(auditEntry.getUserName());
    }

    @Test
    public void testOperationParamsSetterGetter() {
        String operationParams = "param1=value1,param2=value2";
        auditEntry.setOperationParams(operationParams);
        assertEquals(auditEntry.getOperationParams(), operationParams);

        auditEntry.setOperationParams(null);
        assertNull(auditEntry.getOperationParams());
    }

    @Test
    public void testStartTimeSetterGetter() {
        long startTime = System.currentTimeMillis();
        auditEntry.setStartTime(startTime);
        assertEquals(auditEntry.getStartTime(), startTime);

        auditEntry.setStartTime(0L);
        assertEquals(auditEntry.getStartTime(), 0L);

        auditEntry.setStartTime(Long.MAX_VALUE);
        assertEquals(auditEntry.getStartTime(), Long.MAX_VALUE);
    }

    @Test
    public void testEndTimeSetterGetter() {
        long endTime = System.currentTimeMillis();
        auditEntry.setEndTime(endTime);
        assertEquals(auditEntry.getEndTime(), endTime);

        auditEntry.setEndTime(0L);
        assertEquals(auditEntry.getEndTime(), 0L);

        auditEntry.setEndTime(Long.MAX_VALUE);
        assertEquals(auditEntry.getEndTime(), Long.MAX_VALUE);
    }

    @Test
    public void testTargetServerNameSetterGetter() {
        String targetServerName = "target-server";
        auditEntry.setTargetServerName(targetServerName);
        assertEquals(auditEntry.getTargetServerName(), targetServerName);

        auditEntry.setTargetServerName(null);
        assertNull(auditEntry.getTargetServerName());
    }

    @Test
    public void testSourceServerNameSetterGetter() {
        String sourceServerName = "source-server";
        auditEntry.setSourceServerName(sourceServerName);
        assertEquals(auditEntry.getSourceServerName(), sourceServerName);

        auditEntry.setSourceServerName(null);
        assertNull(auditEntry.getSourceServerName());
    }

    @Test
    public void testResultSummarySetterGetter() {
        String resultSummary = "Operation completed successfully with 100 entities processed";
        auditEntry.setResultSummary(resultSummary);
        assertEquals(auditEntry.getResultSummary(), resultSummary);

        auditEntry.setResultSummary(null);
        assertNull(auditEntry.getResultSummary());
    }

    @Test
    public void testToString() {
        auditEntry.setUserName("testuser");
        auditEntry.setOperation(ExportImportAuditEntry.OPERATION_EXPORT);
        auditEntry.setOperationParams("fetchType=full");
        auditEntry.setSourceServerName("source-cluster");
        auditEntry.setTargetServerName("target-cluster");
        auditEntry.setStartTime(1640995200000L);
        auditEntry.setEndTime(1640995260000L);
        auditEntry.setResultSummary("Export completed successfully");

        String result = auditEntry.toString();
        assertNotNull(result);
        assertTrue(result.contains("userName: testuser"));
        assertTrue(result.contains("operation: EXPORT"));
        assertTrue(result.contains("operationParams: fetchType=full"));
        assertTrue(result.contains("sourceClusterName: source-cluster"));
        assertTrue(result.contains("targetClusterName: target-cluster"));
        assertTrue(result.contains("startTime: 1640995200000"));
        assertTrue(result.contains("endTime: 1640995260000"));
        assertTrue(result.contains("resultSummary: Export completed successfully"));
    }

    @Test
    public void testToStringWithNullValues() {
        auditEntry.setUserName(null);
        auditEntry.setOperation(null);
        auditEntry.setOperationParams(null);
        auditEntry.setSourceServerName(null);
        auditEntry.setTargetServerName(null);
        auditEntry.setResultSummary(null);

        String result = auditEntry.toString();
        assertNotNull(result);
        assertTrue(result.contains("userName: null"));
        assertTrue(result.contains("operation: null"));
        assertTrue(result.contains("operationParams: null"));
        assertTrue(result.contains("sourceClusterName: null"));
        assertTrue(result.contains("targetClusterName: null"));
        assertTrue(result.contains("resultSummary: null"));
    }

    @Test
    public void testToStringWithEmptyValues() {
        auditEntry.setUserName("");
        auditEntry.setOperation("");
        auditEntry.setOperationParams("");
        auditEntry.setSourceServerName("");
        auditEntry.setTargetServerName("");
        auditEntry.setResultSummary("");

        String result = auditEntry.toString();
        assertNotNull(result);
        assertTrue(result.contains("userName: "));
        assertTrue(result.contains("operation: "));
        assertTrue(result.contains("operationParams: "));
        assertTrue(result.contains("sourceClusterName: "));
        assertTrue(result.contains("targetClusterName: "));
        assertTrue(result.contains("resultSummary: "));
    }

    @Test
    public void testConstantValues() {
        assertEquals(ExportImportAuditEntry.OPERATION_EXPORT, "EXPORT");
        assertEquals(ExportImportAuditEntry.OPERATION_IMPORT, "IMPORT");
        assertEquals(ExportImportAuditEntry.OPERATION_IMPORT_DELETE_REPL, "IMPORT_DELETE_REPL");
    }

    @Test
    public void testBoundaryValues() {
        // Test with extreme time values
        auditEntry.setStartTime(Long.MAX_VALUE);
        assertEquals(auditEntry.getStartTime(), Long.MAX_VALUE);

        auditEntry.setStartTime(Long.MIN_VALUE);
        assertEquals(auditEntry.getStartTime(), Long.MIN_VALUE);

        auditEntry.setEndTime(Long.MAX_VALUE);
        assertEquals(auditEntry.getEndTime(), Long.MAX_VALUE);

        auditEntry.setEndTime(Long.MIN_VALUE);
        assertEquals(auditEntry.getEndTime(), Long.MIN_VALUE);
    }

    @Test
    public void testSpecialCharactersInStrings() {
        String specialUserName = "spcUN";
        String specialOperation = "spcOp";
        String specialParams = "spcP";
        String specialSourceServer = "spcSS";
        String specialTargetServer = "spcTS";
        String specialResultSummary = "spcRS";

        auditEntry.setUserName(specialUserName);
        auditEntry.setOperation(specialOperation);
        auditEntry.setOperationParams(specialParams);
        auditEntry.setSourceServerName(specialSourceServer);
        auditEntry.setTargetServerName(specialTargetServer);
        auditEntry.setResultSummary(specialResultSummary);

        assertEquals(auditEntry.getUserName(), specialUserName);
        assertEquals(auditEntry.getOperation(), specialOperation);
        assertEquals(auditEntry.getOperationParams(), specialParams);
        assertEquals(auditEntry.getSourceServerName(), specialSourceServer);
        assertEquals(auditEntry.getTargetServerName(), specialTargetServer);
        assertEquals(auditEntry.getResultSummary(), specialResultSummary);
    }

    @Test
    public void testLongStrings() {
        StringBuilder longUserNameBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longUserNameBuilder.append("u");
        }
        String longUserName = longUserNameBuilder.toString();
        StringBuilder longOperationBuilder = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            longOperationBuilder.append("o");
        }
        String longOperation = longOperationBuilder.toString();
        StringBuilder longParamsBuilder = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            longParamsBuilder.append("p");
        }
        String longParams = longParamsBuilder.toString();
        StringBuilder longSourceServerBuilder = new StringBuilder();
        for (int i = 0; i < 800; i++) {
            longSourceServerBuilder.append("s");
        }
        String longSourceServer = longSourceServerBuilder.toString();
        StringBuilder longTargetServerBuilder = new StringBuilder();
        for (int i = 0; i < 800; i++) {
            longTargetServerBuilder.append("t");
        }
        String longTargetServer = longTargetServerBuilder.toString();
        StringBuilder longResultSummaryBuilder = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            longResultSummaryBuilder.append("r");
        }
        String longResultSummary = longResultSummaryBuilder.toString();

        auditEntry.setUserName(longUserName);
        auditEntry.setOperation(longOperation);
        auditEntry.setOperationParams(longParams);
        auditEntry.setSourceServerName(longSourceServer);
        auditEntry.setTargetServerName(longTargetServer);
        auditEntry.setResultSummary(longResultSummary);

        assertEquals(auditEntry.getUserName(), longUserName);
        assertEquals(auditEntry.getOperation(), longOperation);
        assertEquals(auditEntry.getOperationParams(), longParams);
        assertEquals(auditEntry.getSourceServerName(), longSourceServer);
        assertEquals(auditEntry.getTargetServerName(), longTargetServer);
        assertEquals(auditEntry.getResultSummary(), longResultSummary);
    }

    @Test
    public void testTimeSequence() {
        long startTime = 1640995200000L; // January 1, 2022 00:00:00 GMT
        long endTime = 1640995260000L;   // January 1, 2022 00:01:00 GMT

        auditEntry.setStartTime(startTime);
        auditEntry.setEndTime(endTime);

        assertEquals(auditEntry.getStartTime(), startTime);
        assertEquals(auditEntry.getEndTime(), endTime);
        assertTrue(auditEntry.getEndTime() > auditEntry.getStartTime());
    }

    @Test
    public void testNegativeTimeValues() {
        long negativeStartTime = -1000L;
        long negativeEndTime = -500L;

        auditEntry.setStartTime(negativeStartTime);
        auditEntry.setEndTime(negativeEndTime);

        assertEquals(auditEntry.getStartTime(), negativeStartTime);
        assertEquals(auditEntry.getEndTime(), negativeEndTime);
    }

    @Test
    public void testComplexOperationParams() {
        // Test with complex JSON-like operation parameters
        String complexParams = "{\"fetchType\":\"full\",\"skipLineage\":true,\"itemsToExport\":[{\"typeName\":\"Table\",\"guid\":\"guid1\"},{\"typeName\":\"Database\",\"guid\":\"guid2\"}],\"options\":{\"replicatedTo\":\"target-cluster\",\"changeMarker\":12345}}";
        auditEntry.setOperationParams(complexParams);
        assertEquals(auditEntry.getOperationParams(), complexParams);

        String result = auditEntry.toString();
        assertTrue(result.contains("operationParams: " + complexParams));
    }

    @Test
    public void testComplexWorkflow() {
        ExportImportAuditEntry exportEntry = new ExportImportAuditEntry("prod-source-cluster", ExportImportAuditEntry.OPERATION_EXPORT);
        exportEntry.setUserName("admin");
        exportEntry.setOperationParams("fetchType=full,skipLineage=false");
        exportEntry.setTargetServerName("backup-cluster");
        exportEntry.setStartTime(System.currentTimeMillis());
        // Simulate processing time
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        exportEntry.setEndTime(System.currentTimeMillis());
        exportEntry.setResultSummary("Export completed: 1000 entities, 500 relationships exported");

        // Verify export entry
        assertEquals(exportEntry.getOperation(), ExportImportAuditEntry.OPERATION_EXPORT);
        assertEquals(exportEntry.getSourceServerName(), "prod-source-cluster");
        assertEquals(exportEntry.getTargetServerName(), "backup-cluster");
        assertTrue(exportEntry.getEndTime() >= exportEntry.getStartTime());
        assertNotNull(exportEntry.toString());

        // Simulate a corresponding import audit entry
        ExportImportAuditEntry importEntry = new ExportImportAuditEntry("backup-cluster", ExportImportAuditEntry.OPERATION_IMPORT);
        importEntry.setUserName("admin");
        importEntry.setOperationParams("updateTypeDefinition=true,numWorkers=4");
        importEntry.setTargetServerName("staging-cluster");
        importEntry.setStartTime(exportEntry.getEndTime() + 1000); // Import starts after export
        importEntry.setEndTime(importEntry.getStartTime() + 5000);
        importEntry.setResultSummary("Import completed: 1000 entities processed, 995 successful, 5 failed");

        // Verify import entry
        assertEquals(importEntry.getOperation(), ExportImportAuditEntry.OPERATION_IMPORT);
        assertEquals(importEntry.getSourceServerName(), "backup-cluster");
        assertEquals(importEntry.getTargetServerName(), "staging-cluster");
        assertTrue(importEntry.getStartTime() > exportEntry.getEndTime());
        assertNotNull(importEntry.toString());
    }

    @Test
    public void testWhitespaceInValues() {
        String userNameWithSpaces = "  user with spaces  ";
        String operationWithSpaces = "  EXPORT  ";
        String paramsWithSpaces = "  param1=value1, param2=value2  ";

        auditEntry.setUserName(userNameWithSpaces);
        auditEntry.setOperation(operationWithSpaces);
        auditEntry.setOperationParams(paramsWithSpaces);

        assertEquals(auditEntry.getUserName(), userNameWithSpaces);
        assertEquals(auditEntry.getOperation(), operationWithSpaces);
        assertEquals(auditEntry.getOperationParams(), paramsWithSpaces);
    }

    @Test
    public void testMultilineStrings() {
        String multilineUserName = "user\nwith\nnewlines";
        String multilineParams = "param1=value1\nparam2=value2\nparam3=value3";
        String multilineResultSummary = "First line of summary\nSecond line with details\nThird line with errors";

        auditEntry.setUserName(multilineUserName);
        auditEntry.setOperationParams(multilineParams);
        auditEntry.setResultSummary(multilineResultSummary);

        assertEquals(auditEntry.getUserName(), multilineUserName);
        assertEquals(auditEntry.getOperationParams(), multilineParams);
        assertEquals(auditEntry.getResultSummary(), multilineResultSummary);

        String result = auditEntry.toString();
        assertTrue(result.contains(multilineUserName));
        assertTrue(result.contains(multilineParams));
        assertTrue(result.contains(multilineResultSummary));
    }

    @Test
    public void testReplicationDeleteOperation() {
        // Test specifically for IMPORT_DELETE_REPL operation
        ExportImportAuditEntry replEntry = new ExportImportAuditEntry("source-cluster", ExportImportAuditEntry.OPERATION_IMPORT_DELETE_REPL);
        replEntry.setUserName("system");
        replEntry.setOperationParams("replicatedFrom=source-cluster,entityGuids=[guid1,guid2,guid3]");
        replEntry.setTargetServerName("current-cluster");
        replEntry.setStartTime(System.currentTimeMillis());
        replEntry.setEndTime(replEntry.getStartTime() + 2000);
        replEntry.setResultSummary("Replication cleanup: 3 entities marked for deletion");

        assertEquals(replEntry.getOperation(), ExportImportAuditEntry.OPERATION_IMPORT_DELETE_REPL);
        assertTrue(replEntry.getOperationParams().contains("replicatedFrom"));
        assertTrue(replEntry.getResultSummary().contains("deletion"));
    }

    @Test
    public void testZeroTimeValues() {
        auditEntry.setStartTime(0L);
        auditEntry.setEndTime(0L);

        assertEquals(auditEntry.getStartTime(), 0L);
        assertEquals(auditEntry.getEndTime(), 0L);

        String result = auditEntry.toString();
        assertTrue(result.contains("startTime: 0"));
        assertTrue(result.contains("endTime: 0"));
    }
}
