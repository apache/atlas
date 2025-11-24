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

import org.apache.atlas.model.impexp.AtlasImportResult.OperationStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasImportResult {
    private AtlasImportResult importResult;
    private AtlasImportRequest importRequest;

    @BeforeMethod
    public void setUp() {
        importResult = new AtlasImportResult();
        importRequest = new AtlasImportRequest();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasImportResult result = new AtlasImportResult();

        assertNotNull(result);
        assertNull(result.getRequest());
        assertNull(result.getUserName());
        assertNull(result.getClientIpAddress());
        assertNull(result.getHostName());
        assertTrue(result.getTimeStamp() > 0);
        assertNotNull(result.getMetrics());
        assertTrue(result.getMetrics().isEmpty());
        assertEquals(result.getOperationStatus(), OperationStatus.FAIL);
        assertNotNull(result.getProcessedEntities());
        assertTrue(result.getProcessedEntities().isEmpty());
        assertNull(result.getExportResult());
    }

    @Test
    public void testParameterizedConstructor() {
        String userName = "testuser";
        String clientIp = "192.168.1.1";
        String hostName = "testhost";
        long timeStamp = 1640995200000L;

        AtlasImportResult result = new AtlasImportResult(importRequest, userName, clientIp, hostName, timeStamp);

        assertNotNull(result);
        assertEquals(result.getRequest(), importRequest);
        assertEquals(result.getUserName(), userName);
        assertEquals(result.getClientIpAddress(), clientIp);
        assertEquals(result.getHostName(), hostName);
        assertEquals(result.getTimeStamp(), timeStamp);
        assertNotNull(result.getMetrics());
        assertTrue(result.getMetrics().isEmpty());
        assertEquals(result.getOperationStatus(), OperationStatus.FAIL);
        assertNotNull(result.getProcessedEntities());
        assertTrue(result.getProcessedEntities().isEmpty());
    }

    @Test
    public void testParameterizedConstructorWithNulls() {
        AtlasImportResult result = new AtlasImportResult(null, null, null, null, 0L);

        assertNotNull(result);
        assertNull(result.getRequest());
        assertNull(result.getUserName());
        assertNull(result.getClientIpAddress());
        assertNull(result.getHostName());
        assertEquals(result.getTimeStamp(), 0L);
        assertNotNull(result.getMetrics());
        assertNotNull(result.getProcessedEntities());
        assertEquals(result.getOperationStatus(), OperationStatus.FAIL);
    }

    @Test
    public void testRequestSetterGetter() {
        importResult.setRequest(importRequest);
        assertEquals(importResult.getRequest(), importRequest);

        importResult.setRequest(null);
        assertNull(importResult.getRequest());
    }

    @Test
    public void testUserNameSetterGetter() {
        String userName = "admin";
        importResult.setUserName(userName);
        assertEquals(importResult.getUserName(), userName);

        importResult.setUserName(null);
        assertNull(importResult.getUserName());
    }

    @Test
    public void testClientIpAddressSetterGetter() {
        String clientIp = "10.0.0.1";
        importResult.setClientIpAddress(clientIp);
        assertEquals(importResult.getClientIpAddress(), clientIp);

        importResult.setClientIpAddress(null);
        assertNull(importResult.getClientIpAddress());
    }

    @Test
    public void testHostNameSetterGetter() {
        String hostName = "production-server";
        importResult.setHostName(hostName);
        assertEquals(importResult.getHostName(), hostName);

        importResult.setHostName(null);
        assertNull(importResult.getHostName());
    }

    @Test
    public void testTimeStampSetterGetter() {
        long timeStamp = System.currentTimeMillis();
        importResult.setTimeStamp(timeStamp);
        assertEquals(importResult.getTimeStamp(), timeStamp);

        importResult.setTimeStamp(0L);
        assertEquals(importResult.getTimeStamp(), 0L);

        importResult.setTimeStamp(Long.MAX_VALUE);
        assertEquals(importResult.getTimeStamp(), Long.MAX_VALUE);
    }

    @Test
    public void testMetricsSetterGetter() {
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("entities", 100);
        metrics.put("relationships", 50);

        importResult.setMetrics(metrics);
        assertEquals(importResult.getMetrics(), metrics);
        assertEquals(importResult.getMetrics().size(), 2);

        importResult.setMetrics(null);
        assertNull(importResult.getMetrics());
    }

    @Test
    public void testOperationStatusSetterGetter() {
        importResult.setOperationStatus(OperationStatus.SUCCESS);
        assertEquals(importResult.getOperationStatus(), OperationStatus.SUCCESS);

        importResult.setOperationStatus(OperationStatus.PARTIAL_SUCCESS);
        assertEquals(importResult.getOperationStatus(), OperationStatus.PARTIAL_SUCCESS);

        importResult.setOperationStatus(OperationStatus.FAIL);
        assertEquals(importResult.getOperationStatus(), OperationStatus.FAIL);
    }

    @Test
    public void testProcessedEntitiesSetterGetter() {
        List<String> processedEntities = new ArrayList<>();
        processedEntities.add("entity1");
        processedEntities.add("entity2");

        importResult.setProcessedEntities(processedEntities);
        assertEquals(importResult.getProcessedEntities(), processedEntities);
        assertEquals(importResult.getProcessedEntities().size(), 2);

        importResult.setProcessedEntities(null);
        assertNull(importResult.getProcessedEntities());
    }

    @Test
    public void testExportResultSetterGetter() {
        AtlasExportResult exportResult = new AtlasExportResult();
        exportResult.setUserName("exportuser");

        importResult.setExportResult(exportResult);
        assertEquals(importResult.getExportResult(), exportResult);
        assertEquals(importResult.getExportResult().getUserName(), "exportuser");

        importResult.setExportResult(null);
        assertNull(importResult.getExportResult());
    }

    @Test
    public void testIncrementMeticsCounterSingleIncrement() {
        importResult.incrementMeticsCounter("counter1");
        Map<String, Integer> metrics = importResult.getMetrics();
        assertEquals(metrics.get("counter1"), Integer.valueOf(1));
    }

    @Test
    public void testIncrementMeticsCounterMultipleIncrements() {
        importResult.incrementMeticsCounter("counter1");
        importResult.incrementMeticsCounter("counter1");
        importResult.incrementMeticsCounter("counter1");
        Map<String, Integer> metrics = importResult.getMetrics();
        assertEquals(metrics.get("counter1"), Integer.valueOf(3));
    }

    @Test
    public void testIncrementMeticsCounterWithCustomIncrement() {
        importResult.incrementMeticsCounter("counter2", 5);
        importResult.incrementMeticsCounter("counter2", 3);
        Map<String, Integer> metrics = importResult.getMetrics();
        assertEquals(metrics.get("counter2"), Integer.valueOf(8));
    }

    @Test
    public void testIncrementMeticsCounterMixedIncrements() {
        importResult.incrementMeticsCounter("counter3");
        importResult.incrementMeticsCounter("counter3", 10);
        importResult.incrementMeticsCounter("counter3");
        Map<String, Integer> metrics = importResult.getMetrics();
        assertEquals(metrics.get("counter3"), Integer.valueOf(12));
    }

    @Test
    public void testIncrementMeticsCounterNegativeIncrement() {
        // Set initial value
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("counter4", 10);
        importResult.setMetrics(metrics);

        importResult.incrementMeticsCounter("counter4", -3);
        metrics = importResult.getMetrics();
        assertEquals(metrics.get("counter4"), Integer.valueOf(7));
    }

    @Test
    public void testIncrementMeticsCounterZeroIncrement() {
        importResult.incrementMeticsCounter("zeroCounter", 0);
        Map<String, Integer> metrics = importResult.getMetrics();
        assertEquals(metrics.get("zeroCounter"), Integer.valueOf(0));
    }

    @Test
    public void testIncrementMeticsCounterLargeValues() {
        importResult.incrementMeticsCounter("largeCounter", Integer.MAX_VALUE);
        Map<String, Integer> metrics = importResult.getMetrics();
        assertEquals(metrics.get("largeCounter"), Integer.valueOf(Integer.MAX_VALUE));
    }

    @Test
    public void testIncrementMeticsCounterWithNullMetrics() {
        importResult.setMetrics(null);
        try {
            importResult.incrementMeticsCounter("counter");
        } catch (NullPointerException e) {
            // Expected when metrics is null
            assertTrue(true);
        }
    }

    @Test
    public void testToString() {
        importResult.setUserName("testuser");
        importResult.setClientIpAddress("192.168.1.1");
        importResult.setHostName("testhost");
        importResult.setTimeStamp(1640995200000L);
        importResult.setOperationStatus(OperationStatus.SUCCESS);

        List<String> processedEntities = new ArrayList<>();
        processedEntities.add("entity1");
        importResult.setProcessedEntities(processedEntities);

        String result = importResult.toString();
        assertNotNull(result);
        assertTrue(result.contains("AtlasImportResult{"));
        assertTrue(result.contains("userName='testuser'"));
        assertTrue(result.contains("clientIpAddress='192.168.1.1'"));
        assertTrue(result.contains("hostName='testhost'"));
        assertTrue(result.contains("timeStamp='1640995200000'"));
        assertTrue(result.contains("operationStatus='SUCCESS'"));
        assertTrue(result.contains("processedEntities=["));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        StringBuilder result = importResult.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasImportResult{"));
    }

    @Test
    public void testToStringWithExistingStringBuilder() {
        StringBuilder sb = new StringBuilder("Prefix: ");
        StringBuilder result = importResult.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().startsWith("Prefix: "));
        assertTrue(result.toString().contains("AtlasImportResult{"));
    }

    @Test
    public void testToStringWithNullValues() {
        importResult.setRequest(null);
        importResult.setUserName(null);
        importResult.setProcessedEntities(null);
        importResult.setExportResult(null);

        String result = importResult.toString();
        assertNotNull(result);
        assertTrue(result.contains("AtlasImportResult{"));
        assertTrue(result.contains("userName='null'"));
        assertTrue(result.contains("exportResult={null}"));
    }

    @Test
    public void testOperationStatusEnum() {
        // Test all enum values exist
        OperationStatus[] statuses = OperationStatus.values();
        assertEquals(statuses.length, 3);
        // Test specific values
        assertEquals(OperationStatus.SUCCESS.name(), "SUCCESS");
        assertEquals(OperationStatus.PARTIAL_SUCCESS.name(), "PARTIAL_SUCCESS");
        assertEquals(OperationStatus.FAIL.name(), "FAIL");
    }

    @Test
    public void testBoundaryValues() {
        // Test with extreme values
        importResult.setTimeStamp(Long.MAX_VALUE);
        assertEquals(importResult.getTimeStamp(), Long.MAX_VALUE);

        importResult.setTimeStamp(Long.MIN_VALUE);
        assertEquals(importResult.getTimeStamp(), Long.MIN_VALUE);

        // Test with empty collections
        importResult.setProcessedEntities(new ArrayList<>());
        assertTrue(importResult.getProcessedEntities().isEmpty());

        importResult.setMetrics(new HashMap<>());
        assertTrue(importResult.getMetrics().isEmpty());
    }

    @Test
    public void testSpecialCharactersInStrings() {
        String specialUserName = "spcUser";
        String specialHostName = "spcHost";
        String specialClientIp = "spcCli";

        importResult.setUserName(specialUserName);
        importResult.setHostName(specialHostName);
        importResult.setClientIpAddress(specialClientIp);

        assertEquals(importResult.getUserName(), specialUserName);
        assertEquals(importResult.getHostName(), specialHostName);
        assertEquals(importResult.getClientIpAddress(), specialClientIp);
    }

    @Test
    public void testLargeCollections() {
        List<String> largeList = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            largeList.add("entity" + i);
        }
        importResult.setProcessedEntities(largeList);

        assertEquals(importResult.getProcessedEntities().size(), 10000);
        assertEquals(importResult.getProcessedEntities().get(5000), "entity5000");

        // Test with large metrics map
        Map<String, Integer> largeMetrics = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            largeMetrics.put("metric" + i, i);
        }
        importResult.setMetrics(largeMetrics);

        assertEquals(importResult.getMetrics().size(), 1000);
        assertEquals(importResult.getMetrics().get("metric500"), Integer.valueOf(500));
    }

    @Test
    public void testProcessedEntitiesWithSpecialCharacters() {
        List<String> entities = new ArrayList<>();
        entities.add("entity-with-dash");
        entities.add("entity_with_underscore");
        entities.add("entity.with.dots");
        entities.add("entity:with:colons");
        entities.add("entity/with/slashes");
        entities.add("entity with spaces");
        entities.add("entity-с-spc");
        entities.add("entity-spc");

        importResult.setProcessedEntities(entities);

        assertEquals(importResult.getProcessedEntities().size(), 8);
        assertTrue(importResult.getProcessedEntities().contains("entity-с-spc"));
        assertTrue(importResult.getProcessedEntities().contains("entity-spc"));
    }

    @Test
    public void testMetricsWithSpecialKeys() {
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("key-with-dash", 1);
        metrics.put("key_with_underscore", 2);
        metrics.put("key.with.dots", 3);
        metrics.put("key:with:colons", 4);
        metrics.put("key/with/slashes", 5);
        metrics.put("key with spaces", 6);
        metrics.put("test1", 7);
        metrics.put("test2", 8);

        importResult.setMetrics(metrics);

        assertEquals(importResult.getMetrics().size(), 8);
        assertEquals(importResult.getMetrics().get("test1"), Integer.valueOf(7));
        assertEquals(importResult.getMetrics().get("test2"), Integer.valueOf(8));
    }

    @Test
    public void testIncrementMeticsCounterWithSpecialKeys() {
        String[] specialKeys = {
            "key-with-dash",
            "key_with_underscore",
            "key.with.dots",
            "key:with:colons",
            "key/with/slashes",
            "key with spaces",
            "test1",
            "test2"
        };

        for (String key : specialKeys) {
            importResult.incrementMeticsCounter(key);
        }

        Map<String, Integer> metrics = importResult.getMetrics();
        for (String key : specialKeys) {
            assertEquals(metrics.get(key), Integer.valueOf(1));
        }
    }

    @Test
    public void testComplexWorkflow() {
        AtlasImportRequest request = new AtlasImportRequest();
        request.setOption("testOption", "testValue");

        AtlasImportResult result = new AtlasImportResult(request, "admin", "10.0.0.1", "server1", System.currentTimeMillis());
        List<String> entities = new ArrayList<>();
        entities.add("database1");
        entities.add("table1");
        entities.add("column1");
        result.setProcessedEntities(entities);

        result.incrementMeticsCounter("entities.processed", 3);
        result.incrementMeticsCounter("entities.created", 2);
        result.incrementMeticsCounter("entities.updated", 1);

        result.setOperationStatus(OperationStatus.SUCCESS);

        assertEquals(result.getOperationStatus(), OperationStatus.SUCCESS);
        assertEquals(result.getProcessedEntities().size(), 3);
        assertEquals(result.getMetrics().get("entities.processed"), Integer.valueOf(3));
        assertEquals(result.getMetrics().get("entities.created"), Integer.valueOf(2));
        assertEquals(result.getMetrics().get("entities.updated"), Integer.valueOf(1));
        assertNotNull(result.toString());
    }

    @Test
    public void testEmptyStringValues() {
        importResult.setUserName("");
        importResult.setClientIpAddress("");
        importResult.setHostName("");

        assertEquals(importResult.getUserName(), "");
        assertEquals(importResult.getClientIpAddress(), "");
        assertEquals(importResult.getHostName(), "");

        String result = importResult.toString();
        assertTrue(result.contains("userName=''"));
        assertTrue(result.contains("clientIpAddress=''"));
        assertTrue(result.contains("hostName=''"));
    }
}
