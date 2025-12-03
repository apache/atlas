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

import org.apache.atlas.model.impexp.AtlasExportResult.AtlasExportData;
import org.apache.atlas.model.impexp.AtlasExportResult.OperationStatus;
import org.apache.atlas.model.typedef.AtlasTypesDef;
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

public class TestAtlasExportResult {
    private AtlasExportResult exportResult;
    private AtlasExportRequest exportRequest;

    @BeforeMethod
    public void setUp() {
        exportResult = new AtlasExportResult();
        exportRequest = new AtlasExportRequest();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasExportResult result = new AtlasExportResult();

        assertNotNull(result);
        assertNull(result.getRequest());
        assertNull(result.getUserName());
        assertNull(result.getClientIpAddress());
        assertNull(result.getHostName());
        assertTrue(result.getTimeStamp() > 0);
        assertNotNull(result.getMetrics());
        assertTrue(result.getMetrics().isEmpty());
        assertNotNull(result.getData());
        assertEquals(result.getOperationStatus(), OperationStatus.FAIL);
        assertEquals(result.getChangeMarker(), 0L);
        assertNull(result.getSourceClusterName());
    }

    @Test
    public void testParameterizedConstructor() {
        String userName = "testuser";
        String clientIp = "192.168.1.1";
        String hostName = "testhost";
        long timeStamp = 1640995200000L;
        long changeMarker = 12345L;

        AtlasExportResult result = new AtlasExportResult(exportRequest, userName, clientIp, hostName, timeStamp, changeMarker);

        assertNotNull(result);
        assertEquals(result.getRequest(), exportRequest);
        assertEquals(result.getUserName(), userName);
        assertEquals(result.getClientIpAddress(), clientIp);
        assertEquals(result.getHostName(), hostName);
        assertEquals(result.getTimeStamp(), timeStamp);
        assertEquals(result.getChangeMarker(), changeMarker);
        assertNotNull(result.getMetrics());
        assertTrue(result.getMetrics().isEmpty());
        assertNotNull(result.getData());
        assertEquals(result.getOperationStatus(), OperationStatus.FAIL);
    }

    @Test
    public void testParameterizedConstructorWithNulls() {
        AtlasExportResult result = new AtlasExportResult(null, null, null, null, 0L, 0L);

        assertNotNull(result);
        assertNull(result.getRequest());
        assertNull(result.getUserName());
        assertNull(result.getClientIpAddress());
        assertNull(result.getHostName());
        assertEquals(result.getTimeStamp(), 0L);
        assertEquals(result.getChangeMarker(), 0L);
        assertNotNull(result.getMetrics());
        assertNotNull(result.getData());
        assertEquals(result.getOperationStatus(), OperationStatus.FAIL);
    }

    @Test
    public void testRequestSetterGetter() {
        exportResult.setRequest(exportRequest);
        assertEquals(exportResult.getRequest(), exportRequest);

        exportResult.setRequest(null);
        assertNull(exportResult.getRequest());
    }

    @Test
    public void testUserNameSetterGetter() {
        String userName = "admin";
        exportResult.setUserName(userName);
        assertEquals(exportResult.getUserName(), userName);

        exportResult.setUserName(null);
        assertNull(exportResult.getUserName());
    }

    @Test
    public void testClientIpAddressSetterGetter() {
        String clientIp = "10.0.0.1";
        exportResult.setClientIpAddress(clientIp);
        assertEquals(exportResult.getClientIpAddress(), clientIp);

        exportResult.setClientIpAddress(null);
        assertNull(exportResult.getClientIpAddress());
    }

    @Test
    public void testHostNameSetterGetter() {
        String hostName = "production-server";
        exportResult.setHostName(hostName);
        assertEquals(exportResult.getHostName(), hostName);

        exportResult.setHostName(null);
        assertNull(exportResult.getHostName());
    }

    @Test
    public void testTimeStampSetterGetter() {
        long timeStamp = System.currentTimeMillis();
        exportResult.setTimeStamp(timeStamp);
        assertEquals(exportResult.getTimeStamp(), timeStamp);

        exportResult.setTimeStamp(0L);
        assertEquals(exportResult.getTimeStamp(), 0L);

        exportResult.setTimeStamp(Long.MAX_VALUE);
        assertEquals(exportResult.getTimeStamp(), Long.MAX_VALUE);
    }

    @Test
    public void testMetricsSetterGetter() {
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("entities", 100);
        metrics.put("relationships", 50);

        exportResult.setMetrics(metrics);
        assertEquals(exportResult.getMetrics(), metrics);
        assertEquals(exportResult.getMetrics().size(), 2);

        exportResult.setMetrics(null);
        assertNull(exportResult.getMetrics());
    }

    @Test
    public void testDataSetterGetter() {
        AtlasExportData data = new AtlasExportData();
        exportResult.setData(data);
        assertEquals(exportResult.getData(), data);

        exportResult.setData(null);
        assertNull(exportResult.getData());
    }

    @Test
    public void testChangeMarkerSetterGetter() {
        long changeMarker = 99999L;
        exportResult.setChangeMarker(changeMarker);
        assertEquals(exportResult.getChangeMarker(), changeMarker);

        exportResult.setChangeMarker(0L);
        assertEquals(exportResult.getChangeMarker(), 0L);

        exportResult.setChangeMarker(Long.MIN_VALUE);
        assertEquals(exportResult.getChangeMarker(), Long.MIN_VALUE);
    }

    @Test
    public void testOperationStatusSetterGetter() {
        exportResult.setOperationStatus(OperationStatus.SUCCESS);
        assertEquals(exportResult.getOperationStatus(), OperationStatus.SUCCESS);

        exportResult.setOperationStatus(OperationStatus.PARTIAL_SUCCESS);
        assertEquals(exportResult.getOperationStatus(), OperationStatus.PARTIAL_SUCCESS);

        exportResult.setOperationStatus(OperationStatus.INPROGRESS);
        assertEquals(exportResult.getOperationStatus(), OperationStatus.INPROGRESS);

        exportResult.setOperationStatus(OperationStatus.FAIL);
        assertEquals(exportResult.getOperationStatus(), OperationStatus.FAIL);
    }

    @Test
    public void testSourceClusterNameSetterGetter() {
        String clusterName = "source-cluster";
        exportResult.setSourceClusterName(clusterName);
        assertEquals(exportResult.getSourceClusterName(), clusterName);

        exportResult.setSourceClusterName(null);
        assertNull(exportResult.getSourceClusterName());
    }

    @Test
    public void testSetMetric() {
        exportResult.setMetric("testKey", 42);
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertNotNull(metrics);
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.get("testKey"), Integer.valueOf(42));
    }

    @Test
    public void testSetMetricOverwrite() {
        exportResult.setMetric("key", 10);
        exportResult.setMetric("key", 20);
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.get("key"), Integer.valueOf(20));
    }

    @Test
    public void testIncrementMeticsCounterSingleIncrement() {
        exportResult.incrementMeticsCounter("counter1");
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.get("counter1"), Integer.valueOf(1));
    }

    @Test
    public void testIncrementMeticsCounterMultipleIncrements() {
        exportResult.incrementMeticsCounter("counter1");
        exportResult.incrementMeticsCounter("counter1");
        exportResult.incrementMeticsCounter("counter1");
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.get("counter1"), Integer.valueOf(3));
    }

    @Test
    public void testIncrementMeticsCounterWithCustomIncrement() {
        exportResult.incrementMeticsCounter("counter2", 5);
        exportResult.incrementMeticsCounter("counter2", 3);
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.get("counter2"), Integer.valueOf(8));
    }

    @Test
    public void testIncrementMeticsCounterMixedIncrements() {
        exportResult.incrementMeticsCounter("counter3");
        exportResult.incrementMeticsCounter("counter3", 10);
        exportResult.incrementMeticsCounter("counter3");
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.get("counter3"), Integer.valueOf(12));
    }

    @Test
    public void testIncrementMeticsCounterNegativeIncrement() {
        exportResult.setMetric("counter4", 10);
        exportResult.incrementMeticsCounter("counter4", -3);
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.get("counter4"), Integer.valueOf(7));
    }

    @Test
    public void testClear() {
        // Setup data
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("test", 100);
        exportResult.setMetrics(metrics);

        AtlasExportData data = new AtlasExportData();
        List<String> creationOrder = new ArrayList<>();
        creationOrder.add("entity1");
        data.setEntityCreationOrder(creationOrder);
        exportResult.setData(data);

        assertNotNull(exportResult.getMetrics());
        assertNotNull(exportResult.getData());
        assertEquals(exportResult.getMetrics().size(), 1);
        assertEquals(exportResult.getData().getEntityCreationOrder().size(), 1);

        // Clear and verify
        exportResult.clear();
        assertTrue(exportResult.getMetrics().isEmpty());
        assertTrue(exportResult.getData().getTypesDef().isEmpty());
        assertTrue(exportResult.getData().getEntityCreationOrder().isEmpty());
    }

    @Test
    public void testClearWithNullData() {
        exportResult.setData(null);
        exportResult.setMetrics(null);
        // Should not throw exception
        exportResult.clear();
        assertNull(exportResult.getData());
        assertNull(exportResult.getMetrics());
    }

    @Test
    public void testToString() {
        exportResult.setUserName("testuser");
        exportResult.setClientIpAddress("192.168.1.1");
        exportResult.setHostName("testhost");
        exportResult.setTimeStamp(1640995200000L);
        exportResult.setChangeMarker(12345L);
        exportResult.setSourceClusterName("source");
        exportResult.setOperationStatus(OperationStatus.SUCCESS);

        String result = exportResult.toString();
        assertNotNull(result);
        assertTrue(result.contains("AtlasExportResult{"));
        assertTrue(result.contains("userName='testuser'"));
        assertTrue(result.contains("clientIpAddress='192.168.1.1'"));
        assertTrue(result.contains("hostName='testhost'"));
        assertTrue(result.contains("timeStamp='1640995200000'"));
        assertTrue(result.contains("changeMarker='12345'"));
        assertTrue(result.contains("sourceCluster='source'"));
        assertTrue(result.contains("operationStatus='SUCCESS'"));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        StringBuilder result = exportResult.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasExportResult{"));
    }

    @Test
    public void testToStringWithExistingStringBuilder() {
        StringBuilder sb = new StringBuilder("Prefix: ");
        StringBuilder result = exportResult.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().startsWith("Prefix: "));
        assertTrue(result.toString().contains("AtlasExportResult{"));
    }

    @Test
    public void testOperationStatusEnum() {
        OperationStatus[] statuses = OperationStatus.values();

        assertEquals(statuses.length, 4);
        assertEquals(OperationStatus.SUCCESS.name(), "SUCCESS");
        assertEquals(OperationStatus.PARTIAL_SUCCESS.name(), "PARTIAL_SUCCESS");
        assertEquals(OperationStatus.INPROGRESS.name(), "INPROGRESS");
        assertEquals(OperationStatus.FAIL.name(), "FAIL");
    }

    @Test
    public void testEntityCountConstant() {
        assertEquals(AtlasExportResult.ENTITY_COUNT, "entityCount");
    }

    @Test
    public void testAtlasExportDataDefaultConstructor() {
        AtlasExportData data = new AtlasExportData();

        assertNotNull(data);
        assertNotNull(data.getTypesDef());
        assertNotNull(data.getEntityCreationOrder());
        assertTrue(data.getEntityCreationOrder().isEmpty());
    }

    @Test
    public void testAtlasExportDataTypesDefSetterGetter() {
        AtlasExportData data = new AtlasExportData();
        AtlasTypesDef typesDef = new AtlasTypesDef();

        data.setTypesDef(typesDef);
        assertEquals(data.getTypesDef(), typesDef);

        data.setTypesDef(null);
        assertNull(data.getTypesDef());
    }

    @Test
    public void testAtlasExportDataEntityCreationOrderSetterGetter() {
        AtlasExportData data = new AtlasExportData();
        List<String> creationOrder = new ArrayList<>();
        creationOrder.add("entity1");
        creationOrder.add("entity2");

        data.setEntityCreationOrder(creationOrder);
        assertEquals(data.getEntityCreationOrder(), creationOrder);
        assertEquals(data.getEntityCreationOrder().size(), 2);

        data.setEntityCreationOrder(null);
        assertNull(data.getEntityCreationOrder());
    }

    @Test
    public void testAtlasExportDataToString() {
        AtlasExportData data = new AtlasExportData();
        String result = data.toString();
        assertNotNull(result);
        assertTrue(result.contains("AtlasExportData {"));
        assertTrue(result.contains("typesDef={"));
        assertTrue(result.contains("entityCreationOrder={"));
    }

    @Test
    public void testAtlasExportDataToStringWithNullStringBuilder() {
        AtlasExportData data = new AtlasExportData();
        StringBuilder result = data.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("AtlasExportData {"));
    }

    @Test
    public void testAtlasExportDataToStringWithExistingStringBuilder() {
        AtlasExportData data = new AtlasExportData();
        StringBuilder sb = new StringBuilder("Prefix: ");
        StringBuilder result = data.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().startsWith("Prefix: "));
        assertTrue(result.toString().contains("AtlasExportData {"));
    }

    @Test
    public void testAtlasExportDataClear() {
        AtlasExportData data = new AtlasExportData();
        // Add some data
        List<String> creationOrder = new ArrayList<>();
        creationOrder.add("entity1");
        data.setEntityCreationOrder(creationOrder);

        assertEquals(data.getEntityCreationOrder().size(), 1);

        // Clear and verify
        data.clear();
        assertTrue(data.getEntityCreationOrder().isEmpty());
        assertTrue(data.getTypesDef().isEmpty());
    }

    @Test
    public void testAtlasExportDataClearWithNullData() {
        AtlasExportData data = new AtlasExportData();
        data.setTypesDef(null);
        data.setEntityCreationOrder(null);
        // Should not throw exception
        data.clear();
        assertNull(data.getTypesDef());
        assertNull(data.getEntityCreationOrder());
    }

    @Test
    public void testBoundaryValues() {
        // Test with extreme values
        exportResult.setTimeStamp(Long.MAX_VALUE);
        assertEquals(exportResult.getTimeStamp(), Long.MAX_VALUE);

        exportResult.setTimeStamp(Long.MIN_VALUE);
        assertEquals(exportResult.getTimeStamp(), Long.MIN_VALUE);

        exportResult.setChangeMarker(Long.MAX_VALUE);
        assertEquals(exportResult.getChangeMarker(), Long.MAX_VALUE);

        exportResult.setChangeMarker(Long.MIN_VALUE);
        assertEquals(exportResult.getChangeMarker(), Long.MIN_VALUE);
    }

    @Test
    public void testSpecialCharactersInStrings() {
        String specialUserName = "spcUser";
        String specialHostName = "spcHost";
        String specialClusterName = "spcClust";

        exportResult.setUserName(specialUserName);
        exportResult.setHostName(specialHostName);
        exportResult.setSourceClusterName(specialClusterName);

        assertEquals(exportResult.getUserName(), specialUserName);
        assertEquals(exportResult.getHostName(), specialHostName);
        assertEquals(exportResult.getSourceClusterName(), specialClusterName);
    }

    @Test
    public void testLargeMetricsMap() {
        // Test with many metrics
        for (int i = 0; i < 1000; i++) {
            exportResult.setMetric("metric" + i, i);
        }

        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.size(), 1000);
        assertEquals(metrics.get("metric500"), Integer.valueOf(500));
    }

    @Test
    public void testIncrementMeticsCounterWithZero() {
        exportResult.incrementMeticsCounter("zeroCounter", 0);
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.get("zeroCounter"), Integer.valueOf(0));
    }

    @Test
    public void testIncrementMeticsCounterWithLargeValues() {
        exportResult.incrementMeticsCounter("largeCounter", Integer.MAX_VALUE);
        Map<String, Integer> metrics = exportResult.getMetrics();
        assertEquals(metrics.get("largeCounter"), Integer.valueOf(Integer.MAX_VALUE));
    }
}
