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
package org.apache.atlas.model.instance;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNull;

public class TestAtlasEntityHeaders {
    @Test
    public void testConstructors() {
        // Test default constructor
        AtlasEntityHeaders headers = new AtlasEntityHeaders();
        assertNotNull(headers);
        assertNull(headers.getGuidHeaderMap());

        // Test constructor with guid header map
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();
        AtlasEntityHeader header1 = new AtlasEntityHeader("type1", "guid1", null);
        AtlasEntityHeader header2 = new AtlasEntityHeader("type2", "guid2", null);
        guidHeaderMap.put("guid1", header1);
        guidHeaderMap.put("guid2", header2);

        AtlasEntityHeaders headersWithMap = new AtlasEntityHeaders(guidHeaderMap);
        assertNotNull(headersWithMap);
        assertEquals(guidHeaderMap, headersWithMap.getGuidHeaderMap());
    }

    @Test
    public void testGettersAndSetters() {
        AtlasEntityHeaders headers = new AtlasEntityHeaders();

        // Test setting and getting guid header map
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();

        // Create entity headers with different types
        AtlasEntityHeader tableHeader = new AtlasEntityHeader("Table");
        tableHeader.setGuid("table-guid-1");
        tableHeader.setDisplayText("Customer Table");
        tableHeader.setStatus(AtlasEntity.Status.ACTIVE);

        AtlasEntityHeader columnHeader = new AtlasEntityHeader("Column");
        columnHeader.setGuid("column-guid-1");
        columnHeader.setDisplayText("Customer Name Column");
        columnHeader.setStatus(AtlasEntity.Status.ACTIVE);

        AtlasEntityHeader databaseHeader = new AtlasEntityHeader("Database");
        databaseHeader.setGuid("database-guid-1");
        databaseHeader.setDisplayText("Production Database");
        databaseHeader.setStatus(AtlasEntity.Status.ACTIVE);

        guidHeaderMap.put("table-guid-1", tableHeader);
        guidHeaderMap.put("column-guid-1", columnHeader);
        guidHeaderMap.put("database-guid-1", databaseHeader);

        headers.setGuidHeaderMap(guidHeaderMap);
        assertEquals(guidHeaderMap, headers.getGuidHeaderMap());
        assertEquals(3, headers.getGuidHeaderMap().size());

        // Verify individual headers
        assertEquals(tableHeader, headers.getGuidHeaderMap().get("table-guid-1"));
        assertEquals(columnHeader, headers.getGuidHeaderMap().get("column-guid-1"));
        assertEquals(databaseHeader, headers.getGuidHeaderMap().get("database-guid-1"));
    }

    @Test
    public void testNullHandling() {
        AtlasEntityHeaders headers = new AtlasEntityHeaders();

        // Test setting null guid header map
        headers.setGuidHeaderMap(null);
        assertNull(headers.getGuidHeaderMap());

        // Test constructor with null map
        AtlasEntityHeaders headersWithNull = new AtlasEntityHeaders(null);
        assertNotNull(headersWithNull);
        assertNull(headersWithNull.getGuidHeaderMap());
    }

    @Test
    public void testEmptyMap() {
        AtlasEntityHeaders headers = new AtlasEntityHeaders();

        // Test with empty map
        Map<String, AtlasEntityHeader> emptyMap = new HashMap<>();
        headers.setGuidHeaderMap(emptyMap);
        assertEquals(emptyMap, headers.getGuidHeaderMap());
        assertTrue(headers.getGuidHeaderMap().isEmpty());

        // Test constructor with empty map
        AtlasEntityHeaders headersWithEmptyMap = new AtlasEntityHeaders(emptyMap);
        assertEquals(emptyMap, headersWithEmptyMap.getGuidHeaderMap());
        assertTrue(headersWithEmptyMap.getGuidHeaderMap().isEmpty());
    }

    @Test
    public void testMapModification() {
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();
        AtlasEntityHeader header1 = new AtlasEntityHeader("type1", "guid1", null);
        guidHeaderMap.put("guid1", header1);

        AtlasEntityHeaders headers = new AtlasEntityHeaders(guidHeaderMap);

        // Verify we can modify the original map and it affects the headers
        // (since it's the same reference)
        AtlasEntityHeader header2 = new AtlasEntityHeader("type2", "guid2", null);
        guidHeaderMap.put("guid2", header2);
        assertEquals(2, headers.getGuidHeaderMap().size());
        assertTrue(headers.getGuidHeaderMap().containsKey("guid2"));

        // Verify we can modify through the getter
        AtlasEntityHeader header3 = new AtlasEntityHeader("type3", "guid3", null);
        headers.getGuidHeaderMap().put("guid3", header3);
        assertEquals(3, headers.getGuidHeaderMap().size());
        assertTrue(headers.getGuidHeaderMap().containsKey("guid3"));
    }

    @Test
    public void testLargeMap() {
        Map<String, AtlasEntityHeader> largeMap = new HashMap<>();

        // Create a large number of entity headers
        for (int i = 0; i < 1000; i++) {
            AtlasEntityHeader header = new AtlasEntityHeader("type" + i);
            header.setGuid("guid-" + i);
            header.setDisplayText("Entity " + i);
            largeMap.put("guid-" + i, header);
        }

        AtlasEntityHeaders headers = new AtlasEntityHeaders(largeMap);
        assertEquals(1000, headers.getGuidHeaderMap().size());

        // Verify some random entries
        assertTrue(headers.getGuidHeaderMap().containsKey("guid-100"));
        assertTrue(headers.getGuidHeaderMap().containsKey("guid-500"));
        assertTrue(headers.getGuidHeaderMap().containsKey("guid-999"));

        assertEquals("Entity 100", headers.getGuidHeaderMap().get("guid-100").getDisplayText());
        assertEquals("type500", headers.getGuidHeaderMap().get("guid-500").getTypeName());
    }

    @Test
    public void testSpecialCharactersInGuids() {
        AtlasEntityHeaders headers = new AtlasEntityHeaders();
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();

        // Test with special characters in GUIDs
        String[] specialGuids = {
            "guid-with-dashes",
            "guid_with_underscores",
            "guid.with.dots",
            "guid@with@ats",
            "guid with spaces",
            "guid#with#hashes",
            "guid$with$dollars"
        };

        for (String guid : specialGuids) {
            AtlasEntityHeader header = new AtlasEntityHeader("TestType");
            header.setGuid(guid);
            header.setDisplayText("Entity for " + guid);
            guidHeaderMap.put(guid, header);
        }

        headers.setGuidHeaderMap(guidHeaderMap);
        assertEquals(specialGuids.length, headers.getGuidHeaderMap().size());

        // Verify all special GUIDs are present
        for (String guid : specialGuids) {
            assertTrue(headers.getGuidHeaderMap().containsKey(guid));
            assertEquals(guid, headers.getGuidHeaderMap().get(guid).getGuid());
        }
    }

    @Test
    public void testDifferentEntityTypes() {
        AtlasEntityHeaders headers = new AtlasEntityHeaders();
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();

        // Test with various Atlas entity types
        String[] entityTypes = {
            "hive_table",
            "hive_column",
            "hive_database",
            "hdfs_path",
            "kafka_topic",
            "Process",
            "DataSet",
            "LineageInputTable",
            "LineageOutputTable"
        };

        for (int i = 0; i < entityTypes.length; i++) {
            AtlasEntityHeader header = new AtlasEntityHeader(entityTypes[i]);
            header.setGuid("guid-" + i);
            header.setDisplayText("Entity of type " + entityTypes[i]);
            header.setStatus(AtlasEntity.Status.ACTIVE);
            guidHeaderMap.put("guid-" + i, header);
        }

        headers.setGuidHeaderMap(guidHeaderMap);
        assertEquals(entityTypes.length, headers.getGuidHeaderMap().size());

        // Verify each entity type
        for (int i = 0; i < entityTypes.length; i++) {
            AtlasEntityHeader header = headers.getGuidHeaderMap().get("guid-" + i);
            assertEquals(entityTypes[i], header.getTypeName());
            assertEquals("Entity of type " + entityTypes[i], header.getDisplayText());
            assertEquals(AtlasEntity.Status.ACTIVE, header.getStatus());
        }
    }

    @Test
    public void testEntityHeadersWithClassifications() {
        AtlasEntityHeaders headers = new AtlasEntityHeaders();
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();

        // Create entity header with classifications
        AtlasEntityHeader sensitiveTableHeader = new AtlasEntityHeader("Table");
        sensitiveTableHeader.setGuid("sensitive-table-guid");
        sensitiveTableHeader.setDisplayText("Sensitive Customer Data");

        // Add classifications to the header
        AtlasClassification piiClassification = new AtlasClassification("PII");
        piiClassification.setAttribute("level", "HIGH");
        AtlasClassification confidentialClassification = new AtlasClassification("Confidential");

        java.util.List<AtlasClassification> classifications = new java.util.ArrayList<>();
        classifications.add(piiClassification);
        classifications.add(confidentialClassification);
        sensitiveTableHeader.setClassifications(classifications);

        guidHeaderMap.put("sensitive-table-guid", sensitiveTableHeader);
        headers.setGuidHeaderMap(guidHeaderMap);

        // Verify the classifications are preserved
        AtlasEntityHeader retrievedHeader = headers.getGuidHeaderMap().get("sensitive-table-guid");
        assertNotNull(retrievedHeader.getClassifications());
        assertEquals(2, retrievedHeader.getClassifications().size());

        boolean hasPII = false;
        boolean hasConfidential = false;
        for (AtlasClassification classification : retrievedHeader.getClassifications()) {
            if ("PII".equals(classification.getTypeName())) {
                hasPII = true;
                assertEquals("HIGH", classification.getAttribute("level"));
            } else if ("Confidential".equals(classification.getTypeName())) {
                hasConfidential = true;
            }
        }
        assertTrue(hasPII);
        assertTrue(hasConfidential);
    }

    @Test
    public void testEntityHeadersWithComplexAttributes() {
        AtlasEntityHeaders headers = new AtlasEntityHeaders();
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();

        // Create entity header with complex attributes
        AtlasEntityHeader tableHeader = new AtlasEntityHeader("Table");
        tableHeader.setGuid("complex-table-guid");
        tableHeader.setDisplayText("Complex Table with Attributes");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "customer_orders");
        attributes.put("owner", "data_team");
        attributes.put("createTime", System.currentTimeMillis());
        attributes.put("qualifiedName", "customer_orders@prod_cluster");
        attributes.put("comment", "Table containing customer order information");

        tableHeader.setAttributes(attributes);
        guidHeaderMap.put("complex-table-guid", tableHeader);
        headers.setGuidHeaderMap(guidHeaderMap);

        // Verify attributes are preserved
        AtlasEntityHeader retrievedHeader = headers.getGuidHeaderMap().get("complex-table-guid");
        assertNotNull(retrievedHeader.getAttributes());
        assertEquals(5, retrievedHeader.getAttributes().size());
        assertEquals("customer_orders", retrievedHeader.getAttribute("name"));
        assertEquals("data_team", retrievedHeader.getAttribute("owner"));
        assertEquals("customer_orders@prod_cluster", retrievedHeader.getAttribute("qualifiedName"));
    }

    @Test
    public void testRealWorldScenario() {
        // Simulate a real-world scenario with entity headers from different systems
        AtlasEntityHeaders headers = new AtlasEntityHeaders();
        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();

        // Hive entities
        AtlasEntityHeader hiveDatabaseHeader = new AtlasEntityHeader("hive_db");
        hiveDatabaseHeader.setGuid("hive-db-production-001");
        hiveDatabaseHeader.setDisplayText("Production Hive Database");
        hiveDatabaseHeader.setStatus(AtlasEntity.Status.ACTIVE);
        guidHeaderMap.put("hive-db-production-001", hiveDatabaseHeader);

        AtlasEntityHeader hiveTableHeader = new AtlasEntityHeader("hive_table");
        hiveTableHeader.setGuid("hive-table-customers-001");
        hiveTableHeader.setDisplayText("Customer Data Table");
        hiveTableHeader.setStatus(AtlasEntity.Status.ACTIVE);
        guidHeaderMap.put("hive-table-customers-001", hiveTableHeader);

        // Kafka entities
        AtlasEntityHeader kafkaTopicHeader = new AtlasEntityHeader("kafka_topic");
        kafkaTopicHeader.setGuid("kafka-topic-events-001");
        kafkaTopicHeader.setDisplayText("Customer Events Topic");
        kafkaTopicHeader.setStatus(AtlasEntity.Status.ACTIVE);
        guidHeaderMap.put("kafka-topic-events-001", kafkaTopicHeader);

        // Process entities
        AtlasEntityHeader etlProcessHeader = new AtlasEntityHeader("Process");
        etlProcessHeader.setGuid("etl-process-daily-001");
        etlProcessHeader.setDisplayText("Daily ETL Process");
        etlProcessHeader.setStatus(AtlasEntity.Status.ACTIVE);
        guidHeaderMap.put("etl-process-daily-001", etlProcessHeader);

        // HDFS entities
        AtlasEntityHeader hdfsPathHeader = new AtlasEntityHeader("hdfs_path");
        hdfsPathHeader.setGuid("hdfs-path-warehouse-001");
        hdfsPathHeader.setDisplayText("/data/warehouse/customers");
        hdfsPathHeader.setStatus(AtlasEntity.Status.ACTIVE);
        guidHeaderMap.put("hdfs-path-warehouse-001", hdfsPathHeader);

        headers.setGuidHeaderMap(guidHeaderMap);

        // Verify the complete scenario
        assertEquals(5, headers.getGuidHeaderMap().size());

        // Verify each system's entities
        assertTrue(headers.getGuidHeaderMap().containsKey("hive-db-production-001"));
        assertTrue(headers.getGuidHeaderMap().containsKey("hive-table-customers-001"));
        assertTrue(headers.getGuidHeaderMap().containsKey("kafka-topic-events-001"));
        assertTrue(headers.getGuidHeaderMap().containsKey("etl-process-daily-001"));
        assertTrue(headers.getGuidHeaderMap().containsKey("hdfs-path-warehouse-001"));

        // Verify entity types
        assertEquals("hive_db", headers.getGuidHeaderMap().get("hive-db-production-001").getTypeName());
        assertEquals("hive_table", headers.getGuidHeaderMap().get("hive-table-customers-001").getTypeName());
        assertEquals("kafka_topic", headers.getGuidHeaderMap().get("kafka-topic-events-001").getTypeName());
        assertEquals("Process", headers.getGuidHeaderMap().get("etl-process-daily-001").getTypeName());
        assertEquals("hdfs_path", headers.getGuidHeaderMap().get("hdfs-path-warehouse-001").getTypeName());

        // Verify all are active
        for (AtlasEntityHeader header : headers.getGuidHeaderMap().values()) {
            assertEquals(AtlasEntity.Status.ACTIVE, header.getStatus());
        }
    }

    @Test
    public void testSerializability() {
        // Verify that AtlasEntityHeaders implements Serializable correctly
        AtlasEntityHeaders headers = new AtlasEntityHeaders();
        assertTrue(headers instanceof java.io.Serializable);

        Map<String, AtlasEntityHeader> guidHeaderMap = new HashMap<>();
        AtlasEntityHeader header = new AtlasEntityHeader("TestType");
        header.setGuid("test-guid");
        guidHeaderMap.put("test-guid", header);
        headers.setGuidHeaderMap(guidHeaderMap);

        assertNotNull(headers.getGuidHeaderMap());
    }
}
