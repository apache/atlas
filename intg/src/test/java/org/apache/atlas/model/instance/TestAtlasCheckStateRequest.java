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

import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNull;

public class TestAtlasCheckStateRequest {
    @Test
    public void testConstructor() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();
        assertNotNull(request);
        assertNull(request.getEntityGuids());
        assertNull(request.getEntityTypes());
        assertFalse(request.getFixIssues()); // default should be false
    }

    @Test
    public void testGettersAndSetters() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        // Test entity guids
        Set<String> entityGuids = new HashSet<>();
        entityGuids.add("guid1");
        entityGuids.add("guid2");
        entityGuids.add("guid3");
        request.setEntityGuids(entityGuids);
        assertEquals(entityGuids, request.getEntityGuids());

        // Test entity types
        Set<String> entityTypes = new HashSet<>();
        entityTypes.add("Table");
        entityTypes.add("Column");
        entityTypes.add("Database");
        request.setEntityTypes(entityTypes);
        assertEquals(entityTypes, request.getEntityTypes());

        // Test fix issues flag
        request.setFixIssues(true);
        assertTrue(request.getFixIssues());

        request.setFixIssues(false);
        assertFalse(request.getFixIssues());
    }

    @Test
    public void testToString() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        // Test toString with null values
        String toStringNull = request.toString();
        assertNotNull(toStringNull);
        assertTrue(toStringNull.contains("AtlasCheckStateRequest"));
        assertTrue(toStringNull.contains("entityGuids=[]"));
        assertTrue(toStringNull.contains("entityTypes=[]"));
        assertTrue(toStringNull.contains("fixIssues=false"));

        // Test toString with populated data
        Set<String> entityGuids = new HashSet<>();
        entityGuids.add("guid1");
        entityGuids.add("guid2");
        request.setEntityGuids(entityGuids);

        Set<String> entityTypes = new HashSet<>();
        entityTypes.add("Table");
        entityTypes.add("Column");
        request.setEntityTypes(entityTypes);

        request.setFixIssues(true);

        String toStringWithData = request.toString();
        assertNotNull(toStringWithData);
        assertTrue(toStringWithData.contains("AtlasCheckStateRequest"));
        assertTrue(toStringWithData.contains("guid1") || toStringWithData.contains("guid2"));
        assertTrue(toStringWithData.contains("Table") || toStringWithData.contains("Column"));
        assertTrue(toStringWithData.contains("fixIssues=true"));

        StringBuilder sb = request.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("AtlasCheckStateRequest"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        request.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
        assertTrue(existingSb.toString().contains("AtlasCheckStateRequest"));
    }

    @Test
    public void testNullHandling() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        request.setEntityGuids(null);
        assertNull(request.getEntityGuids());

        request.setEntityTypes(null);
        assertNull(request.getEntityTypes());

        // Test toString with null values
        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasCheckStateRequest"));
        assertTrue(toString.contains("entityGuids=[]"));
        assertTrue(toString.contains("entityTypes=[]"));
    }

    @Test
    public void testEmptyCollections() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        // Test with empty entity guids
        Set<String> emptyEntityGuids = new HashSet<>();
        request.setEntityGuids(emptyEntityGuids);
        assertEquals(emptyEntityGuids, request.getEntityGuids());
        assertTrue(request.getEntityGuids().isEmpty());

        // Test with empty entity types
        Set<String> emptyEntityTypes = new HashSet<>();
        request.setEntityTypes(emptyEntityTypes);
        assertEquals(emptyEntityTypes, request.getEntityTypes());
        assertTrue(request.getEntityTypes().isEmpty());

        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasCheckStateRequest"));
        assertTrue(toString.contains("entityGuids=[]"));
        assertTrue(toString.contains("entityTypes=[]"));
    }

    @Test
    public void testLargeCollections() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        // Test with large number of entity guids
        Set<String> largeEntityGuids = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            largeEntityGuids.add("guid-" + i);
        }
        request.setEntityGuids(largeEntityGuids);
        assertEquals(100, request.getEntityGuids().size());

        // Test with multiple entity types
        Set<String> multipleEntityTypes = new HashSet<>();
        multipleEntityTypes.add("Table");
        multipleEntityTypes.add("Column");
        multipleEntityTypes.add("Database");
        multipleEntityTypes.add("Process");
        multipleEntityTypes.add("DataSet");
        request.setEntityTypes(multipleEntityTypes);
        assertEquals(5, request.getEntityTypes().size());

        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasCheckStateRequest"));
    }

    @Test
    public void testSpecialCharactersInGuids() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        Set<String> specialGuids = new HashSet<>();
        specialGuids.add("guid-with-dashes");
        specialGuids.add("guid_with_underscores");
        specialGuids.add("guid.with.dots");
        specialGuids.add("guid@with@ats");
        specialGuids.add("guid with spaces");
        request.setEntityGuids(specialGuids);

        assertEquals(specialGuids, request.getEntityGuids());
        assertEquals(5, request.getEntityGuids().size());

        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasCheckStateRequest"));
    }

    @Test
    public void testEntityTypesVariety() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        Set<String> entityTypes = new HashSet<>();
        entityTypes.add("hive_table");
        entityTypes.add("hive_column");
        entityTypes.add("hive_database");
        entityTypes.add("hdfs_path");
        entityTypes.add("kafka_topic");
        entityTypes.add("Process");
        entityTypes.add("DataSet");
        entityTypes.add("LineageInputTable");
        entityTypes.add("LineageOutputTable");

        request.setEntityTypes(entityTypes);
        assertEquals(entityTypes, request.getEntityTypes());
        assertEquals(9, request.getEntityTypes().size());

        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasCheckStateRequest"));
    }

    @Test
    public void testFixIssuesFlag() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        // Default should be false
        assertFalse(request.getFixIssues());

        // Test setting to true
        request.setFixIssues(true);
        assertTrue(request.getFixIssues());

        // Test setting back to false
        request.setFixIssues(false);
        assertFalse(request.getFixIssues());

        // Verify toString reflects the flag
        String toStringFalse = request.toString();
        assertTrue(toStringFalse.contains("fixIssues=false"));

        request.setFixIssues(true);
        String toStringTrue = request.toString();
        assertTrue(toStringTrue.contains("fixIssues=true"));
    }

    @Test
    public void testCollectionModification() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        Set<String> entityGuids = new HashSet<>();
        entityGuids.add("guid1");
        request.setEntityGuids(entityGuids);

        // Verify we can modify the original set and it affects the request
        // (since it's the same reference)
        entityGuids.add("guid2");
        assertEquals(2, request.getEntityGuids().size());
        assertTrue(request.getEntityGuids().contains("guid2"));

        // Verify we can modify through the getter
        request.getEntityGuids().add("guid3");
        assertEquals(3, request.getEntityGuids().size());
        assertTrue(request.getEntityGuids().contains("guid3"));
    }

    @Test
    public void testRealWorldScenario() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        // Check specific critical entities by GUID
        Set<String> criticalEntityGuids = new HashSet<>();
        criticalEntityGuids.add("customer-table-guid-123");
        criticalEntityGuids.add("order-table-guid-456");
        criticalEntityGuids.add("payment-table-guid-789");
        request.setEntityGuids(criticalEntityGuids);

        // Also check all tables and columns
        Set<String> entityTypes = new HashSet<>();
        entityTypes.add("hive_table");
        entityTypes.add("hive_column");
        request.setEntityTypes(entityTypes);

        // Enable auto-fix for issues
        request.setFixIssues(true);

        // Verify the request is properly configured
        assertEquals(criticalEntityGuids, request.getEntityGuids());
        assertEquals(entityTypes, request.getEntityTypes());
        assertTrue(request.getFixIssues());

        // Verify toString contains relevant information
        String toString = request.toString();
        assertTrue(toString.contains("customer-table-guid-123"));
        assertTrue(toString.contains("hive_table") || toString.contains("hive_column"));
        assertTrue(toString.contains("fixIssues=true"));
    }

    @Test
    public void testScenarioWithOnlyGuids() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        Set<String> entityGuids = new HashSet<>();
        entityGuids.add("important-entity-1");
        entityGuids.add("important-entity-2");
        request.setEntityGuids(entityGuids);

        request.setFixIssues(false);

        assertNotNull(request.getEntityGuids());
        assertEquals(2, request.getEntityGuids().size());
        assertNull(request.getEntityTypes());
        assertFalse(request.getFixIssues());
    }

    @Test
    public void testScenarioWithOnlyTypes() {
        AtlasCheckStateRequest request = new AtlasCheckStateRequest();

        Set<String> entityTypes = new HashSet<>();
        entityTypes.add("Process");
        entityTypes.add("DataSet");
        request.setEntityTypes(entityTypes);

        request.setFixIssues(true);

        assertNull(request.getEntityGuids());
        assertNotNull(request.getEntityTypes());
        assertEquals(2, request.getEntityTypes().size());
        assertTrue(request.getFixIssues());
    }
}
