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

public class TestGuidMapping {
    @Test
    public void testConstructors() {
        // Test default constructor
        GuidMapping guidMapping = new GuidMapping();
        assertNotNull(guidMapping);
        assertNull(guidMapping.getGuidAssignments());

        // Test constructor with guid assignments
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("tempGuid1", "realGuid1");
        guidAssignments.put("tempGuid2", "realGuid2");

        GuidMapping guidMappingWithAssignments = new GuidMapping(guidAssignments);
        assertNotNull(guidMappingWithAssignments);
        assertEquals(guidAssignments, guidMappingWithAssignments.getGuidAssignments());
    }

    @Test
    public void testGettersAndSetters() {
        GuidMapping guidMapping = new GuidMapping();

        // Test setting and getting guid assignments
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("tempGuid1", "realGuid1");
        guidAssignments.put("tempGuid2", "realGuid2");
        guidAssignments.put("tempGuid3", "realGuid3");

        guidMapping.setGuidAssignments(guidAssignments);
        assertEquals(guidAssignments, guidMapping.getGuidAssignments());

        // Test setting null
        guidMapping.setGuidAssignments(null);
        assertNull(guidMapping.getGuidAssignments());
    }

    @Test
    public void testToString() {
        GuidMapping guidMapping = new GuidMapping();

        // Test toString with null assignments
        String toStringNull = guidMapping.toString();
        assertNotNull(toStringNull);
        assertTrue(toStringNull.contains("GuidMapping"));
        assertTrue(toStringNull.contains("guidAssignments="));

        // Test toString with assignments
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("tempGuid1", "realGuid1");
        guidAssignments.put("tempGuid2", "realGuid2");
        guidMapping.setGuidAssignments(guidAssignments);

        String toStringWithAssignments = guidMapping.toString();
        assertNotNull(toStringWithAssignments);
        assertTrue(toStringWithAssignments.contains("GuidMapping"));
        assertTrue(toStringWithAssignments.contains("guidAssignments="));
        assertTrue(toStringWithAssignments.contains("tempGuid1") ||
                  toStringWithAssignments.contains("tempGuid2"));

        // Test toString with StringBuilder parameter
        StringBuilder sb = guidMapping.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("GuidMapping"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        guidMapping.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
        assertTrue(existingSb.toString().contains("GuidMapping"));
    }

    @Test
    public void testEmptyGuidAssignments() {
        Map<String, String> emptyMap = new HashMap<>();
        GuidMapping guidMapping = new GuidMapping(emptyMap);

        assertEquals(emptyMap, guidMapping.getGuidAssignments());
        assertTrue(guidMapping.getGuidAssignments().isEmpty());

        String toString = guidMapping.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("GuidMapping"));
    }

    @Test
    public void testLargeGuidAssignments() {
        Map<String, String> largeMap = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            largeMap.put("tempGuid" + i, "realGuid" + i);
        }

        GuidMapping guidMapping = new GuidMapping(largeMap);
        assertEquals(largeMap, guidMapping.getGuidAssignments());
        assertEquals(100, guidMapping.getGuidAssignments().size());

        String toString = guidMapping.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("GuidMapping"));
    }

    @Test
    public void testGuidAssignmentModification() {
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("tempGuid1", "realGuid1");

        GuidMapping guidMapping = new GuidMapping(guidAssignments);

        guidAssignments.put("tempGuid2", "realGuid2");
        assertEquals(2, guidMapping.getGuidAssignments().size());
        assertTrue(guidMapping.getGuidAssignments().containsKey("tempGuid2"));

        // Verify we can modify through the getter
        guidMapping.getGuidAssignments().put("tempGuid3", "realGuid3");
        assertEquals(3, guidMapping.getGuidAssignments().size());
        assertTrue(guidMapping.getGuidAssignments().containsKey("tempGuid3"));
    }

    @Test
    public void testSpecialCharactersInGuids() {
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("temp-guid_1.special", "real-guid_1.special");
        guidAssignments.put("temp@guid#2", "real@guid#2");
        guidAssignments.put("temp guid with spaces", "real guid with spaces");

        GuidMapping guidMapping = new GuidMapping(guidAssignments);
        assertEquals(guidAssignments, guidMapping.getGuidAssignments());

        String toString = guidMapping.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("GuidMapping"));
    }

    @Test
    public void testNullValuesInGuidAssignments() {
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("tempGuid1", "realGuid1");
        guidAssignments.put("tempGuid2", null);  // null value
        guidAssignments.put(null, "realGuid3");  // null key

        GuidMapping guidMapping = new GuidMapping(guidAssignments);
        assertEquals(guidAssignments, guidMapping.getGuidAssignments());
        assertEquals(3, guidMapping.getGuidAssignments().size());

        String toString = guidMapping.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("GuidMapping"));
    }
}
