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
package org.apache.atlas.model.glossary.enums;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasTermAssignmentStatus {
    @Test
    public void testEnumValues() {
        // Test that all expected enum values exist
        AtlasTermAssignmentStatus[] values = AtlasTermAssignmentStatus.values();

        assertEquals(values.length, 7, "Expected 7 enum values");

        // Verify specific values exist
        assertNotNull(AtlasTermAssignmentStatus.DISCOVERED);
        assertNotNull(AtlasTermAssignmentStatus.PROPOSED);
        assertNotNull(AtlasTermAssignmentStatus.IMPORTED);
        assertNotNull(AtlasTermAssignmentStatus.VALIDATED);
        assertNotNull(AtlasTermAssignmentStatus.DEPRECATED);
        assertNotNull(AtlasTermAssignmentStatus.OBSOLETE);
        assertNotNull(AtlasTermAssignmentStatus.OTHER);
    }

    @Test
    public void testEnumValueMapping() {
        assertEquals(AtlasTermAssignmentStatus.DISCOVERED.getValue(), 0);
        assertEquals(AtlasTermAssignmentStatus.PROPOSED.getValue(), 1);
        assertEquals(AtlasTermAssignmentStatus.IMPORTED.getValue(), 2);
        assertEquals(AtlasTermAssignmentStatus.VALIDATED.getValue(), 3);
        assertEquals(AtlasTermAssignmentStatus.DEPRECATED.getValue(), 4);
        assertEquals(AtlasTermAssignmentStatus.OBSOLETE.getValue(), 5);
        assertEquals(AtlasTermAssignmentStatus.OTHER.getValue(), 6);
    }

    @Test
    public void testValueOf() {
        assertEquals(AtlasTermAssignmentStatus.valueOf("DISCOVERED"),
                     AtlasTermAssignmentStatus.DISCOVERED);
        assertEquals(AtlasTermAssignmentStatus.valueOf("PROPOSED"),
                     AtlasTermAssignmentStatus.PROPOSED);
        assertEquals(AtlasTermAssignmentStatus.valueOf("IMPORTED"),
                     AtlasTermAssignmentStatus.IMPORTED);
        assertEquals(AtlasTermAssignmentStatus.valueOf("VALIDATED"),
                     AtlasTermAssignmentStatus.VALIDATED);
        assertEquals(AtlasTermAssignmentStatus.valueOf("DEPRECATED"),
                     AtlasTermAssignmentStatus.DEPRECATED);
        assertEquals(AtlasTermAssignmentStatus.valueOf("OBSOLETE"),
                     AtlasTermAssignmentStatus.OBSOLETE);
        assertEquals(AtlasTermAssignmentStatus.valueOf("OTHER"),
                     AtlasTermAssignmentStatus.OTHER);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValueOfInvalidValue() {
        AtlasTermAssignmentStatus.valueOf("INVALID_STATUS");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testValueOfNullValue() {
        AtlasTermAssignmentStatus.valueOf(null);
    }

    @Test
    public void testEnumOrdering() {
        AtlasTermAssignmentStatus[] values = AtlasTermAssignmentStatus.values();

        assertEquals(values[0], AtlasTermAssignmentStatus.DISCOVERED);
        assertEquals(values[1], AtlasTermAssignmentStatus.PROPOSED);
        assertEquals(values[2], AtlasTermAssignmentStatus.IMPORTED);
        assertEquals(values[3], AtlasTermAssignmentStatus.VALIDATED);
        assertEquals(values[4], AtlasTermAssignmentStatus.DEPRECATED);
        assertEquals(values[5], AtlasTermAssignmentStatus.OBSOLETE);
        assertEquals(values[6], AtlasTermAssignmentStatus.OTHER);
    }

    @Test
    public void testEnumOrdinal() {
        assertEquals(AtlasTermAssignmentStatus.DISCOVERED.ordinal(), 0);
        assertEquals(AtlasTermAssignmentStatus.PROPOSED.ordinal(), 1);
        assertEquals(AtlasTermAssignmentStatus.IMPORTED.ordinal(), 2);
        assertEquals(AtlasTermAssignmentStatus.VALIDATED.ordinal(), 3);
        assertEquals(AtlasTermAssignmentStatus.DEPRECATED.ordinal(), 4);
        assertEquals(AtlasTermAssignmentStatus.OBSOLETE.ordinal(), 5);
        assertEquals(AtlasTermAssignmentStatus.OTHER.ordinal(), 6);
    }

    @Test
    public void testEnumName() {
        assertEquals(AtlasTermAssignmentStatus.DISCOVERED.name(), "DISCOVERED");
        assertEquals(AtlasTermAssignmentStatus.PROPOSED.name(), "PROPOSED");
        assertEquals(AtlasTermAssignmentStatus.IMPORTED.name(), "IMPORTED");
        assertEquals(AtlasTermAssignmentStatus.VALIDATED.name(), "VALIDATED");
        assertEquals(AtlasTermAssignmentStatus.DEPRECATED.name(), "DEPRECATED");
        assertEquals(AtlasTermAssignmentStatus.OBSOLETE.name(), "OBSOLETE");
        assertEquals(AtlasTermAssignmentStatus.OTHER.name(), "OTHER");
    }

    @Test
    public void testToString() {
        assertEquals(AtlasTermAssignmentStatus.DISCOVERED.toString(), "DISCOVERED");
        assertEquals(AtlasTermAssignmentStatus.PROPOSED.toString(), "PROPOSED");
        assertEquals(AtlasTermAssignmentStatus.IMPORTED.toString(), "IMPORTED");
        assertEquals(AtlasTermAssignmentStatus.VALIDATED.toString(), "VALIDATED");
        assertEquals(AtlasTermAssignmentStatus.DEPRECATED.toString(), "DEPRECATED");
        assertEquals(AtlasTermAssignmentStatus.OBSOLETE.toString(), "OBSOLETE");
        assertEquals(AtlasTermAssignmentStatus.OTHER.toString(), "OTHER");
    }

    @Test
    public void testUniqueValues() {
        AtlasTermAssignmentStatus[] values = AtlasTermAssignmentStatus.values();

        for (int i = 0; i < values.length; i++) {
            for (int j = i + 1; j < values.length; j++) {
                assertTrue(values[i].getValue() != values[j].getValue(),
                          "Values should be unique: " + values[i] + " and " + values[j]);
            }
        }
    }

    @Test
    public void testCompareTo() {
        assertTrue(AtlasTermAssignmentStatus.DISCOVERED.compareTo(AtlasTermAssignmentStatus.PROPOSED) < 0);
        assertTrue(AtlasTermAssignmentStatus.PROPOSED.compareTo(AtlasTermAssignmentStatus.DISCOVERED) > 0);
        assertTrue(AtlasTermAssignmentStatus.VALIDATED.compareTo(AtlasTermAssignmentStatus.VALIDATED) == 0);
        assertTrue(AtlasTermAssignmentStatus.OTHER.compareTo(AtlasTermAssignmentStatus.DISCOVERED) > 0);
    }
}
