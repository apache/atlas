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

public class TestAtlasTermRelationshipStatus {
    @Test
    public void testEnumValues() {
        // Test that all expected enum values exist
        AtlasTermRelationshipStatus[] values = AtlasTermRelationshipStatus.values();

        assertEquals(values.length, 5, "Expected 5 enum values");

        // Verify specific values exist
        assertNotNull(AtlasTermRelationshipStatus.DRAFT);
        assertNotNull(AtlasTermRelationshipStatus.ACTIVE);
        assertNotNull(AtlasTermRelationshipStatus.DEPRECATED);
        assertNotNull(AtlasTermRelationshipStatus.OBSOLETE);
        assertNotNull(AtlasTermRelationshipStatus.OTHER);
    }

    @Test
    public void testEnumValueMapping() {
        // Test that enum values map to correct integers
        assertEquals(AtlasTermRelationshipStatus.DRAFT.getValue(), 0);
        assertEquals(AtlasTermRelationshipStatus.ACTIVE.getValue(), 1);
        assertEquals(AtlasTermRelationshipStatus.DEPRECATED.getValue(), 2);
        assertEquals(AtlasTermRelationshipStatus.OBSOLETE.getValue(), 3);
        assertEquals(AtlasTermRelationshipStatus.OTHER.getValue(), 99);
    }

    @Test
    public void testValueOf() {
        // Test valueOf method for all enum constants
        assertEquals(AtlasTermRelationshipStatus.valueOf("DRAFT"),
                     AtlasTermRelationshipStatus.DRAFT);
        assertEquals(AtlasTermRelationshipStatus.valueOf("ACTIVE"),
                     AtlasTermRelationshipStatus.ACTIVE);
        assertEquals(AtlasTermRelationshipStatus.valueOf("DEPRECATED"),
                     AtlasTermRelationshipStatus.DEPRECATED);
        assertEquals(AtlasTermRelationshipStatus.valueOf("OBSOLETE"),
                     AtlasTermRelationshipStatus.OBSOLETE);
        assertEquals(AtlasTermRelationshipStatus.valueOf("OTHER"),
                     AtlasTermRelationshipStatus.OTHER);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValueOfInvalidValue() {
        // Test that valueOf throws exception for invalid enum name
        AtlasTermRelationshipStatus.valueOf("INVALID_STATUS");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testValueOfNullValue() {
        // Test that valueOf throws exception for null input
        AtlasTermRelationshipStatus.valueOf(null);
    }

    @Test
    public void testEnumOrdering() {
        // Test that enum values are in the expected order
        AtlasTermRelationshipStatus[] values = AtlasTermRelationshipStatus.values();

        assertEquals(values[0], AtlasTermRelationshipStatus.DRAFT);
        assertEquals(values[1], AtlasTermRelationshipStatus.ACTIVE);
        assertEquals(values[2], AtlasTermRelationshipStatus.DEPRECATED);
        assertEquals(values[3], AtlasTermRelationshipStatus.OBSOLETE);
        assertEquals(values[4], AtlasTermRelationshipStatus.OTHER);
    }

    @Test
    public void testEnumOrdinal() {
        // Test ordinal values match expected positions
        assertEquals(AtlasTermRelationshipStatus.DRAFT.ordinal(), 0);
        assertEquals(AtlasTermRelationshipStatus.ACTIVE.ordinal(), 1);
        assertEquals(AtlasTermRelationshipStatus.DEPRECATED.ordinal(), 2);
        assertEquals(AtlasTermRelationshipStatus.OBSOLETE.ordinal(), 3);
        assertEquals(AtlasTermRelationshipStatus.OTHER.ordinal(), 4);
    }

    @Test
    public void testEnumName() {
        // Test name() method returns correct string values
        assertEquals(AtlasTermRelationshipStatus.DRAFT.name(), "DRAFT");
        assertEquals(AtlasTermRelationshipStatus.ACTIVE.name(), "ACTIVE");
        assertEquals(AtlasTermRelationshipStatus.DEPRECATED.name(), "DEPRECATED");
        assertEquals(AtlasTermRelationshipStatus.OBSOLETE.name(), "OBSOLETE");
        assertEquals(AtlasTermRelationshipStatus.OTHER.name(), "OTHER");
    }

    @Test
    public void testToString() {
        // Test toString method returns the enum name (default behavior)
        assertEquals(AtlasTermRelationshipStatus.DRAFT.toString(), "DRAFT");
        assertEquals(AtlasTermRelationshipStatus.ACTIVE.toString(), "ACTIVE");
        assertEquals(AtlasTermRelationshipStatus.DEPRECATED.toString(), "DEPRECATED");
        assertEquals(AtlasTermRelationshipStatus.OBSOLETE.toString(), "OBSOLETE");
        assertEquals(AtlasTermRelationshipStatus.OTHER.toString(), "OTHER");
    }

    @Test
    public void testSpecificValues() {
        // Test specific value mappings that are not sequential
        assertEquals(AtlasTermRelationshipStatus.OTHER.getValue(), 99,
                    "OTHER should have value 99");

        // Verify the gap in sequential values
        assertTrue(AtlasTermRelationshipStatus.OTHER.getValue() > AtlasTermRelationshipStatus.OBSOLETE.getValue(),
                  "OTHER value should be much higher than OBSOLETE");
    }

    @Test
    public void testCompareTo() {
        // Test enum comparison based on ordinal values (not getValue())
        assertTrue(AtlasTermRelationshipStatus.DRAFT.compareTo(AtlasTermRelationshipStatus.ACTIVE) < 0);
        assertTrue(AtlasTermRelationshipStatus.ACTIVE.compareTo(AtlasTermRelationshipStatus.DRAFT) > 0);
        assertTrue(AtlasTermRelationshipStatus.DEPRECATED.compareTo(AtlasTermRelationshipStatus.DEPRECATED) == 0);
        assertTrue(AtlasTermRelationshipStatus.OTHER.compareTo(AtlasTermRelationshipStatus.DRAFT) > 0);
    }

    @Test
    public void testValueConsistency() {
        // Test that getValue() returns consistent values
        for (AtlasTermRelationshipStatus status : AtlasTermRelationshipStatus.values()) {
            assertEquals(status.getValue(), status.getValue(),
                        "getValue() should return consistent values for " + status);
        }
    }
}
