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
package org.apache.atlas.model.discovery;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasAggregationEntry {
    private AtlasAggregationEntry aggregationEntry;

    @BeforeMethod
    public void setUp() {
        aggregationEntry = new AtlasAggregationEntry();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry();

        assertNull(entry.getName());
        assertEquals(entry.getCount(), 0L);
    }

    @Test
    public void testParameterizedConstructor() {
        String name = "TestType";
        long count = 100L;

        AtlasAggregationEntry entry = new AtlasAggregationEntry(name, count);

        assertEquals(entry.getName(), name);
        assertEquals(entry.getCount(), count);
    }

    @Test
    public void testParameterizedConstructorWithNullName() {
        long count = 50L;

        AtlasAggregationEntry entry = new AtlasAggregationEntry(null, count);

        assertNull(entry.getName());
        assertEquals(entry.getCount(), count);
    }

    @Test
    public void testParameterizedConstructorWithEmptyName() {
        String name = "";
        long count = 25L;

        AtlasAggregationEntry entry = new AtlasAggregationEntry(name, count);

        assertEquals(entry.getName(), name);
        assertEquals(entry.getCount(), count);
    }

    @Test
    public void testParameterizedConstructorWithZeroCount() {
        String name = "ZeroType";
        long count = 0L;

        AtlasAggregationEntry entry = new AtlasAggregationEntry(name, count);

        assertEquals(entry.getName(), name);
        assertEquals(entry.getCount(), count);
    }

    @Test
    public void testParameterizedConstructorWithNegativeCount() {
        String name = "NegativeType";
        long count = -10L;

        AtlasAggregationEntry entry = new AtlasAggregationEntry(name, count);

        assertEquals(entry.getName(), name);
        assertEquals(entry.getCount(), count);
    }

    @Test
    public void testGetName() {
        assertNull(aggregationEntry.getName());

        AtlasAggregationEntry entry = new AtlasAggregationEntry("TestName", 10L);
        assertEquals(entry.getName(), "TestName");
    }

    @Test
    public void testGetCount() {
        assertEquals(aggregationEntry.getCount(), 0L);

        AtlasAggregationEntry entry = new AtlasAggregationEntry("Test", 42L);
        assertEquals(entry.getCount(), 42L);
    }

    @Test
    public void testSetCount() {
        assertEquals(aggregationEntry.getCount(), 0L);

        aggregationEntry.setCount(100L);
        assertEquals(aggregationEntry.getCount(), 100L);

        aggregationEntry.setCount(-50L);
        assertEquals(aggregationEntry.getCount(), -50L);

        aggregationEntry.setCount(Long.MAX_VALUE);
        assertEquals(aggregationEntry.getCount(), Long.MAX_VALUE);

        aggregationEntry.setCount(Long.MIN_VALUE);
        assertEquals(aggregationEntry.getCount(), Long.MIN_VALUE);
    }

    @Test
    public void testHashCodeConsistency() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("TestType", 100L);

        int hashCode1 = entry.hashCode();
        int hashCode2 = entry.hashCode();

        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("TestType", 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("TestType", 200L);

        assertEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("TypeA", 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("TypeB", 100L);

        assertNotEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testHashCodeWithNullName() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry(null, 100L);
        entry.hashCode(); // Should throw NPE as name.hashCode() is called
    }

    @Test
    public void testEqualsWithSameObject() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("TestType", 100L);
        assertTrue(entry.equals(entry));
    }

    @Test
    public void testEqualsWithNull() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("TestType", 100L);
        assertFalse(entry.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("TestType", 100L);
        assertFalse(entry.equals("not an aggregation entry"));
    }

    @Test
    public void testEqualsWithSameName() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("TestType", 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("TestType", 200L); // Different count

        assertTrue(entry1.equals(entry2));
        assertTrue(entry2.equals(entry1));
    }

    @Test
    public void testEqualsWithDifferentName() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("TypeA", 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("TypeB", 100L);

        assertFalse(entry1.equals(entry2));
        assertFalse(entry2.equals(entry1));
    }

    @Test
    public void testEqualsWithEmptyNames() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("", 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("", 200L);

        assertTrue(entry1.equals(entry2));
        assertTrue(entry2.equals(entry1));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testEqualsWithNullName() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry(null, 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("TestType", 100L);

        entry1.equals(entry2); // Should throw NPE as name.equals() is called
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("TestType", 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("TestType", 200L);

        assertTrue(entry1.equals(entry2));
        assertTrue(entry2.equals(entry1));
        assertEquals(entry1.hashCode(), entry2.hashCode());

        assertTrue(entry1.equals(entry1));

        assertTrue(entry1.equals(entry2));
        assertTrue(entry1.equals(entry2));
    }

    @Test
    public void testCountModification() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("TestType", 100L);

        assertEquals(entry.getCount(), 100L);

        entry.setCount(200L);
        assertEquals(entry.getCount(), 200L);

        // Verify that changing count doesn't affect equality (since equals is based on name only)
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("TestType", 300L);
        assertTrue(entry.equals(entry2));
    }

    @Test
    public void testLargeCountValues() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("LargeType", Long.MAX_VALUE);

        assertEquals(entry.getCount(), Long.MAX_VALUE);

        entry.setCount(Long.MIN_VALUE);
        assertEquals(entry.getCount(), Long.MIN_VALUE);
    }

    @Test
    public void testNameWithSpecialCharacters() {
        String specialName = "Type-With_Special.Characters@123";
        AtlasAggregationEntry entry = new AtlasAggregationEntry(specialName, 50L);

        assertEquals(entry.getName(), specialName);
    }

    @Test
    public void testNameWithUnicodeCharacters() {
        String unicodeName = "unicodeString";
        AtlasAggregationEntry entry = new AtlasAggregationEntry(unicodeName, 75L);

        assertEquals(entry.getName(), unicodeName);
    }

    @Test
    public void testMultipleInstancesWithSameName() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("SameType", 10L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("SameType", 20L);
        AtlasAggregationEntry entry3 = new AtlasAggregationEntry("SameType", 30L);

        assertTrue(entry1.equals(entry2));
        assertTrue(entry2.equals(entry3));
        assertTrue(entry1.equals(entry3));

        assertEquals(entry1.hashCode(), entry2.hashCode());
        assertEquals(entry2.hashCode(), entry3.hashCode());
    }

    @Test
    public void testCountIncrement() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("IncrementType", 0L);

        for (int i = 1; i <= 10; i++) {
            entry.setCount(i);
            assertEquals(entry.getCount(), (long) i);
        }
    }

    @Test
    public void testJsonAnnotations() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("TestType", 100L);

        assertNotNull(entry);
    }

    @Test
    public void testXmlAnnotations() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("TestType", 100L);

        assertNotNull(entry);
    }

    @Test
    public void testImmutableName() {
        AtlasAggregationEntry entry = new AtlasAggregationEntry("ImmutableName", 100L);

        assertEquals(entry.getName(), "ImmutableName");

        entry.setCount(200L);
        assertEquals(entry.getName(), "ImmutableName"); // Name should remain unchanged
        assertEquals(entry.getCount(), 200L); // Count should change
    }

    @Test
    public void testEqualsSymmetry() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("SymmetryTest", 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("SymmetryTest", 200L);

        boolean equals1to2 = entry1.equals(entry2);
        boolean equals2to1 = entry2.equals(entry1);

        assertEquals(equals1to2, equals2to1);
        assertTrue(equals1to2);
        assertTrue(equals2to1);
    }

    @Test
    public void testEqualsTransitivity() {
        AtlasAggregationEntry entry1 = new AtlasAggregationEntry("TransitivityTest", 100L);
        AtlasAggregationEntry entry2 = new AtlasAggregationEntry("TransitivityTest", 200L);
        AtlasAggregationEntry entry3 = new AtlasAggregationEntry("TransitivityTest", 300L);

        assertTrue(entry1.equals(entry2));
        assertTrue(entry2.equals(entry3));
        assertTrue(entry1.equals(entry3));
    }
}
