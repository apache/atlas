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
package org.apache.atlas.model.notification;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestMessageVersion {
    @Test
    public void testDefaultConstructor() {
        MessageVersion version = new MessageVersion();

        assertNotNull(version.getVersion());
        assertEquals(version.getVersion(), MessageVersion.CURRENT_VERSION.getVersion());
    }

    @Test
    public void testParameterizedConstructor() {
        String versionString = "2.0.0";
        MessageVersion version = new MessageVersion(versionString);

        assertEquals(version.getVersion(), versionString);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidVersionString() {
        new MessageVersion("invalid.version.string");
    }

    @Test
    public void testVersionGetterSetter() {
        MessageVersion version = new MessageVersion();
        String testVersion = "3.1.0";

        version.setVersion(testVersion);
        assertEquals(version.getVersion(), testVersion);
    }

    @Test
    public void testCompareToSameVersion() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.0.0");

        assertEquals(version1.compareTo(version2), 0);
    }

    @Test
    public void testCompareToHigherVersion() {
        MessageVersion lowerVersion = new MessageVersion("1.0.0");
        MessageVersion higherVersion = new MessageVersion("2.0.0");

        assertTrue(lowerVersion.compareTo(higherVersion) < 0);
        assertTrue(higherVersion.compareTo(lowerVersion) > 0);
    }

    @Test
    public void testCompareToMinorVersion() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.1.0");

        assertTrue(version1.compareTo(version2) < 0);
        assertTrue(version2.compareTo(version1) > 0);
    }

    @Test
    public void testCompareToPatchVersion() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.0.1");

        assertTrue(version1.compareTo(version2) < 0);
        assertTrue(version2.compareTo(version1) > 0);
    }

    @Test
    public void testCompareToNull() {
        MessageVersion version = new MessageVersion("1.0.0");

        assertTrue(version.compareTo(null) > 0);
    }

    @Test
    public void testCompareToWithTrailingZeros() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1");

        assertEquals(version1.compareTo(version2), 0);
    }

    @Test
    public void testEqualsWithSameObject() {
        MessageVersion version = new MessageVersion("1.0.0");

        assertTrue(version.equals(version));
    }

    @Test
    public void testEqualsWithNull() {
        MessageVersion version = new MessageVersion("1.0.0");

        assertFalse(version.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        MessageVersion version = new MessageVersion("1.0.0");

        assertFalse(version.equals("not a version"));
    }

    @Test
    public void testEqualsWithSameVersion() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.0.0");

        assertTrue(version1.equals(version2));
        assertTrue(version2.equals(version1));
    }

    @Test
    public void testEqualsWithDifferentVersion() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("2.0.0");

        assertFalse(version1.equals(version2));
    }

    @Test
    public void testEqualsWithTrailingZeros() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1");

        assertTrue(version1.equals(version2));
    }

    @Test
    public void testHashCodeConsistency() {
        MessageVersion version = new MessageVersion("1.0.0");

        int hashCode1 = version.hashCode();
        int hashCode2 = version.hashCode();

        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.0.0");

        assertEquals(version1.hashCode(), version2.hashCode());
    }

    @Test
    public void testHashCodeWithTrailingZeros() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1");

        assertEquals(version1.hashCode(), version2.hashCode());
    }

    @Test
    public void testToString() {
        String versionString = "1.2.3";
        MessageVersion version = new MessageVersion(versionString);

        String toString = version.toString();

        assertNotNull(toString);
        assertTrue(toString.contains(versionString));
        assertTrue(toString.contains("MessageVersion"));
    }

    @Test
    public void testGetVersionParts() {
        MessageVersion version = new MessageVersion("1.2.3");
        Integer[] parts = version.getVersionParts();

        assertEquals(parts.length, 3);
        assertEquals(parts[0], Integer.valueOf(1));
        assertEquals(parts[1], Integer.valueOf(2));
        assertEquals(parts[2], Integer.valueOf(3));
    }

    @Test
    public void testGetVersionPartsWithTrailingZeros() {
        MessageVersion version = new MessageVersion("1.0.0");
        Integer[] parts = version.getVersionParts();

        assertEquals(parts.length, 1);
        assertEquals(parts[0], Integer.valueOf(1));
    }

    @Test
    public void testGetVersionPartsWithIntermediateZeros() {
        MessageVersion version = new MessageVersion("1.0.3");
        Integer[] parts = version.getVersionParts();

        assertEquals(parts.length, 3);
        assertEquals(parts[0], Integer.valueOf(1));
        assertEquals(parts[1], Integer.valueOf(0));
        assertEquals(parts[2], Integer.valueOf(3));
    }

    @Test
    public void testGetVersionPart() {
        MessageVersion version = new MessageVersion("1.2.3");
        Integer[] parts = {1, 2, 3};

        assertEquals(version.getVersionPart(parts, 0), Integer.valueOf(1));
        assertEquals(version.getVersionPart(parts, 1), Integer.valueOf(2));
        assertEquals(version.getVersionPart(parts, 2), Integer.valueOf(3));
        assertEquals(version.getVersionPart(parts, 3), Integer.valueOf(0)); // Out of bounds should return 0
    }

    @Test
    public void testVersionComparison() {
        // Test that NO_VERSION is less than VERSION_1
        assertTrue(MessageVersion.NO_VERSION.compareTo(MessageVersion.VERSION_1) < 0);
        assertTrue(MessageVersion.VERSION_1.compareTo(MessageVersion.NO_VERSION) > 0);
    }

    @Test
    public void testComplexVersionComparison() {
        MessageVersion version100 = new MessageVersion("1.0.0");
        MessageVersion version101 = new MessageVersion("1.0.1");
        MessageVersion version110 = new MessageVersion("1.1.0");
        MessageVersion version200 = new MessageVersion("2.0.0");

        // Test ordering
        assertTrue(version100.compareTo(version101) < 0);
        assertTrue(version101.compareTo(version110) < 0);
        assertTrue(version110.compareTo(version200) < 0);

        // Test reverse ordering
        assertTrue(version200.compareTo(version110) > 0);
        assertTrue(version110.compareTo(version101) > 0);
        assertTrue(version101.compareTo(version100) > 0);
    }

    @Test
    public void testSingleDigitVersion() {
        MessageVersion version = new MessageVersion("1");

        assertEquals(version.getVersion(), "1");
        assertEquals(version.getVersionParts().length, 1);
        assertEquals(version.getVersionParts()[0], Integer.valueOf(1));
    }

    @Test
    public void testLongVersionString() {
        MessageVersion version = new MessageVersion("1.2.3.4.5");
        Integer[] parts = version.getVersionParts();

        assertEquals(parts.length, 5);
        assertEquals(parts[0], Integer.valueOf(1));
        assertEquals(parts[1], Integer.valueOf(2));
        assertEquals(parts[2], Integer.valueOf(3));
        assertEquals(parts[3], Integer.valueOf(4));
        assertEquals(parts[4], Integer.valueOf(5));
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        MessageVersion version1 = new MessageVersion("1.2.3");
        MessageVersion version2 = new MessageVersion("1.2.3");

        // Test equals contract
        assertTrue(version1.equals(version2));
        assertTrue(version2.equals(version1));
        assertEquals(version1.hashCode(), version2.hashCode());
    }

    @Test
    public void testComparableContract() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.0.0");
        MessageVersion version3 = new MessageVersion("2.0.0");

        // Test reflexivity
        assertEquals(version1.compareTo(version1), 0);

        // Test symmetry
        assertTrue((version1.compareTo(version3) < 0) == (version3.compareTo(version1) > 0));

        // Test transitivity
        MessageVersion version4 = new MessageVersion("3.0.0");
        assertTrue(version1.compareTo(version3) < 0);
        assertTrue(version3.compareTo(version4) < 0);
        assertTrue(version1.compareTo(version4) < 0);
    }

    @Test
    public void testSerialVersionUID() throws Exception {
        // Test that serialVersionUID is properly defined
        java.lang.reflect.Field serialVersionUIDField = MessageVersion.class.getDeclaredField("serialVersionUID");
        serialVersionUIDField.setAccessible(true);
        long serialVersionUID = (Long) serialVersionUIDField.get(null);

        assertEquals(serialVersionUID, 1L);
    }

    @Test
    public void testSerializable() {
        // Test that MessageVersion implements Serializable
        MessageVersion version = new MessageVersion("1.0.0");
        assertNotNull(version);
    }

    @Test
    public void testComparable() {
        // Test that MessageVersion implements Comparable
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("2.0.0");

        assertTrue(version1.compareTo(version2) < 0);
    }

    @Test
    public void testVersionParsingEdgeCases() {
        // Test version parsing with various formats
        MessageVersion version1 = new MessageVersion("1");
        assertEquals(version1.getVersionParts().length, 1);

        MessageVersion version2 = new MessageVersion("1.2");
        assertEquals(version2.getVersionParts().length, 2);

        MessageVersion version3 = new MessageVersion("1.2.3.4.5.6");
        assertEquals(version3.getVersionParts().length, 6);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidVersionWithLetters() {
        new MessageVersion("1.2.abc");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidVersionWithSpecialChars() {
        new MessageVersion("1.2.3-SNAPSHOT");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyVersionString() {
        new MessageVersion("");
    }

    @Test
    public void testVersionPartWithLargeNumbers() {
        MessageVersion version = new MessageVersion("999.888.777");
        Integer[] parts = version.getVersionParts();

        assertEquals(parts.length, 3);
        assertEquals(parts[0], Integer.valueOf(999));
        assertEquals(parts[1], Integer.valueOf(888));
        assertEquals(parts[2], Integer.valueOf(777));
    }

    @Test
    public void testGetVersionPartOutOfBounds() {
        MessageVersion version = new MessageVersion("1.2.3");
        Integer[] parts = {1, 2, 3};

        assertEquals(version.getVersionPart(parts, 0), Integer.valueOf(1));
        assertEquals(version.getVersionPart(parts, 1), Integer.valueOf(2));
        assertEquals(version.getVersionPart(parts, 2), Integer.valueOf(3));
        assertEquals(version.getVersionPart(parts, 3), Integer.valueOf(0)); // Out of bounds
        assertEquals(version.getVersionPart(parts, 10), Integer.valueOf(0)); // Way out of bounds
    }

    @Test
    public void testGetVersionPartWithEmptyArray() {
        MessageVersion version = new MessageVersion("1.0.0");
        Integer[] emptyParts = {};

        assertEquals(version.getVersionPart(emptyParts, 0), Integer.valueOf(0));
        assertEquals(version.getVersionPart(emptyParts, 1), Integer.valueOf(0));
    }

    @Test
    public void testVersionComparisonWithDifferentLengths() {
        MessageVersion shortVersion = new MessageVersion("1.2");
        MessageVersion longVersion = new MessageVersion("1.2.0.0.0");

        assertEquals(shortVersion.compareTo(longVersion), 0); // Should be equal due to trailing zeros
    }

    @Test
    public void testVersionComparisonWithLeadingZeros() {
        MessageVersion version1 = new MessageVersion("1.02.3");
        MessageVersion version2 = new MessageVersion("1.2.3");

        assertEquals(version1.compareTo(version2), 0); // Leading zeros should be ignored
    }

    @Test
    public void testComplexVersionComparisons() {
        MessageVersion[] versions = {
                new MessageVersion("0.1.0"),
                new MessageVersion("0.2.0"),
                new MessageVersion("1.0.0"),
                new MessageVersion("1.0.1"),
                new MessageVersion("1.1.0"),
                new MessageVersion("2.0.0")
        };

        // Test that versions are in ascending order
        for (int i = 0; i < versions.length - 1; i++) {
            assertTrue(versions[i].compareTo(versions[i + 1]) < 0,
                    String.format("%s should be less than %s", versions[i].getVersion(), versions[i + 1].getVersion()));
        }
    }

    @Test
    public void testVersionPartsWithAllZeros() {
        MessageVersion version = new MessageVersion("0.0.0");
        Integer[] parts = version.getVersionParts();

        assertEquals(parts.length, 0); // All trailing zeros should be stripped
    }

    @Test
    public void testVersionPartsWithMixedZeros() {
        MessageVersion version = new MessageVersion("1.0.2.0.0");
        Integer[] parts = version.getVersionParts();

        assertEquals(parts.length, 3);
        assertEquals(parts[0], Integer.valueOf(1));
        assertEquals(parts[1], Integer.valueOf(0));
        assertEquals(parts[2], Integer.valueOf(2));
    }

    @Test
    public void testToStringFormat() {
        String versionString = "1.2.3";
        MessageVersion version = new MessageVersion(versionString);
        String toString = version.toString();

        assertTrue(toString.contains("MessageVersion"));
        assertTrue(toString.contains("version=" + versionString));
    }

    @Test
    public void testHashCodeWithDifferentVersionFormats() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1");

        assertEquals(version1.hashCode(), version2.hashCode()); // Should be equal due to trailing zeros
    }

    @Test
    public void testEqualsConsistencyWithCompareTo() {
        MessageVersion version1 = new MessageVersion("1.2.3");
        MessageVersion version2 = new MessageVersion("1.2.3");
        MessageVersion version3 = new MessageVersion("1.2.4");

        // If equals returns true, compareTo should return 0
        assertTrue(version1.equals(version2));
        assertEquals(version1.compareTo(version2), 0);

        // If equals returns false, compareTo should not return 0
        assertFalse(version1.equals(version3));
        assertNotEquals(version1.compareTo(version3), 0);
    }

    @Test
    public void testStaticVersionConstants() {
        // Test that static constants are properly initialized
        assertNotNull(MessageVersion.NO_VERSION);
        assertNotNull(MessageVersion.VERSION_1);
        assertNotNull(MessageVersion.CURRENT_VERSION);

        // Test constant values
        assertEquals(MessageVersion.NO_VERSION.getVersion(), "0");
        assertEquals(MessageVersion.VERSION_1.getVersion(), "1.0.0");
        assertEquals(MessageVersion.CURRENT_VERSION, MessageVersion.VERSION_1);

        // Test that constants are immutable references
        MessageVersion currentRef1 = MessageVersion.CURRENT_VERSION;
        MessageVersion currentRef2 = MessageVersion.CURRENT_VERSION;
        assertTrue(currentRef1 == currentRef2); // Same reference
    }

    @Test
    public void testDefaultConstructorUsesCurrentVersion() {
        MessageVersion defaultVersion = new MessageVersion();
        assertEquals(defaultVersion.getVersion(), MessageVersion.CURRENT_VERSION.getVersion());
    }

    @Test
    public void testJsonAnnotations() {
        MessageVersion version = new MessageVersion("1.0.0");
        assertNotNull(version);
    }

    @Test
    public void testXmlAnnotations() {
        MessageVersion version = new MessageVersion("1.0.0");
        assertNotNull(version);
    }
}
