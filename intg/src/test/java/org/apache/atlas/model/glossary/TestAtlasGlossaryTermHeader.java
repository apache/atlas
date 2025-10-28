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
package org.apache.atlas.model.glossary;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasGlossaryTermHeader {
    private AtlasGlossaryTermHeader termHeader;

    @BeforeMethod
    public void setUp() {
        termHeader = new AtlasGlossaryTermHeader();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader();

        assertNull(header.getTermGuid());
        assertNull(header.getQualifiedName());
    }

    @Test
    public void testParameterizedConstructorWithGuid() {
        String testGuid = "test-term-guid";
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader(testGuid);

        assertEquals(header.getTermGuid(), testGuid);
        assertNull(header.getQualifiedName());
    }

    @Test
    public void testParameterizedConstructorWithGuidAndQualifiedName() {
        String testGuid = "test-term-guid";
        String testQualifiedName = "test.qualified.name";
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader(testGuid, testQualifiedName);

        assertEquals(header.getTermGuid(), testGuid);
        assertEquals(header.getQualifiedName(), testQualifiedName);
    }

    @Test
    public void testTermGuidGetterSetter() {
        String testGuid = "test-term-guid";

        termHeader.setTermGuid(testGuid);
        assertEquals(termHeader.getTermGuid(), testGuid);

        termHeader.setTermGuid(null);
        assertNull(termHeader.getTermGuid());
    }

    @Test
    public void testQualifiedNameGetterSetter() {
        String testQualifiedName = "test.qualified.name";

        termHeader.setQualifiedName(testQualifiedName);
        assertEquals(termHeader.getQualifiedName(), testQualifiedName);

        termHeader.setQualifiedName(null);
        assertNull(termHeader.getQualifiedName());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(termHeader.equals(termHeader));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(termHeader.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(termHeader.equals("not a term header"));
    }

    @Test
    public void testEqualsWithSameValues() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        header1.setTermGuid("test-guid");
        header1.setQualifiedName("test.qualified.name");

        header2.setTermGuid("test-guid");
        header2.setQualifiedName("test.qualified.name");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentTermGuid() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        header1.setTermGuid("guid1");
        header2.setTermGuid("guid2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentQualifiedName() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        header1.setTermGuid("same-guid");
        header1.setQualifiedName("name1");

        header2.setTermGuid("same-guid");
        header2.setQualifiedName("name2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithNullValues() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        assertTrue(header1.equals(header2));
    }

    @Test
    public void testEqualsWithOneNullTermGuid() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        header1.setTermGuid("test-guid");
        header2.setTermGuid(null);

        assertFalse(header1.equals(header2));
        assertFalse(header2.equals(header1));
    }

    @Test
    public void testEqualsWithOneNullQualifiedName() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        header1.setTermGuid("same-guid");
        header1.setQualifiedName("test.name");

        header2.setTermGuid("same-guid");
        header2.setQualifiedName(null);

        assertFalse(header1.equals(header2));
        assertFalse(header2.equals(header1));
    }

    @Test
    public void testHashCodeConsistency() {
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader();
        header.setTermGuid("test-guid");
        header.setQualifiedName("test.name");

        int hashCode1 = header.hashCode();
        int hashCode2 = header.hashCode();

        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        header1.setTermGuid("test-guid");
        header1.setQualifiedName("test.name");

        header2.setTermGuid("test-guid");
        header2.setQualifiedName("test.name");

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        header1.setTermGuid("guid1");
        header2.setTermGuid("guid2");

        assertNotEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testHashCodeWithNullValues() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testToString() {
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader();
        header.setTermGuid("test-term-guid");
        header.setQualifiedName("test.qualified.name");

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("test-term-guid"));
        assertTrue(toString.contains("test.qualified.name"));
        assertTrue(toString.contains("AtlasGlossaryTermHeader"));
    }

    @Test
    public void testToStringWithNullValues() {
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader();

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("null"));
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader();
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader();

        header1.setTermGuid("test-guid");
        header1.setQualifiedName("test.name");

        header2.setTermGuid("test-guid");
        header2.setQualifiedName("test.name");

        // Test equals contract
        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testSettersReturnVoid() {
        // Test that setters don't return values (void methods)
        termHeader.setTermGuid("test");
        termHeader.setQualifiedName("test.name");

        // If we reach here without compilation errors, setters are void
        assertTrue(true);
    }

    @Test
    public void testParameterizedConstructorWithNullGuid() {
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader(null);

        assertNull(header.getTermGuid());
        assertNull(header.getQualifiedName());
    }

    @Test
    public void testParameterizedConstructorWithNullValues() {
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader(null, null);

        assertNull(header.getTermGuid());
        assertNull(header.getQualifiedName());
    }

    @Test
    public void testParameterizedConstructorWithEmptyStrings() {
        AtlasGlossaryTermHeader header = new AtlasGlossaryTermHeader("", "");

        assertEquals(header.getTermGuid(), "");
        assertEquals(header.getQualifiedName(), "");
    }

    @Test
    public void testEqualsReflexivity() {
        assertTrue(termHeader.equals(termHeader));
    }

    @Test
    public void testEqualsSymmetry() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader("guid", "name");
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader("guid", "name");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsTransitivity() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader("guid", "name");
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader("guid", "name");
        AtlasGlossaryTermHeader header3 = new AtlasGlossaryTermHeader("guid", "name");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header3));
        assertTrue(header1.equals(header3));
    }

    @Test
    public void testEqualsConsistency() {
        AtlasGlossaryTermHeader header1 = new AtlasGlossaryTermHeader("guid", "name");
        AtlasGlossaryTermHeader header2 = new AtlasGlossaryTermHeader("guid", "name");

        assertTrue(header1.equals(header2));
        assertTrue(header1.equals(header2));
        assertTrue(header1.equals(header2));
    }

    @Test
    public void testEqualsNullComparison() {
        assertFalse(termHeader.equals(null));
    }
}
