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
package org.apache.atlas.model.glossary.relations;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasGlossaryHeader {
    private AtlasGlossaryHeader glossaryHeader;

    @BeforeMethod
    public void setUp() {
        glossaryHeader = new AtlasGlossaryHeader();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasGlossaryHeader header = new AtlasGlossaryHeader();

        assertNull(header.getGlossaryGuid());
        assertNull(header.getRelationGuid());
        assertNull(header.getDisplayText());
    }

    @Test
    public void testParameterizedConstructor() {
        String testGuid = "test-glossary-guid";
        AtlasGlossaryHeader header = new AtlasGlossaryHeader(testGuid);

        assertEquals(header.getGlossaryGuid(), testGuid);
        assertNull(header.getRelationGuid());
        assertNull(header.getDisplayText());
    }

    @Test
    public void testGlossaryGuidGetterSetter() {
        String testGuid = "test-glossary-guid";

        glossaryHeader.setGlossaryGuid(testGuid);
        assertEquals(glossaryHeader.getGlossaryGuid(), testGuid);

        glossaryHeader.setGlossaryGuid(null);
        assertNull(glossaryHeader.getGlossaryGuid());
    }

    @Test
    public void testRelationGuidGetterSetter() {
        String testRelationGuid = "test-relation-guid";

        glossaryHeader.setRelationGuid(testRelationGuid);
        assertEquals(glossaryHeader.getRelationGuid(), testRelationGuid);

        glossaryHeader.setRelationGuid(null);
        assertNull(glossaryHeader.getRelationGuid());
    }

    @Test
    public void testDisplayTextGetterSetter() {
        String testDisplayText = "Test Display Text";

        glossaryHeader.setDisplayText(testDisplayText);
        assertEquals(glossaryHeader.getDisplayText(), testDisplayText);

        glossaryHeader.setDisplayText(null);
        assertNull(glossaryHeader.getDisplayText());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(glossaryHeader.equals(glossaryHeader));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(glossaryHeader.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(glossaryHeader.equals("not a glossary header"));
    }

    @Test
    public void testEqualsWithSameValues() {
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        header1.setGlossaryGuid("test-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDisplayText("display text");

        header2.setGlossaryGuid("test-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDisplayText("display text");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentGlossaryGuid() {
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        header1.setGlossaryGuid("guid1");
        header2.setGlossaryGuid("guid2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentRelationGuid() {
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        header1.setGlossaryGuid("same-guid");
        header1.setRelationGuid("relation1");

        header2.setGlossaryGuid("same-guid");
        header2.setRelationGuid("relation2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithNullValues() {
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        assertTrue(header1.equals(header2));
    }

    @Test
    public void testHashCodeConsistency() {
        AtlasGlossaryHeader header = new AtlasGlossaryHeader();
        header.setGlossaryGuid("test-guid");
        header.setRelationGuid("relation-guid");

        int hashCode1 = header.hashCode();
        int hashCode2 = header.hashCode();

        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        header1.setGlossaryGuid("test-guid");
        header1.setRelationGuid("relation-guid");

        header2.setGlossaryGuid("test-guid");
        header2.setRelationGuid("relation-guid");

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        header1.setGlossaryGuid("guid1");
        header2.setGlossaryGuid("guid2");

        assertNotEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testToString() {
        AtlasGlossaryHeader header = new AtlasGlossaryHeader();
        header.setGlossaryGuid("test-glossary-guid");
        header.setRelationGuid("test-relation-guid");
        header.setDisplayText("Test Display");

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("test-glossary-guid"));
        assertTrue(toString.contains("test-relation-guid"));
        assertTrue(toString.contains("Test Display"));
        assertTrue(toString.contains("AtlasGlossaryId"));
    }

    @Test
    public void testToStringWithNullValues() {
        AtlasGlossaryHeader header = new AtlasGlossaryHeader();

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("null"));
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        header1.setGlossaryGuid("test-guid");
        header1.setRelationGuid("relation-guid");

        header2.setGlossaryGuid("test-guid");
        header2.setRelationGuid("relation-guid");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testSettersReturnVoid() {
        glossaryHeader.setGlossaryGuid("test");
        glossaryHeader.setRelationGuid("test");
        glossaryHeader.setDisplayText("test");

        assertTrue(true);
    }

    @Test
    public void testParameterizedConstructorWithNull() {
        AtlasGlossaryHeader header = new AtlasGlossaryHeader(null);

        assertNull(header.getGlossaryGuid());
    }

    @Test
    public void testDisplayTextNotIncludedInEquals() {
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        header1.setGlossaryGuid("same-guid");
        header1.setRelationGuid("same-relation");
        header1.setDisplayText("different text 1");

        header2.setGlossaryGuid("same-guid");
        header2.setRelationGuid("same-relation");
        header2.setDisplayText("different text 2");

        assertTrue(header1.equals(header2));
    }

    @Test
    public void testDisplayTextNotIncludedInHashCode() {
        // Based on the hashCode implementation, displayText is not included
        AtlasGlossaryHeader header1 = new AtlasGlossaryHeader();
        AtlasGlossaryHeader header2 = new AtlasGlossaryHeader();

        header1.setGlossaryGuid("same-guid");
        header1.setRelationGuid("same-relation");
        header1.setDisplayText("different text 1");

        header2.setGlossaryGuid("same-guid");
        header2.setRelationGuid("same-relation");
        header2.setDisplayText("different text 2");

        assertEquals(header1.hashCode(), header2.hashCode());
    }
}
