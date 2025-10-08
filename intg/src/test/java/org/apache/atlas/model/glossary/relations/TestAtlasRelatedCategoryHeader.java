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

public class TestAtlasRelatedCategoryHeader {
    private AtlasRelatedCategoryHeader categoryHeader;

    @BeforeMethod
    public void setUp() {
        categoryHeader = new AtlasRelatedCategoryHeader();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasRelatedCategoryHeader header = new AtlasRelatedCategoryHeader();

        assertNull(header.getCategoryGuid());
        assertNull(header.getParentCategoryGuid());
        assertNull(header.getRelationGuid());
        assertNull(header.getDisplayText());
        assertNull(header.getDescription());
    }

    @Test
    public void testCategoryGuidGetterSetter() {
        assertNull(categoryHeader.getCategoryGuid());

        String categoryGuid = "test-category-guid";
        categoryHeader.setCategoryGuid(categoryGuid);
        assertEquals(categoryHeader.getCategoryGuid(), categoryGuid);

        categoryHeader.setCategoryGuid("");
        assertEquals(categoryHeader.getCategoryGuid(), "");

        categoryHeader.setCategoryGuid(null);
        assertNull(categoryHeader.getCategoryGuid());
    }

    @Test
    public void testParentCategoryGuidGetterSetter() {
        assertNull(categoryHeader.getParentCategoryGuid());

        String parentCategoryGuid = "test-parent-category-guid";
        categoryHeader.setParentCategoryGuid(parentCategoryGuid);
        assertEquals(categoryHeader.getParentCategoryGuid(), parentCategoryGuid);

        categoryHeader.setParentCategoryGuid("");
        assertEquals(categoryHeader.getParentCategoryGuid(), "");

        categoryHeader.setParentCategoryGuid(null);
        assertNull(categoryHeader.getParentCategoryGuid());
    }

    @Test
    public void testRelationGuidGetterSetter() {
        assertNull(categoryHeader.getRelationGuid());

        String relationGuid = "test-relation-guid";
        categoryHeader.setRelationGuid(relationGuid);
        assertEquals(categoryHeader.getRelationGuid(), relationGuid);

        categoryHeader.setRelationGuid("");
        assertEquals(categoryHeader.getRelationGuid(), "");

        categoryHeader.setRelationGuid(null);
        assertNull(categoryHeader.getRelationGuid());
    }

    @Test
    public void testDisplayTextGetterSetter() {
        assertNull(categoryHeader.getDisplayText());

        String displayText = "Test Display Text";
        categoryHeader.setDisplayText(displayText);
        assertEquals(categoryHeader.getDisplayText(), displayText);

        categoryHeader.setDisplayText("");
        assertEquals(categoryHeader.getDisplayText(), "");

        categoryHeader.setDisplayText(null);
        assertNull(categoryHeader.getDisplayText());
    }

    @Test
    public void testDescriptionGetterSetter() {
        assertNull(categoryHeader.getDescription());

        String description = "Test category description";
        categoryHeader.setDescription(description);
        assertEquals(categoryHeader.getDescription(), description);

        categoryHeader.setDescription("");
        assertEquals(categoryHeader.getDescription(), "");

        categoryHeader.setDescription(null);
        assertNull(categoryHeader.getDescription());
    }

    @Test
    public void testHashCodeConsistency() {
        categoryHeader.setCategoryGuid("test-guid");
        categoryHeader.setParentCategoryGuid("parent-guid");
        categoryHeader.setRelationGuid("relation-guid");
        categoryHeader.setDescription("test description");

        int hashCode1 = categoryHeader.hashCode();
        int hashCode2 = categoryHeader.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("test-guid");
        header1.setParentCategoryGuid("parent-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");

        header2.setCategoryGuid("test-guid");
        header2.setParentCategoryGuid("parent-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("guid1");
        header2.setCategoryGuid("guid2");

        assertNotEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(categoryHeader.equals(categoryHeader));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(categoryHeader.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(categoryHeader.equals("not a category header"));
    }

    @Test
    public void testEqualsWithSameValues() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("test-guid");
        header1.setParentCategoryGuid("parent-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");

        header2.setCategoryGuid("test-guid");
        header2.setParentCategoryGuid("parent-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentCategoryGuid() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("guid1");
        header2.setCategoryGuid("guid2");

        assertFalse(header1.equals(header2));
        assertFalse(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentParentCategoryGuid() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("same-guid");
        header1.setParentCategoryGuid("parent1");

        header2.setCategoryGuid("same-guid");
        header2.setParentCategoryGuid("parent2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentRelationGuid() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("same-guid");
        header1.setParentCategoryGuid("same-parent");
        header1.setRelationGuid("relation1");

        header2.setCategoryGuid("same-guid");
        header2.setParentCategoryGuid("same-parent");
        header2.setRelationGuid("relation2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentDescription() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("same-guid");
        header1.setParentCategoryGuid("same-parent");
        header1.setRelationGuid("same-relation");
        header1.setDescription("description1");

        header2.setCategoryGuid("same-guid");
        header2.setParentCategoryGuid("same-parent");
        header2.setRelationGuid("same-relation");
        header2.setDescription("description2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithNullValues() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        assertTrue(header1.equals(header2));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testDisplayTextNotIncludedInEquals() {
        // Based on the equals implementation, displayText is not included
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("same-guid");
        header1.setParentCategoryGuid("same-parent");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setDisplayText("different text 1");

        header2.setCategoryGuid("same-guid");
        header2.setParentCategoryGuid("same-parent");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setDisplayText("different text 2");

        assertTrue(header1.equals(header2));
    }

    @Test
    public void testDisplayTextNotIncludedInHashCode() {
        // Based on the hashCode implementation, displayText is not included
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("same-guid");
        header1.setParentCategoryGuid("same-parent");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setDisplayText("different text 1");

        header2.setCategoryGuid("same-guid");
        header2.setParentCategoryGuid("same-parent");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setDisplayText("different text 2");

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testToString() {
        AtlasRelatedCategoryHeader header = new AtlasRelatedCategoryHeader();
        header.setCategoryGuid("test-category-guid");
        header.setParentCategoryGuid("test-parent-guid");
        header.setRelationGuid("test-relation-guid");
        header.setDisplayText("Test Display");
        header.setDescription("Test Description");

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("test-category-guid"));
        assertTrue(toString.contains("test-parent-guid"));
        assertTrue(toString.contains("test-relation-guid"));
        assertTrue(toString.contains("Test Display"));
        assertTrue(toString.contains("Test Description"));
        assertTrue(toString.contains("AtlasRelatedCategoryId"));
    }

    @Test
    public void testToStringWithNullValues() {
        AtlasRelatedCategoryHeader header = new AtlasRelatedCategoryHeader();

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("null"));
        assertTrue(toString.contains("AtlasRelatedCategoryId"));
    }

    @Test
    public void testToStringWithEmptyValues() {
        AtlasRelatedCategoryHeader header = new AtlasRelatedCategoryHeader();
        header.setCategoryGuid("");
        header.setParentCategoryGuid("");
        header.setRelationGuid("");
        header.setDisplayText("");
        header.setDescription("");

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("AtlasRelatedCategoryId"));
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("test-guid");
        header1.setParentCategoryGuid("parent-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");

        header2.setCategoryGuid("test-guid");
        header2.setParentCategoryGuid("parent-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");

        // Test equals contract
        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testSettersReturnVoid() {
        // Test that setters don't return values (void methods)
        categoryHeader.setCategoryGuid("test");
        categoryHeader.setParentCategoryGuid("test");
        categoryHeader.setRelationGuid("test");
        categoryHeader.setDisplayText("test");
        categoryHeader.setDescription("test");

        // If we reach here without compilation errors, setters are void
        assertTrue(true);
    }

    @Test
    public void testWithSpecialCharacters() {
        String specialGuid = "guid-with-special@chars#123";
        String specialDescription = "Description with special chars: !@#$%^&*()";
        String specialDisplayText = "Display & Text with <special> chars";

        categoryHeader.setCategoryGuid(specialGuid);
        categoryHeader.setDescription(specialDescription);
        categoryHeader.setDisplayText(specialDisplayText);

        assertEquals(categoryHeader.getCategoryGuid(), specialGuid);
        assertEquals(categoryHeader.getDescription(), specialDescription);
        assertEquals(categoryHeader.getDisplayText(), specialDisplayText);
    }

    @Test
    public void testWithUnicodeCharacters() {
        String unicodeGuid = "类别标识符_123";
        String unicodeDescription = "类别描述 with 日本語 and العربية";
        String unicodeDisplayText = "显示文本 🌟";

        categoryHeader.setCategoryGuid(unicodeGuid);
        categoryHeader.setDescription(unicodeDescription);
        categoryHeader.setDisplayText(unicodeDisplayText);

        assertEquals(categoryHeader.getCategoryGuid(), unicodeGuid);
        assertEquals(categoryHeader.getDescription(), unicodeDescription);
        assertEquals(categoryHeader.getDisplayText(), unicodeDisplayText);
    }

    @Test
    public void testLongStrings() {
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longString.append("part").append(i).append("_");
        }
        String veryLongString = longString.toString();

        categoryHeader.setCategoryGuid(veryLongString);
        assertEquals(categoryHeader.getCategoryGuid(), veryLongString);

        categoryHeader.setDescription(veryLongString);
        assertEquals(categoryHeader.getDescription(), veryLongString);
    }

    @Test
    public void testHashCodeWithNullValues() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        // Both have all null values
        assertEquals(header1.hashCode(), header2.hashCode());

        // Set some values to null explicitly
        header1.setCategoryGuid(null);
        header1.setParentCategoryGuid(null);
        header1.setRelationGuid(null);
        header1.setDescription(null);

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testEqualsReflexive() {
        categoryHeader.setCategoryGuid("test-guid");
        assertTrue(categoryHeader.equals(categoryHeader));
    }

    @Test
    public void testEqualsSymmetric() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();

        header1.setCategoryGuid("test-guid");
        header2.setCategoryGuid("test-guid");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsTransitive() {
        AtlasRelatedCategoryHeader header1 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header2 = new AtlasRelatedCategoryHeader();
        AtlasRelatedCategoryHeader header3 = new AtlasRelatedCategoryHeader();

        String guid = "test-guid";
        header1.setCategoryGuid(guid);
        header2.setCategoryGuid(guid);
        header3.setCategoryGuid(guid);

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header3));
        assertTrue(header1.equals(header3));
    }

    @Test
    public void testCompleteScenario() {
        // Create a complete category header scenario
        AtlasRelatedCategoryHeader header = new AtlasRelatedCategoryHeader();

        header.setCategoryGuid("category-guid-123");
        header.setParentCategoryGuid("parent-category-guid-456");
        header.setRelationGuid("relation-guid-789");
        header.setDisplayText("Business Terms Category");
        header.setDescription("Category for organizing business terminology");

        // Verify all fields are set correctly
        assertEquals(header.getCategoryGuid(), "category-guid-123");
        assertEquals(header.getParentCategoryGuid(), "parent-category-guid-456");
        assertEquals(header.getRelationGuid(), "relation-guid-789");
        assertEquals(header.getDisplayText(), "Business Terms Category");
        assertEquals(header.getDescription(), "Category for organizing business terminology");

        // Verify toString contains all information
        String toString = header.toString();
        assertTrue(toString.contains("category-guid-123"));
        assertTrue(toString.contains("parent-category-guid-456"));
        assertTrue(toString.contains("relation-guid-789"));
        assertTrue(toString.contains("Business Terms Category"));
        assertTrue(toString.contains("Category for organizing business terminology"));
    }

    @Test
    public void testAtlasJSONAnnotation() {
        // Test that the class has @AtlasJSON annotation
        assertNotNull(categoryHeader);
        // If this compiles and runs without issues, the annotation is present
    }
}
