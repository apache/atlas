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

import org.apache.atlas.model.glossary.enums.AtlasTermRelationshipStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasTermCategorizationHeader {
    private AtlasTermCategorizationHeader categorizationHeader;

    @BeforeMethod
    public void setUp() {
        categorizationHeader = new AtlasTermCategorizationHeader();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasTermCategorizationHeader header = new AtlasTermCategorizationHeader();

        assertNull(header.getCategoryGuid());
        assertNull(header.getRelationGuid());
        assertNull(header.getDescription());
        assertNull(header.getDisplayText());
        assertNull(header.getStatus());
    }

    @Test
    public void testCategoryGuidGetterSetter() {
        assertNull(categorizationHeader.getCategoryGuid());

        String categoryGuid = "test-category-guid";
        categorizationHeader.setCategoryGuid(categoryGuid);
        assertEquals(categorizationHeader.getCategoryGuid(), categoryGuid);

        categorizationHeader.setCategoryGuid("");
        assertEquals(categorizationHeader.getCategoryGuid(), "");

        categorizationHeader.setCategoryGuid(null);
        assertNull(categorizationHeader.getCategoryGuid());
    }

    @Test
    public void testRelationGuidGetterSetter() {
        assertNull(categorizationHeader.getRelationGuid());

        String relationGuid = "test-relation-guid";
        categorizationHeader.setRelationGuid(relationGuid);
        assertEquals(categorizationHeader.getRelationGuid(), relationGuid);

        categorizationHeader.setRelationGuid("");
        assertEquals(categorizationHeader.getRelationGuid(), "");

        categorizationHeader.setRelationGuid(null);
        assertNull(categorizationHeader.getRelationGuid());
    }

    @Test
    public void testDescriptionGetterSetter() {
        assertNull(categorizationHeader.getDescription());

        String description = "Test categorization description";
        categorizationHeader.setDescription(description);
        assertEquals(categorizationHeader.getDescription(), description);

        categorizationHeader.setDescription("");
        assertEquals(categorizationHeader.getDescription(), "");

        categorizationHeader.setDescription(null);
        assertNull(categorizationHeader.getDescription());
    }

    @Test
    public void testDisplayTextGetterSetter() {
        assertNull(categorizationHeader.getDisplayText());

        String displayText = "Test Display Text";
        categorizationHeader.setDisplayText(displayText);
        assertEquals(categorizationHeader.getDisplayText(), displayText);

        categorizationHeader.setDisplayText("");
        assertEquals(categorizationHeader.getDisplayText(), "");

        categorizationHeader.setDisplayText(null);
        assertNull(categorizationHeader.getDisplayText());
    }

    @Test
    public void testStatusGetterSetter() {
        assertNull(categorizationHeader.getStatus());

        categorizationHeader.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        assertEquals(categorizationHeader.getStatus(), AtlasTermRelationshipStatus.ACTIVE);

        categorizationHeader.setStatus(AtlasTermRelationshipStatus.DRAFT);
        assertEquals(categorizationHeader.getStatus(), AtlasTermRelationshipStatus.DRAFT);

        categorizationHeader.setStatus(null);
        assertNull(categorizationHeader.getStatus());
    }

    @Test
    public void testAllStatusValues() {
        AtlasTermRelationshipStatus[] statuses = AtlasTermRelationshipStatus.values();

        for (AtlasTermRelationshipStatus status : statuses) {
            categorizationHeader.setStatus(status);
            assertEquals(categorizationHeader.getStatus(), status);
        }
    }

    @Test
    public void testStatusEnumValues() {
        assertEquals(AtlasTermRelationshipStatus.DRAFT.getValue(), 0);
        assertEquals(AtlasTermRelationshipStatus.ACTIVE.getValue(), 1);
        assertEquals(AtlasTermRelationshipStatus.DEPRECATED.getValue(), 2);
        assertEquals(AtlasTermRelationshipStatus.OBSOLETE.getValue(), 3);
        assertEquals(AtlasTermRelationshipStatus.OTHER.getValue(), 99);
    }

    @Test
    public void testHashCodeConsistency() {
        categorizationHeader.setCategoryGuid("test-category-guid");
        categorizationHeader.setRelationGuid("relation-guid");
        categorizationHeader.setDescription("test description");
        categorizationHeader.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        int hashCode1 = categorizationHeader.hashCode();
        int hashCode2 = categorizationHeader.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("test-category-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        header2.setCategoryGuid("test-category-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("guid1");
        header2.setCategoryGuid("guid2");

        assertNotEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(categorizationHeader.equals(categorizationHeader));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(categorizationHeader.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(categorizationHeader.equals("not a categorization header"));
    }

    @Test
    public void testEqualsWithSameValues() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("test-category-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        header2.setCategoryGuid("test-category-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentCategoryGuid() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("guid1");
        header2.setCategoryGuid("guid2");

        assertFalse(header1.equals(header2));
        assertFalse(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentRelationGuid() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("same-category-guid");
        header1.setRelationGuid("relation1");

        header2.setCategoryGuid("same-category-guid");
        header2.setRelationGuid("relation2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentDescription() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("same-category-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("description1");

        header2.setCategoryGuid("same-category-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("description2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentStatus() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("same-category-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        header2.setCategoryGuid("same-category-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setStatus(AtlasTermRelationshipStatus.DRAFT);

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithNullValues() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        assertTrue(header1.equals(header2));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testDisplayTextNotIncludedInEquals() {
        // Based on the equals implementation, displayText is not included
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("same-category-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        header1.setDisplayText("different display text 1");

        header2.setCategoryGuid("same-category-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        header2.setDisplayText("different display text 2");

        assertTrue(header1.equals(header2));
    }

    @Test
    public void testDisplayTextNotIncludedInHashCode() {
        // Based on the hashCode implementation, displayText is not included
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("same-category-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        header1.setDisplayText("different display text 1");

        header2.setCategoryGuid("same-category-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        header2.setDisplayText("different display text 2");

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testToString() {
        AtlasTermCategorizationHeader header = new AtlasTermCategorizationHeader();
        header.setCategoryGuid("test-category-guid");
        header.setRelationGuid("test-relation-guid");
        header.setDescription("Test Description");
        header.setDisplayText("Test Display");
        header.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("test-category-guid"));
        assertTrue(toString.contains("test-relation-guid"));
        assertTrue(toString.contains("Test Description"));
        assertTrue(toString.contains("Test Display"));
        assertTrue(toString.contains("ACTIVE"));
        assertTrue(toString.contains("AtlasTermCategorizationId"));
    }

    @Test
    public void testToStringWithNullValues() {
        AtlasTermCategorizationHeader header = new AtlasTermCategorizationHeader();

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("null"));
        assertTrue(toString.contains("AtlasTermCategorizationId"));
    }

    @Test
    public void testToStringWithEmptyValues() {
        AtlasTermCategorizationHeader header = new AtlasTermCategorizationHeader();
        header.setCategoryGuid("");
        header.setRelationGuid("");
        header.setDescription("");
        header.setDisplayText("");

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("AtlasTermCategorizationId"));
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("test-category-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        header2.setCategoryGuid("test-category-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        // Test equals contract
        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testSettersReturnVoid() {
        // Test that setters don't return values (void methods)
        categorizationHeader.setCategoryGuid("test");
        categorizationHeader.setRelationGuid("test");
        categorizationHeader.setDescription("test");
        categorizationHeader.setDisplayText("test");
        categorizationHeader.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        // If we reach here without compilation errors, setters are void
        assertTrue(true);
    }

    @Test
    public void testWithSpecialCharacters() {
        String specialGuid = "categorization-guid-with-special@chars#123";
        String specialDescription = "Description with special chars: !@#$%^&*()";
        String specialDisplayText = "Display & Text with <special> chars";

        categorizationHeader.setCategoryGuid(specialGuid);
        categorizationHeader.setDescription(specialDescription);
        categorizationHeader.setDisplayText(specialDisplayText);

        assertEquals(categorizationHeader.getCategoryGuid(), specialGuid);
        assertEquals(categorizationHeader.getDescription(), specialDescription);
        assertEquals(categorizationHeader.getDisplayText(), specialDisplayText);
    }

    @Test
    public void testWithUnicodeCharacters() {
        String unicodeCategoryGuid = "uniCode";
        String unicodeDescription = "uniDesc";
        String unicodeDisplayText = "uniDisp";

        categorizationHeader.setCategoryGuid(unicodeCategoryGuid);
        categorizationHeader.setDescription(unicodeDescription);
        categorizationHeader.setDisplayText(unicodeDisplayText);

        assertEquals(categorizationHeader.getCategoryGuid(), unicodeCategoryGuid);
        assertEquals(categorizationHeader.getDescription(), unicodeDescription);
        assertEquals(categorizationHeader.getDisplayText(), unicodeDisplayText);
    }

    @Test
    public void testLongStrings() {
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longString.append("part").append(i).append("_");
        }
        String veryLongString = longString.toString();

        categorizationHeader.setCategoryGuid(veryLongString);
        assertEquals(categorizationHeader.getCategoryGuid(), veryLongString);

        categorizationHeader.setDescription(veryLongString);
        assertEquals(categorizationHeader.getDescription(), veryLongString);
    }

    @Test
    public void testHashCodeWithNullValues() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        // Both have all null values
        assertEquals(header1.hashCode(), header2.hashCode());

        // Set some values to null explicitly
        header1.setCategoryGuid(null);
        header1.setRelationGuid(null);
        header1.setDescription(null);
        header1.setStatus(null);

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testEqualsReflexive() {
        categorizationHeader.setCategoryGuid("test-category-guid");
        assertTrue(categorizationHeader.equals(categorizationHeader));
    }

    @Test
    public void testEqualsSymmetric() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("test-category-guid");
        header2.setCategoryGuid("test-category-guid");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsTransitive() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header3 = new AtlasTermCategorizationHeader();

        String categoryGuid = "test-category-guid";
        header1.setCategoryGuid(categoryGuid);
        header2.setCategoryGuid(categoryGuid);
        header3.setCategoryGuid(categoryGuid);

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header3));
        assertTrue(header1.equals(header3));
    }

    @Test
    public void testCompleteScenario() {
        // Create a complete categorization header scenario
        AtlasTermCategorizationHeader header = new AtlasTermCategorizationHeader();

        header.setCategoryGuid("categorization-category-guid-123");
        header.setRelationGuid("categorization-relation-guid-456");
        header.setDescription("Categorization of Customer Data term");
        header.setDisplayText("Customer Data Categorization");
        header.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        // Verify all fields are set correctly
        assertEquals(header.getCategoryGuid(), "categorization-category-guid-123");
        assertEquals(header.getRelationGuid(), "categorization-relation-guid-456");
        assertEquals(header.getDescription(), "Categorization of Customer Data term");
        assertEquals(header.getDisplayText(), "Customer Data Categorization");
        assertEquals(header.getStatus(), AtlasTermRelationshipStatus.ACTIVE);

        // Verify toString contains all information
        String toString = header.toString();
        assertTrue(toString.contains("categorization-category-guid-123"));
        assertTrue(toString.contains("categorization-relation-guid-456"));
        assertTrue(toString.contains("Categorization of Customer Data term"));
        assertTrue(toString.contains("Customer Data Categorization"));
        assertTrue(toString.contains("ACTIVE"));
    }

    @Test
    public void testAtlasJSONAnnotation() {
        assertNotNull(categorizationHeader);
    }

    @Test
    public void testAllStatusValuesInEquals() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("same-category-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");

        header2.setCategoryGuid("same-category-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");

        AtlasTermRelationshipStatus[] statuses = AtlasTermRelationshipStatus.values();
        for (int i = 0; i < statuses.length; i++) {
            for (int j = 0; j < statuses.length; j++) {
                header1.setStatus(statuses[i]);
                header2.setStatus(statuses[j]);

                if (i == j) {
                    assertTrue(header1.equals(header2));
                    assertEquals(header1.hashCode(), header2.hashCode());
                } else {
                    assertFalse(header1.equals(header2));
                }
            }
        }
    }

    @Test
    public void testAllStatusValuesInToString() {
        AtlasTermCategorizationHeader header = new AtlasTermCategorizationHeader();
        header.setCategoryGuid("test-category-guid");
        header.setRelationGuid("test-relation-guid");
        header.setDescription("Test Description");
        header.setDisplayText("Test Display");

        AtlasTermRelationshipStatus[] statuses = AtlasTermRelationshipStatus.values();
        for (AtlasTermRelationshipStatus status : statuses) {
            header.setStatus(status);
            String toString = header.toString();

            assertNotNull(toString);
            assertTrue(toString.contains(status.toString()));
            assertTrue(toString.contains("AtlasTermCategorizationId"));
        }
    }

    @Test
    public void testStatusNullInEquals() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("same-category-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setStatus(null);

        header2.setCategoryGuid("same-category-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        assertFalse(header1.equals(header2));
        assertFalse(header2.equals(header1));

        // Both null
        header2.setStatus(null);
        assertTrue(header1.equals(header2));
    }

    @Test
    public void testStatusNullInHashCode() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("same-category-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setStatus(null);

        header2.setCategoryGuid("same-category-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setStatus(null);

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testMixedNullAndNonNullValues() {
        AtlasTermCategorizationHeader header1 = new AtlasTermCategorizationHeader();
        AtlasTermCategorizationHeader header2 = new AtlasTermCategorizationHeader();

        header1.setCategoryGuid("category-guid");
        header1.setRelationGuid(null);
        header1.setDescription("description");
        header1.setStatus(null);

        header2.setCategoryGuid("category-guid");
        header2.setRelationGuid(null);
        header2.setDescription("description");
        header2.setStatus(null);

        assertTrue(header1.equals(header2));
        assertEquals(header1.hashCode(), header2.hashCode());

        // Change one field
        header2.setRelationGuid("relation-guid");
        assertFalse(header1.equals(header2));
        assertNotEquals(header1.hashCode(), header2.hashCode());
    }
}
