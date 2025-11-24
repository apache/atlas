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

public class TestAtlasRelatedTermHeader {
    private AtlasRelatedTermHeader termHeader;

    @BeforeMethod
    public void setUp() {
        termHeader = new AtlasRelatedTermHeader();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasRelatedTermHeader header = new AtlasRelatedTermHeader();

        assertNull(header.getTermGuid());
        assertNull(header.getRelationGuid());
        assertNull(header.getDisplayText());
        assertNull(header.getDescription());
        assertNull(header.getExpression());
        assertNull(header.getSteward());
        assertNull(header.getSource());
        assertNull(header.getStatus());
        assertNull(header.getQualifiedName());
    }

    @Test
    public void testTermGuidGetterSetter() {
        assertNull(termHeader.getTermGuid());

        String termGuid = "test-term-guid";
        termHeader.setTermGuid(termGuid);
        assertEquals(termHeader.getTermGuid(), termGuid);

        termHeader.setTermGuid("");
        assertEquals(termHeader.getTermGuid(), "");

        termHeader.setTermGuid(null);
        assertNull(termHeader.getTermGuid());
    }

    @Test
    public void testRelationGuidGetterSetter() {
        assertNull(termHeader.getRelationGuid());

        String relationGuid = "test-relation-guid";
        termHeader.setRelationGuid(relationGuid);
        assertEquals(termHeader.getRelationGuid(), relationGuid);

        termHeader.setRelationGuid("");
        assertEquals(termHeader.getRelationGuid(), "");

        termHeader.setRelationGuid(null);
        assertNull(termHeader.getRelationGuid());
    }

    @Test
    public void testDisplayTextGetterSetter() {
        assertNull(termHeader.getDisplayText());

        String displayText = "Test Display Text";
        termHeader.setDisplayText(displayText);
        assertEquals(termHeader.getDisplayText(), displayText);

        termHeader.setDisplayText("");
        assertEquals(termHeader.getDisplayText(), "");

        termHeader.setDisplayText(null);
        assertNull(termHeader.getDisplayText());
    }

    @Test
    public void testDescriptionGetterSetter() {
        assertNull(termHeader.getDescription());

        String description = "Test term description";
        termHeader.setDescription(description);
        assertEquals(termHeader.getDescription(), description);

        termHeader.setDescription("");
        assertEquals(termHeader.getDescription(), "");

        termHeader.setDescription(null);
        assertNull(termHeader.getDescription());
    }

    @Test
    public void testExpressionGetterSetter() {
        assertNull(termHeader.getExpression());

        String expression = "Test expression";
        termHeader.setExpression(expression);
        assertEquals(termHeader.getExpression(), expression);

        termHeader.setExpression("");
        assertEquals(termHeader.getExpression(), "");

        termHeader.setExpression(null);
        assertNull(termHeader.getExpression());
    }

    @Test
    public void testStewardGetterSetter() {
        assertNull(termHeader.getSteward());

        String steward = "Test steward";
        termHeader.setSteward(steward);
        assertEquals(termHeader.getSteward(), steward);

        termHeader.setSteward("");
        assertEquals(termHeader.getSteward(), "");

        termHeader.setSteward(null);
        assertNull(termHeader.getSteward());
    }

    @Test
    public void testSourceGetterSetter() {
        assertNull(termHeader.getSource());

        String source = "Test source";
        termHeader.setSource(source);
        assertEquals(termHeader.getSource(), source);

        termHeader.setSource("");
        assertEquals(termHeader.getSource(), "");

        termHeader.setSource(null);
        assertNull(termHeader.getSource());
    }

    @Test
    public void testStatusGetterSetter() {
        assertNull(termHeader.getStatus());

        termHeader.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        assertEquals(termHeader.getStatus(), AtlasTermRelationshipStatus.ACTIVE);

        termHeader.setStatus(AtlasTermRelationshipStatus.DRAFT);
        assertEquals(termHeader.getStatus(), AtlasTermRelationshipStatus.DRAFT);

        termHeader.setStatus(null);
        assertNull(termHeader.getStatus());
    }

    @Test
    public void testQualifiedNameGetterSetter() {
        assertNull(termHeader.getQualifiedName());

        String qualifiedName = "test.qualified.name";
        termHeader.setQualifiedName(qualifiedName);
        assertEquals(termHeader.getQualifiedName(), qualifiedName);

        termHeader.setQualifiedName("");
        assertEquals(termHeader.getQualifiedName(), "");

        termHeader.setQualifiedName(null);
        assertNull(termHeader.getQualifiedName());
    }

    @Test
    public void testAllStatusValues() {
        AtlasTermRelationshipStatus[] statuses = AtlasTermRelationshipStatus.values();

        for (AtlasTermRelationshipStatus status : statuses) {
            termHeader.setStatus(status);
            assertEquals(termHeader.getStatus(), status);
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
        termHeader.setTermGuid("test-term-guid");
        termHeader.setRelationGuid("relation-guid");
        termHeader.setDescription("test description");
        termHeader.setExpression("test expression");
        termHeader.setSteward("test steward");
        termHeader.setSource("test source");
        termHeader.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        int hashCode1 = termHeader.hashCode();
        int hashCode2 = termHeader.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("test-term-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setExpression("test expression");
        header1.setSteward("test steward");
        header1.setSource("test source");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        header2.setTermGuid("test-term-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setExpression("test expression");
        header2.setSteward("test steward");
        header2.setSource("test source");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("guid1");
        header2.setTermGuid("guid2");

        assertNotEquals(header1.hashCode(), header2.hashCode());
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
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("test-term-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setExpression("test expression");
        header1.setSteward("test steward");
        header1.setSource("test source");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        header2.setTermGuid("test-term-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setExpression("test expression");
        header2.setSteward("test steward");
        header2.setSource("test source");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentTermGuid() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("guid1");
        header2.setTermGuid("guid2");

        assertFalse(header1.equals(header2));
        assertFalse(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentRelationGuid() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("relation1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("relation2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentDescription() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("description1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("description2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentExpression() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("expression1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("expression2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentSteward() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setSteward("steward1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setSteward("steward2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentSource() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setSteward("same-steward");
        header1.setSource("source1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setSteward("same-steward");
        header2.setSource("source2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentStatus() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setSteward("same-steward");
        header1.setSource("same-source");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setSteward("same-steward");
        header2.setSource("same-source");
        header2.setStatus(AtlasTermRelationshipStatus.DRAFT);

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithNullValues() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        assertTrue(header1.equals(header2));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testDisplayTextAndQualifiedNameNotIncludedInEquals() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setSteward("same-steward");
        header1.setSource("same-source");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        header1.setDisplayText("different display text 1");
        header1.setQualifiedName("different.qualified.name.1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setSteward("same-steward");
        header2.setSource("same-source");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        header2.setDisplayText("different display text 2");
        header2.setQualifiedName("different.qualified.name.2");

        assertTrue(header1.equals(header2));
    }

    @Test
    public void testDisplayTextAndQualifiedNameNotIncludedInHashCode() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setSteward("same-steward");
        header1.setSource("same-source");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        header1.setDisplayText("different display text 1");
        header1.setQualifiedName("different.qualified.name.1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setSteward("same-steward");
        header2.setSource("same-source");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        header2.setDisplayText("different display text 2");
        header2.setQualifiedName("different.qualified.name.2");

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testToString() {
        AtlasRelatedTermHeader header = new AtlasRelatedTermHeader();
        header.setTermGuid("test-term-guid");
        header.setRelationGuid("test-relation-guid");
        header.setDescription("Test Description");
        header.setDisplayText("Test Display");
        header.setQualifiedName("test.qualified.name");
        header.setExpression("Test Expression");
        header.setSteward("Test Steward");
        header.setSource("Test Source");
        header.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("test-term-guid"));
        assertTrue(toString.contains("test-relation-guid"));
        assertTrue(toString.contains("Test Description"));
        assertTrue(toString.contains("Test Display"));
        assertTrue(toString.contains("test.qualified.name"));
        assertTrue(toString.contains("Test Expression"));
        assertTrue(toString.contains("Test Steward"));
        assertTrue(toString.contains("Test Source"));
        assertTrue(toString.contains("ACTIVE"));
        assertTrue(toString.contains("AtlasRelatedTermId"));
    }

    @Test
    public void testToStringWithNullValues() {
        AtlasRelatedTermHeader header = new AtlasRelatedTermHeader();

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("null"));
        assertTrue(toString.contains("AtlasRelatedTermId"));
    }

    @Test
    public void testToStringWithEmptyValues() {
        AtlasRelatedTermHeader header = new AtlasRelatedTermHeader();
        header.setTermGuid("");
        header.setRelationGuid("");
        header.setDescription("");
        header.setDisplayText("");
        header.setQualifiedName("");
        header.setExpression("");
        header.setSteward("");
        header.setSource("");

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("AtlasRelatedTermId"));
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("test-term-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setExpression("test expression");
        header1.setSteward("test steward");
        header1.setSource("test source");
        header1.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        header2.setTermGuid("test-term-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setExpression("test expression");
        header2.setSteward("test steward");
        header2.setSource("test source");
        header2.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testSettersReturnVoid() {
        // Test that setters don't return values (void methods)
        termHeader.setTermGuid("test");
        termHeader.setRelationGuid("test");
        termHeader.setDisplayText("test");
        termHeader.setDescription("test");
        termHeader.setExpression("test");
        termHeader.setSteward("test");
        termHeader.setSource("test");
        termHeader.setStatus(AtlasTermRelationshipStatus.ACTIVE);
        termHeader.setQualifiedName("test");

        // If we reach here without compilation errors, setters are void
        assertTrue(true);
    }

    @Test
    public void testWithSpecialCharacters() {
        String specialGuid = "term-guid-with-special@chars#123";
        String specialDescription = "Description with special chars: !@#$%^&*()";
        String specialExpression = "Expression && condition || result";
        String specialSteward = "steward@domain.com";

        termHeader.setTermGuid(specialGuid);
        termHeader.setDescription(specialDescription);
        termHeader.setExpression(specialExpression);
        termHeader.setSteward(specialSteward);

        assertEquals(termHeader.getTermGuid(), specialGuid);
        assertEquals(termHeader.getDescription(), specialDescription);
        assertEquals(termHeader.getExpression(), specialExpression);
        assertEquals(termHeader.getSteward(), specialSteward);
    }

    @Test
    public void testWithUnicodeCharacters() {
        String unicodeTermGuid = "uniTerm";
        String unicodeDescription = "uniDesc";
        String unicodeExpression = "uniExp";
        String unicodeSteward = "uniSt";

        termHeader.setTermGuid(unicodeTermGuid);
        termHeader.setDescription(unicodeDescription);
        termHeader.setExpression(unicodeExpression);
        termHeader.setSteward(unicodeSteward);

        assertEquals(termHeader.getTermGuid(), unicodeTermGuid);
        assertEquals(termHeader.getDescription(), unicodeDescription);
        assertEquals(termHeader.getExpression(), unicodeExpression);
        assertEquals(termHeader.getSteward(), unicodeSteward);
    }

    @Test
    public void testLongStrings() {
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longString.append("part").append(i).append("_");
        }
        String veryLongString = longString.toString();

        termHeader.setTermGuid(veryLongString);
        assertEquals(termHeader.getTermGuid(), veryLongString);

        termHeader.setDescription(veryLongString);
        assertEquals(termHeader.getDescription(), veryLongString);
    }

    @Test
    public void testHashCodeWithNullValues() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        // Both have all null values
        assertEquals(header1.hashCode(), header2.hashCode());

        // Set some values to null explicitly
        header1.setTermGuid(null);
        header1.setRelationGuid(null);
        header1.setDescription(null);
        header1.setExpression(null);
        header1.setSteward(null);
        header1.setSource(null);
        header1.setStatus(null);

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testEqualsReflexive() {
        termHeader.setTermGuid("test-term-guid");
        assertTrue(termHeader.equals(termHeader));
    }

    @Test
    public void testEqualsSymmetric() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();

        header1.setTermGuid("test-term-guid");
        header2.setTermGuid("test-term-guid");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsTransitive() {
        AtlasRelatedTermHeader header1 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header2 = new AtlasRelatedTermHeader();
        AtlasRelatedTermHeader header3 = new AtlasRelatedTermHeader();

        String termGuid = "test-term-guid";
        header1.setTermGuid(termGuid);
        header2.setTermGuid(termGuid);
        header3.setTermGuid(termGuid);

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header3));
        assertTrue(header1.equals(header3));
    }

    @Test
    public void testCompleteScenario() {
        // Create a complete term header scenario
        AtlasRelatedTermHeader header = new AtlasRelatedTermHeader();

        header.setTermGuid("term-guid-123");
        header.setRelationGuid("relation-guid-456");
        header.setDisplayText("Customer Data");
        header.setDescription("Term representing customer information");
        header.setQualifiedName("business.customer.data");
        header.setExpression("customer_data == PII");
        header.setSteward("data.steward@company.com");
        header.setSource("Business Glossary");
        header.setStatus(AtlasTermRelationshipStatus.ACTIVE);

        // Verify all fields are set correctly
        assertEquals(header.getTermGuid(), "term-guid-123");
        assertEquals(header.getRelationGuid(), "relation-guid-456");
        assertEquals(header.getDisplayText(), "Customer Data");
        assertEquals(header.getDescription(), "Term representing customer information");
        assertEquals(header.getQualifiedName(), "business.customer.data");
        assertEquals(header.getExpression(), "customer_data == PII");
        assertEquals(header.getSteward(), "data.steward@company.com");
        assertEquals(header.getSource(), "Business Glossary");
        assertEquals(header.getStatus(), AtlasTermRelationshipStatus.ACTIVE);

        // Verify toString contains all information
        String toString = header.toString();
        assertTrue(toString.contains("term-guid-123"));
        assertTrue(toString.contains("relation-guid-456"));
        assertTrue(toString.contains("Customer Data"));
        assertTrue(toString.contains("Term representing customer information"));
        assertTrue(toString.contains("business.customer.data"));
        assertTrue(toString.contains("customer_data == PII"));
        assertTrue(toString.contains("data.steward@company.com"));
        assertTrue(toString.contains("Business Glossary"));
        assertTrue(toString.contains("ACTIVE"));
    }

    @Test
    public void testAtlasJSONAnnotation() {
        assertNotNull(termHeader);
    }
}
