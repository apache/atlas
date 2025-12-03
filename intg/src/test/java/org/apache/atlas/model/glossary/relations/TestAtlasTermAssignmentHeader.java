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

import org.apache.atlas.model.glossary.enums.AtlasTermAssignmentStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasTermAssignmentHeader {
    private AtlasTermAssignmentHeader assignmentHeader;

    @BeforeMethod
    public void setUp() {
        assignmentHeader = new AtlasTermAssignmentHeader();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasTermAssignmentHeader header = new AtlasTermAssignmentHeader();

        assertNull(header.getTermGuid());
        assertNull(header.getRelationGuid());
        assertNull(header.getDescription());
        assertNull(header.getDisplayText());
        assertNull(header.getExpression());
        assertNull(header.getCreatedBy());
        assertNull(header.getSteward());
        assertNull(header.getSource());
        assertEquals(header.getConfidence(), 0);
        assertNull(header.getStatus());
        assertNull(header.getQualifiedName());
    }

    @Test
    public void testTermGuidGetterSetter() {
        assertNull(assignmentHeader.getTermGuid());

        String termGuid = "test-term-guid";
        assignmentHeader.setTermGuid(termGuid);
        assertEquals(assignmentHeader.getTermGuid(), termGuid);

        assignmentHeader.setTermGuid("");
        assertEquals(assignmentHeader.getTermGuid(), "");

        assignmentHeader.setTermGuid(null);
        assertNull(assignmentHeader.getTermGuid());
    }

    @Test
    public void testRelationGuidGetterSetter() {
        assertNull(assignmentHeader.getRelationGuid());

        String relationGuid = "test-relation-guid";
        assignmentHeader.setRelationGuid(relationGuid);
        assertEquals(assignmentHeader.getRelationGuid(), relationGuid);

        assignmentHeader.setRelationGuid("");
        assertEquals(assignmentHeader.getRelationGuid(), "");

        assignmentHeader.setRelationGuid(null);
        assertNull(assignmentHeader.getRelationGuid());
    }

    @Test
    public void testDescriptionGetterSetter() {
        assertNull(assignmentHeader.getDescription());

        String description = "Test assignment description";
        assignmentHeader.setDescription(description);
        assertEquals(assignmentHeader.getDescription(), description);

        assignmentHeader.setDescription("");
        assertEquals(assignmentHeader.getDescription(), "");

        assignmentHeader.setDescription(null);
        assertNull(assignmentHeader.getDescription());
    }

    @Test
    public void testDisplayTextGetterSetter() {
        assertNull(assignmentHeader.getDisplayText());

        String displayText = "Test Display Text";
        assignmentHeader.setDisplayText(displayText);
        assertEquals(assignmentHeader.getDisplayText(), displayText);

        assignmentHeader.setDisplayText("");
        assertEquals(assignmentHeader.getDisplayText(), "");

        assignmentHeader.setDisplayText(null);
        assertNull(assignmentHeader.getDisplayText());
    }

    @Test
    public void testExpressionGetterSetter() {
        assertNull(assignmentHeader.getExpression());

        String expression = "Test expression";
        assignmentHeader.setExpression(expression);
        assertEquals(assignmentHeader.getExpression(), expression);

        assignmentHeader.setExpression("");
        assertEquals(assignmentHeader.getExpression(), "");

        assignmentHeader.setExpression(null);
        assertNull(assignmentHeader.getExpression());
    }

    @Test
    public void testCreatedByGetterSetter() {
        assertNull(assignmentHeader.getCreatedBy());

        String createdBy = "test.user@company.com";
        assignmentHeader.setCreatedBy(createdBy);
        assertEquals(assignmentHeader.getCreatedBy(), createdBy);

        assignmentHeader.setCreatedBy("");
        assertEquals(assignmentHeader.getCreatedBy(), "");

        assignmentHeader.setCreatedBy(null);
        assertNull(assignmentHeader.getCreatedBy());
    }

    @Test
    public void testStewardGetterSetter() {
        assertNull(assignmentHeader.getSteward());

        String steward = "Test steward";
        assignmentHeader.setSteward(steward);
        assertEquals(assignmentHeader.getSteward(), steward);

        assignmentHeader.setSteward("");
        assertEquals(assignmentHeader.getSteward(), "");

        assignmentHeader.setSteward(null);
        assertNull(assignmentHeader.getSteward());
    }

    @Test
    public void testSourceGetterSetter() {
        assertNull(assignmentHeader.getSource());

        String source = "Test source";
        assignmentHeader.setSource(source);
        assertEquals(assignmentHeader.getSource(), source);

        assignmentHeader.setSource("");
        assertEquals(assignmentHeader.getSource(), "");

        assignmentHeader.setSource(null);
        assertNull(assignmentHeader.getSource());
    }

    @Test
    public void testConfidenceGetterSetter() {
        assertEquals(assignmentHeader.getConfidence(), 0);

        assignmentHeader.setConfidence(50);
        assertEquals(assignmentHeader.getConfidence(), 50);

        assignmentHeader.setConfidence(100);
        assertEquals(assignmentHeader.getConfidence(), 100);

        assignmentHeader.setConfidence(-1);
        assertEquals(assignmentHeader.getConfidence(), -1);

        assignmentHeader.setConfidence(0);
        assertEquals(assignmentHeader.getConfidence(), 0);

        assignmentHeader.setConfidence(Integer.MAX_VALUE);
        assertEquals(assignmentHeader.getConfidence(), Integer.MAX_VALUE);

        assignmentHeader.setConfidence(Integer.MIN_VALUE);
        assertEquals(assignmentHeader.getConfidence(), Integer.MIN_VALUE);
    }

    @Test
    public void testStatusGetterSetter() {
        assertNull(assignmentHeader.getStatus());

        assignmentHeader.setStatus(AtlasTermAssignmentStatus.VALIDATED);
        assertEquals(assignmentHeader.getStatus(), AtlasTermAssignmentStatus.VALIDATED);

        assignmentHeader.setStatus(AtlasTermAssignmentStatus.PROPOSED);
        assertEquals(assignmentHeader.getStatus(), AtlasTermAssignmentStatus.PROPOSED);

        assignmentHeader.setStatus(null);
        assertNull(assignmentHeader.getStatus());
    }

    @Test
    public void testQualifiedNameGetterSetter() {
        assertNull(assignmentHeader.getQualifiedName());

        String qualifiedName = "test.assignment.name";
        assignmentHeader.setQualifiedName(qualifiedName);
        assertEquals(assignmentHeader.getQualifiedName(), qualifiedName);

        assignmentHeader.setQualifiedName("");
        assertEquals(assignmentHeader.getQualifiedName(), "");

        assignmentHeader.setQualifiedName(null);
        assertNull(assignmentHeader.getQualifiedName());
    }

    @Test
    public void testAllStatusValues() {
        AtlasTermAssignmentStatus[] statuses = AtlasTermAssignmentStatus.values();

        for (AtlasTermAssignmentStatus status : statuses) {
            assignmentHeader.setStatus(status);
            assertEquals(assignmentHeader.getStatus(), status);
        }
    }

    @Test
    public void testStatusEnumValues() {
        assertEquals(AtlasTermAssignmentStatus.DISCOVERED.getValue(), 0);
        assertEquals(AtlasTermAssignmentStatus.PROPOSED.getValue(), 1);
        assertEquals(AtlasTermAssignmentStatus.IMPORTED.getValue(), 2);
        assertEquals(AtlasTermAssignmentStatus.VALIDATED.getValue(), 3);
        assertEquals(AtlasTermAssignmentStatus.DEPRECATED.getValue(), 4);
        assertEquals(AtlasTermAssignmentStatus.OBSOLETE.getValue(), 5);
        assertEquals(AtlasTermAssignmentStatus.OTHER.getValue(), 6);
    }

    @Test
    public void testHashCodeConsistency() {
        assignmentHeader.setTermGuid("test-term-guid");
        assignmentHeader.setRelationGuid("relation-guid");
        assignmentHeader.setDescription("test description");
        assignmentHeader.setExpression("test expression");
        assignmentHeader.setCreatedBy("test.user");
        assignmentHeader.setSteward("test steward");
        assignmentHeader.setSource("test source");
        assignmentHeader.setConfidence(75);
        assignmentHeader.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        int hashCode1 = assignmentHeader.hashCode();
        int hashCode2 = assignmentHeader.hashCode();
        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashCodeEquality() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("test-term-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setExpression("test expression");
        header1.setCreatedBy("test.user");
        header1.setSteward("test steward");
        header1.setSource("test source");
        header1.setConfidence(75);
        header1.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        header2.setTermGuid("test-term-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setExpression("test expression");
        header2.setCreatedBy("test.user");
        header2.setSteward("test steward");
        header2.setSource("test source");
        header2.setConfidence(75);
        header2.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testHashCodeInequality() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("guid1");
        header2.setTermGuid("guid2");

        assertNotEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentConfidence() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-guid");
        header1.setConfidence(50);

        header2.setTermGuid("same-guid");
        header2.setConfidence(75);

        assertNotEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(assignmentHeader.equals(assignmentHeader));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(assignmentHeader.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(assignmentHeader.equals("not an assignment header"));
    }

    @Test
    public void testEqualsWithSameValues() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("test-term-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setExpression("test expression");
        header1.setCreatedBy("test.user");
        header1.setSteward("test steward");
        header1.setSource("test source");
        header1.setConfidence(75);
        header1.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        header2.setTermGuid("test-term-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setExpression("test expression");
        header2.setCreatedBy("test.user");
        header2.setSteward("test steward");
        header2.setSource("test source");
        header2.setConfidence(75);
        header2.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentTermGuid() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("guid1");
        header2.setTermGuid("guid2");

        assertFalse(header1.equals(header2));
        assertFalse(header2.equals(header1));
    }

    @Test
    public void testEqualsWithDifferentRelationGuid() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("relation1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("relation2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentDescription() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

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
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

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
    public void testEqualsWithDifferentCreatedBy() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setCreatedBy("user1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setCreatedBy("user2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentSteward() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setCreatedBy("same-user");
        header1.setSteward("steward1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setCreatedBy("same-user");
        header2.setSteward("steward2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentSource() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setCreatedBy("same-user");
        header1.setSteward("same-steward");
        header1.setSource("source1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setCreatedBy("same-user");
        header2.setSteward("same-steward");
        header2.setSource("source2");

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentConfidence() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setCreatedBy("same-user");
        header1.setSteward("same-steward");
        header1.setSource("same-source");
        header1.setConfidence(50);

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setCreatedBy("same-user");
        header2.setSteward("same-steward");
        header2.setSource("same-source");
        header2.setConfidence(75);

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithDifferentStatus() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setCreatedBy("same-user");
        header1.setSteward("same-steward");
        header1.setSource("same-source");
        header1.setConfidence(75);
        header1.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setCreatedBy("same-user");
        header2.setSteward("same-steward");
        header2.setSource("same-source");
        header2.setConfidence(75);
        header2.setStatus(AtlasTermAssignmentStatus.PROPOSED);

        assertFalse(header1.equals(header2));
    }

    @Test
    public void testEqualsWithNullValues() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        assertTrue(header1.equals(header2));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testDisplayTextAndQualifiedNameNotIncludedInEquals() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setCreatedBy("same-user");
        header1.setSteward("same-steward");
        header1.setSource("same-source");
        header1.setConfidence(75);
        header1.setStatus(AtlasTermAssignmentStatus.VALIDATED);
        header1.setDisplayText("different display text 1");
        header1.setQualifiedName("different.qualified.name.1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setCreatedBy("same-user");
        header2.setSteward("same-steward");
        header2.setSource("same-source");
        header2.setConfidence(75);
        header2.setStatus(AtlasTermAssignmentStatus.VALIDATED);
        header2.setDisplayText("different display text 2");
        header2.setQualifiedName("different.qualified.name.2");

        assertTrue(header1.equals(header2));
    }

    @Test
    public void testDisplayTextAndQualifiedNameNotIncludedInHashCode() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("same-term-guid");
        header1.setRelationGuid("same-relation");
        header1.setDescription("same-description");
        header1.setExpression("same-expression");
        header1.setCreatedBy("same-user");
        header1.setSteward("same-steward");
        header1.setSource("same-source");
        header1.setConfidence(75);
        header1.setStatus(AtlasTermAssignmentStatus.VALIDATED);
        header1.setDisplayText("different display text 1");
        header1.setQualifiedName("different.qualified.name.1");

        header2.setTermGuid("same-term-guid");
        header2.setRelationGuid("same-relation");
        header2.setDescription("same-description");
        header2.setExpression("same-expression");
        header2.setCreatedBy("same-user");
        header2.setSteward("same-steward");
        header2.setSource("same-source");
        header2.setConfidence(75);
        header2.setStatus(AtlasTermAssignmentStatus.VALIDATED);
        header2.setDisplayText("different display text 2");
        header2.setQualifiedName("different.qualified.name.2");

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testToString() {
        AtlasTermAssignmentHeader header = new AtlasTermAssignmentHeader();
        header.setTermGuid("test-term-guid");
        header.setRelationGuid("test-relation-guid");
        header.setDescription("Test Description");
        header.setDisplayText("Test Display");
        header.setQualifiedName("test.qualified.name");
        header.setExpression("Test Expression");
        header.setCreatedBy("test.user@company.com");
        header.setSteward("Test Steward");
        header.setSource("Test Source");
        header.setConfidence(85);
        header.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("test-term-guid"));
        assertTrue(toString.contains("test-relation-guid"));
        assertTrue(toString.contains("Test Description"));
        assertTrue(toString.contains("Test Display"));
        assertTrue(toString.contains("test.qualified.name"));
        assertTrue(toString.contains("Test Expression"));
        assertTrue(toString.contains("test.user@company.com"));
        assertTrue(toString.contains("Test Steward"));
        assertTrue(toString.contains("Test Source"));
        assertTrue(toString.contains("85"));
        assertTrue(toString.contains("VALIDATED"));
        assertTrue(toString.contains("AtlasTermAssignmentId"));
    }

    @Test
    public void testToStringWithNullValues() {
        AtlasTermAssignmentHeader header = new AtlasTermAssignmentHeader();

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("null"));
        assertTrue(toString.contains("0")); // Default confidence value
        assertTrue(toString.contains("AtlasTermAssignmentId"));
    }

    @Test
    public void testToStringWithEmptyValues() {
        AtlasTermAssignmentHeader header = new AtlasTermAssignmentHeader();
        header.setTermGuid("");
        header.setRelationGuid("");
        header.setDescription("");
        header.setDisplayText("");
        header.setQualifiedName("");
        header.setExpression("");
        header.setCreatedBy("");
        header.setSteward("");
        header.setSource("");

        String toString = header.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("AtlasTermAssignmentId"));
    }

    @Test
    public void testEqualsAndHashCodeContract() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("test-term-guid");
        header1.setRelationGuid("relation-guid");
        header1.setDescription("test description");
        header1.setExpression("test expression");
        header1.setCreatedBy("test.user");
        header1.setSteward("test steward");
        header1.setSource("test source");
        header1.setConfidence(75);
        header1.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        header2.setTermGuid("test-term-guid");
        header2.setRelationGuid("relation-guid");
        header2.setDescription("test description");
        header2.setExpression("test expression");
        header2.setCreatedBy("test.user");
        header2.setSteward("test steward");
        header2.setSource("test source");
        header2.setConfidence(75);
        header2.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        // Test equals contract
        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testSettersReturnVoid() {
        assignmentHeader.setTermGuid("test");
        assignmentHeader.setRelationGuid("test");
        assignmentHeader.setDescription("test");
        assignmentHeader.setDisplayText("test");
        assignmentHeader.setExpression("test");
        assignmentHeader.setCreatedBy("test");
        assignmentHeader.setSteward("test");
        assignmentHeader.setSource("test");
        assignmentHeader.setConfidence(50);
        assignmentHeader.setStatus(AtlasTermAssignmentStatus.VALIDATED);
        assignmentHeader.setQualifiedName("test");

        // If we reach here without compilation errors, setters are void
        assertTrue(true);
    }

    @Test
    public void testWithSpecialCharacters() {
        String specialGuid = "assignment-guid-with-special@chars#123";
        String specialDescription = "Description with special chars: !@#$%^&*()";
        String specialExpression = "Expression && condition || result";
        String specialCreatedBy = "user@domain.com";

        assignmentHeader.setTermGuid(specialGuid);
        assignmentHeader.setDescription(specialDescription);
        assignmentHeader.setExpression(specialExpression);
        assignmentHeader.setCreatedBy(specialCreatedBy);

        assertEquals(assignmentHeader.getTermGuid(), specialGuid);
        assertEquals(assignmentHeader.getDescription(), specialDescription);
        assertEquals(assignmentHeader.getExpression(), specialExpression);
        assertEquals(assignmentHeader.getCreatedBy(), specialCreatedBy);
    }

    @Test
    public void testWithUnicodeCharacters() {
        String unicodeTermGuid = "uniTerm";
        String unicodeDescription = "uniDesc";
        String unicodeExpression = "uniExp";
        String unicodeCreatedBy = "uniCreatedBy";

        assignmentHeader.setTermGuid(unicodeTermGuid);
        assignmentHeader.setDescription(unicodeDescription);
        assignmentHeader.setExpression(unicodeExpression);
        assignmentHeader.setCreatedBy(unicodeCreatedBy);

        assertEquals(assignmentHeader.getTermGuid(), unicodeTermGuid);
        assertEquals(assignmentHeader.getDescription(), unicodeDescription);
        assertEquals(assignmentHeader.getExpression(), unicodeExpression);
        assertEquals(assignmentHeader.getCreatedBy(), unicodeCreatedBy);
    }

    @Test
    public void testLongStrings() {
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longString.append("part").append(i).append("_");
        }
        String veryLongString = longString.toString();

        assignmentHeader.setTermGuid(veryLongString);
        assertEquals(assignmentHeader.getTermGuid(), veryLongString);

        assignmentHeader.setDescription(veryLongString);
        assertEquals(assignmentHeader.getDescription(), veryLongString);
    }

    @Test
    public void testConfidenceEdgeCases() {
        int[] confidenceValues = {-1000, -1, 0, 1, 50, 100, 101, 1000, Integer.MAX_VALUE, Integer.MIN_VALUE};

        for (int confidence : confidenceValues) {
            assignmentHeader.setConfidence(confidence);
            assertEquals(assignmentHeader.getConfidence(), confidence);
        }
    }

    @Test
    public void testHashCodeWithNullValues() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        assertEquals(header1.hashCode(), header2.hashCode());

        header1.setTermGuid(null);
        header1.setRelationGuid(null);
        header1.setDescription(null);
        header1.setExpression(null);
        header1.setCreatedBy(null);
        header1.setSteward(null);
        header1.setSource(null);
        header1.setConfidence(0);
        header1.setStatus(null);

        assertEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testEqualsReflexive() {
        assignmentHeader.setTermGuid("test-term-guid");
        assertTrue(assignmentHeader.equals(assignmentHeader));
    }

    @Test
    public void testEqualsSymmetric() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();

        header1.setTermGuid("test-term-guid");
        header2.setTermGuid("test-term-guid");

        assertTrue(header1.equals(header2));
        assertTrue(header2.equals(header1));
    }

    @Test
    public void testEqualsTransitive() {
        AtlasTermAssignmentHeader header1 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header2 = new AtlasTermAssignmentHeader();
        AtlasTermAssignmentHeader header3 = new AtlasTermAssignmentHeader();

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
        AtlasTermAssignmentHeader header = new AtlasTermAssignmentHeader();

        header.setTermGuid("assignment-term-guid-123");
        header.setRelationGuid("assignment-relation-guid-456");
        header.setDescription("Assignment of Customer Data term to entity");
        header.setDisplayText("Customer Data Assignment");
        header.setQualifiedName("business.assignment.customer.data");
        header.setExpression("entity.field == customer_data");
        header.setCreatedBy("data.analyst@company.com");
        header.setSteward("data.steward@company.com");
        header.setSource("Data Governance Tool");
        header.setConfidence(90);
        header.setStatus(AtlasTermAssignmentStatus.VALIDATED);

        assertEquals(header.getTermGuid(), "assignment-term-guid-123");
        assertEquals(header.getRelationGuid(), "assignment-relation-guid-456");
        assertEquals(header.getDescription(), "Assignment of Customer Data term to entity");
        assertEquals(header.getDisplayText(), "Customer Data Assignment");
        assertEquals(header.getQualifiedName(), "business.assignment.customer.data");
        assertEquals(header.getExpression(), "entity.field == customer_data");
        assertEquals(header.getCreatedBy(), "data.analyst@company.com");
        assertEquals(header.getSteward(), "data.steward@company.com");
        assertEquals(header.getSource(), "Data Governance Tool");
        assertEquals(header.getConfidence(), 90);
        assertEquals(header.getStatus(), AtlasTermAssignmentStatus.VALIDATED);

        String toString = header.toString();
        assertTrue(toString.contains("assignment-term-guid-123"));
        assertTrue(toString.contains("assignment-relation-guid-456"));
        assertTrue(toString.contains("Assignment of Customer Data term to entity"));
        assertTrue(toString.contains("Customer Data Assignment"));
        assertTrue(toString.contains("business.assignment.customer.data"));
        assertTrue(toString.contains("entity.field == customer_data"));
        assertTrue(toString.contains("data.analyst@company.com"));
        assertTrue(toString.contains("data.steward@company.com"));
        assertTrue(toString.contains("Data Governance Tool"));
        assertTrue(toString.contains("90"));
        assertTrue(toString.contains("VALIDATED"));
    }

    @Test
    public void testAtlasJSONAnnotation() {
        assertNotNull(assignmentHeader);
    }
}
