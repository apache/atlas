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
package org.apache.atlas.model.instance;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestClassificationAssociateRequest {
    @Test
    public void testConstructors() {
        // Test default constructor
        ClassificationAssociateRequest request = new ClassificationAssociateRequest();
        assertNotNull(request);
        assertNull(request.getEntityGuids());
        assertNull(request.getClassification());
        assertNull(request.getEntityTypeName());
        assertNull(request.getEntitiesUniqueAttributes());

        // Test constructor with entity guids and classification
        List<String> entityGuids = new ArrayList<>();
        entityGuids.add("guid1");
        entityGuids.add("guid2");
        AtlasClassification classification = new AtlasClassification("testClassification");

        ClassificationAssociateRequest requestWithGuids = new ClassificationAssociateRequest(entityGuids, classification);
        assertEquals(entityGuids, requestWithGuids.getEntityGuids());
        assertEquals(classification, requestWithGuids.getClassification());
        assertNull(requestWithGuids.getEntityTypeName());
        assertNull(requestWithGuids.getEntitiesUniqueAttributes());

        // Test constructor with entity type, unique attributes and classification
        String entityTypeName = "testEntityType";
        List<Map<String, Object>> entitiesUniqueAttributes = new ArrayList<>();
        Map<String, Object> uniqueAttributes1 = new HashMap<>();
        uniqueAttributes1.put("name", "entity1");
        Map<String, Object> uniqueAttributes2 = new HashMap<>();
        uniqueAttributes2.put("name", "entity2");
        entitiesUniqueAttributes.add(uniqueAttributes1);
        entitiesUniqueAttributes.add(uniqueAttributes2);

        ClassificationAssociateRequest requestWithUniqueAttrs = new ClassificationAssociateRequest(
                entityTypeName, entitiesUniqueAttributes, classification);
        assertEquals(entityTypeName, requestWithUniqueAttrs.getEntityTypeName());
        assertEquals(entitiesUniqueAttributes, requestWithUniqueAttrs.getEntitiesUniqueAttributes());
        assertEquals(classification, requestWithUniqueAttrs.getClassification());
        assertNull(requestWithUniqueAttrs.getEntityGuids());

        // Test constructor with all parameters
        ClassificationAssociateRequest requestWithAll = new ClassificationAssociateRequest(
                entityGuids, entityTypeName, entitiesUniqueAttributes, classification);
        assertEquals(entityGuids, requestWithAll.getEntityGuids());
        assertEquals(entityTypeName, requestWithAll.getEntityTypeName());
        assertEquals(entitiesUniqueAttributes, requestWithAll.getEntitiesUniqueAttributes());
        assertEquals(classification, requestWithAll.getClassification());
    }

    @Test
    public void testGettersAndSetters() {
        ClassificationAssociateRequest request = new ClassificationAssociateRequest();

        // Test entity type name
        request.setEntityTypeName("testType");
        assertEquals("testType", request.getEntityTypeName());

        // Test entity guids
        List<String> entityGuids = new ArrayList<>();
        entityGuids.add("guid1");
        entityGuids.add("guid2");
        request.setEntityGuids(entityGuids);
        assertEquals(entityGuids, request.getEntityGuids());

        // Test classification
        AtlasClassification classification = new AtlasClassification("testClassification");
        classification.setAttribute("attr1", "value1");
        request.setClassification(classification);
        assertEquals(classification, request.getClassification());

        // Test entities unique attributes
        List<Map<String, Object>> entitiesUniqueAttributes = new ArrayList<>();
        Map<String, Object> uniqueAttributes = new HashMap<>();
        uniqueAttributes.put("name", "entity1");
        uniqueAttributes.put("qualifiedName", "entity1@cluster");
        entitiesUniqueAttributes.add(uniqueAttributes);
        request.setEntitiesUniqueAttributes(entitiesUniqueAttributes);
        assertEquals(entitiesUniqueAttributes, request.getEntitiesUniqueAttributes());
    }

    @Test
    public void testEquals() {
        AtlasClassification classification = new AtlasClassification("testClassification");
        List<String> entityGuids = new ArrayList<>();
        entityGuids.add("guid1");
        entityGuids.add("guid2");

        List<Map<String, Object>> entitiesUniqueAttributes = new ArrayList<>();
        Map<String, Object> uniqueAttributes = new HashMap<>();
        uniqueAttributes.put("name", "entity1");
        entitiesUniqueAttributes.add(uniqueAttributes);

        ClassificationAssociateRequest request1 = new ClassificationAssociateRequest();
        request1.setClassification(classification);
        request1.setEntityGuids(entityGuids);
        request1.setEntitiesUniqueAttributes(entitiesUniqueAttributes);

        ClassificationAssociateRequest request2 = new ClassificationAssociateRequest();
        request2.setClassification(classification);
        request2.setEntityGuids(entityGuids);
        request2.setEntitiesUniqueAttributes(entitiesUniqueAttributes);

        // Test equals
        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());

        // Test self equality
        assertEquals(request1, request1);

        // Test null equality
        assertNotEquals(request1, null);

        // Test different class equality
        assertNotEquals(request1, "string");

        // Test different classification
        AtlasClassification differentClassification = new AtlasClassification("differentClassification");
        request2.setClassification(differentClassification);
        assertNotEquals(request1, request2);

        // Reset and test different entity guids
        request2.setClassification(classification);
        List<String> differentEntityGuids = new ArrayList<>();
        differentEntityGuids.add("guid3");
        request2.setEntityGuids(differentEntityGuids);
        assertNotEquals(request1, request2);

        // Reset and test different unique attributes
        request2.setEntityGuids(entityGuids);
        List<Map<String, Object>> differentUniqueAttributes = new ArrayList<>();
        Map<String, Object> differentAttributes = new HashMap<>();
        differentAttributes.put("name", "entity2");
        differentUniqueAttributes.add(differentAttributes);
        request2.setEntitiesUniqueAttributes(differentUniqueAttributes);
        assertNotEquals(request1, request2);
    }

    @Test
    public void testHashCode() {
        AtlasClassification classification = new AtlasClassification("testClassification");
        List<String> entityGuids = new ArrayList<>();
        entityGuids.add("guid1");

        ClassificationAssociateRequest request1 = new ClassificationAssociateRequest();
        request1.setClassification(classification);
        request1.setEntityGuids(entityGuids);

        ClassificationAssociateRequest request2 = new ClassificationAssociateRequest();
        request2.setClassification(classification);
        request2.setEntityGuids(entityGuids);

        assertEquals(request1.hashCode(), request2.hashCode());

        // Change a property and verify hashCode changes
        request2.setClassification(new AtlasClassification("differentClassification"));
        assertNotEquals(request1.hashCode(), request2.hashCode());
    }

    @Test
    public void testToString() {
        ClassificationAssociateRequest request = new ClassificationAssociateRequest();

        // Test toString with null values
        String toStringNull = request.toString();
        assertNotNull(toStringNull);
        assertTrue(toStringNull.contains("ClassificationAssociateRequest"));

        // Test toString with populated data
        AtlasClassification classification = new AtlasClassification("testClassification");
        classification.setAttribute("attr1", "value1");

        List<String> entityGuids = new ArrayList<>();
        entityGuids.add("guid1");
        entityGuids.add("guid2");

        String entityTypeName = "testEntityType";

        List<Map<String, Object>> entitiesUniqueAttributes = new ArrayList<>();
        Map<String, Object> uniqueAttributes = new HashMap<>();
        uniqueAttributes.put("name", "entity1");
        uniqueAttributes.put("qualifiedName", "entity1@cluster");
        entitiesUniqueAttributes.add(uniqueAttributes);

        request.setClassification(classification);
        request.setEntityGuids(entityGuids);
        request.setEntityTypeName(entityTypeName);
        request.setEntitiesUniqueAttributes(entitiesUniqueAttributes);

        String toStringWithData = request.toString();
        assertNotNull(toStringWithData);
        assertTrue(toStringWithData.contains("ClassificationAssociateRequest"));
        assertTrue(toStringWithData.contains("testClassification"));
        assertTrue(toStringWithData.contains("guid1"));
        assertTrue(toStringWithData.contains("testEntityType"));
        assertTrue(toStringWithData.contains("entity1"));

        // Test toString with StringBuilder parameter
        StringBuilder sb = request.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("ClassificationAssociateRequest"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        request.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
    }

    @Test
    public void testNullHandling() {
        ClassificationAssociateRequest request = new ClassificationAssociateRequest();

        // Test setting null values
        request.setClassification(null);
        assertNull(request.getClassification());

        request.setEntityGuids(null);
        assertNull(request.getEntityGuids());

        request.setEntityTypeName(null);
        assertNull(request.getEntityTypeName());

        request.setEntitiesUniqueAttributes(null);
        assertNull(request.getEntitiesUniqueAttributes());

        // Test toString with null values
        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("ClassificationAssociateRequest"));
    }

    @Test
    public void testEmptyCollections() {
        ClassificationAssociateRequest request = new ClassificationAssociateRequest();

        // Test with empty entity guids
        List<String> emptyEntityGuids = new ArrayList<>();
        request.setEntityGuids(emptyEntityGuids);
        assertEquals(emptyEntityGuids, request.getEntityGuids());
        assertTrue(request.getEntityGuids().isEmpty());

        // Test with empty unique attributes
        List<Map<String, Object>> emptyUniqueAttributes = new ArrayList<>();
        request.setEntitiesUniqueAttributes(emptyUniqueAttributes);
        assertEquals(emptyUniqueAttributes, request.getEntitiesUniqueAttributes());
        assertTrue(request.getEntitiesUniqueAttributes().isEmpty());

        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("ClassificationAssociateRequest"));
    }

    @Test
    public void testComplexClassification() {
        ClassificationAssociateRequest request = new ClassificationAssociateRequest();
        AtlasClassification complexClassification = new AtlasClassification("ComplexClassification");
        complexClassification.setAttribute("level", "HIGH");
        complexClassification.setAttribute("category", "SENSITIVE");
        complexClassification.setAttribute("expiration", "2024-12-31");
        complexClassification.setPropagate(true);
        complexClassification.setEntityGuid("sourceEntityGuid");

        request.setClassification(complexClassification);
        assertEquals(complexClassification, request.getClassification());

        String toString = request.toString();
        assertTrue(toString.contains("ComplexClassification"));
    }

    @Test
    public void testEqualityWithNullValues() {
        ClassificationAssociateRequest request1 = new ClassificationAssociateRequest();
        ClassificationAssociateRequest request2 = new ClassificationAssociateRequest();

        // Set empty lists to avoid NPE in CollectionUtils.isEqualCollection
        request1.setEntitiesUniqueAttributes(new ArrayList<>());
        request2.setEntitiesUniqueAttributes(new ArrayList<>());

        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());

        // Test equality when one has null and other has values
        request1.setEntityGuids(new ArrayList<>());
        assertNotEquals(request1, request2);

        // Test equality when both have same non-null values
        request2.setEntityGuids(new ArrayList<>());
        assertEquals(request1, request2);
    }

    @Test
    public void testRealWorldScenario() {
        AtlasClassification piiClassification = new AtlasClassification("PII");
        piiClassification.setAttribute("level", "HIGH");
        piiClassification.setAttribute("type", "PERSONAL_DATA");
        piiClassification.setPropagate(false);

        // Some entities identified by GUIDs
        List<String> entityGuids = new ArrayList<>();
        entityGuids.add("table-guid-1");
        entityGuids.add("column-guid-1");

        // Some entities identified by unique attributes
        List<Map<String, Object>> entitiesUniqueAttributes = new ArrayList<>();

        Map<String, Object> tableUniqueAttrs = new HashMap<>();
        tableUniqueAttrs.put("qualifiedName", "customer_table@prod_cluster");
        entitiesUniqueAttributes.add(tableUniqueAttrs);

        Map<String, Object> columnUniqueAttrs = new HashMap<>();
        columnUniqueAttrs.put("qualifiedName", "customer_table.email@prod_cluster");
        entitiesUniqueAttributes.add(columnUniqueAttrs);

        ClassificationAssociateRequest request = new ClassificationAssociateRequest(
                entityGuids, "Table", entitiesUniqueAttributes, piiClassification);

        // Verify all fields are set correctly
        assertEquals(entityGuids, request.getEntityGuids());
        assertEquals("Table", request.getEntityTypeName());
        assertEquals(entitiesUniqueAttributes, request.getEntitiesUniqueAttributes());
        assertEquals(piiClassification, request.getClassification());

        // Verify toString contains relevant information
        String toString = request.toString();
        assertTrue(toString.contains("PII"));
        assertTrue(toString.contains("table-guid-1"));
        assertTrue(toString.contains("Table"));
        assertTrue(toString.contains("customer_table@prod_cluster"));
    }
}
