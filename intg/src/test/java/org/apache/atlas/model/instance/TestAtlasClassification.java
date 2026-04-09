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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.ModelTestUtil;
import org.apache.atlas.model.TimeBoundary;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.model.instance.AtlasEntity.Status;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasClassification {
    @Test
    public void testClassificationSerDe() throws AtlasBaseException {
        AtlasClassificationDef  classificationDef  = ModelTestUtil.getClassificationDef();
        AtlasTypeRegistry       typeRegistry       = ModelTestUtil.getTypesRegistry();
        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classificationDef.getName());
        assertNotNull(classificationType);

        AtlasClassification ent1 = ModelTestUtil.newClassification(classificationDef, typeRegistry);

        String jsonString = AtlasType.toJson(ent1);

        AtlasClassification ent2 = AtlasType.fromJson(jsonString, AtlasClassification.class);

        classificationType.normalizeAttributeValues(ent2);

        assertEquals(ent2, ent1, "Incorrect serialization/deserialization of AtlasClassification");
    }

    @Test
    public void testClassificationSerDeWithSuperType() throws AtlasBaseException {
        AtlasClassificationDef  classificationDef  = ModelTestUtil.getClassificationDefWithSuperType();
        AtlasTypeRegistry       typeRegistry       = ModelTestUtil.getTypesRegistry();
        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classificationDef.getName());

        assertNotNull(classificationType);

        AtlasClassification ent1 = classificationType.createDefaultValue();

        String jsonString = AtlasType.toJson(ent1);

        AtlasClassification ent2 = AtlasType.fromJson(jsonString, AtlasClassification.class);

        classificationType.normalizeAttributeValues(ent2);

        assertEquals(ent2, ent1, "Incorrect serialization/deserialization of AtlasClassification with superType");
    }

    @Test
    public void testClassificationSerDeWithSuperTypes() throws AtlasBaseException {
        AtlasClassificationDef  classificationDef  = ModelTestUtil.getClassificationDefWithSuperTypes();
        AtlasTypeRegistry       typeRegistry       = ModelTestUtil.getTypesRegistry();
        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classificationDef.getName());

        assertNotNull(classificationType);

        AtlasClassification ent1 = classificationType.createDefaultValue();

        String jsonString = AtlasType.toJson(ent1);

        AtlasClassification ent2 = AtlasType.fromJson(jsonString, AtlasClassification.class);

        classificationType.normalizeAttributeValues(ent2);

        assertEquals(ent2, ent1, "Incorrect serialization/deserialization of AtlasClassification with superTypes");
    }

    @Test
    public void testConstructors() {
        // Test default constructor
        AtlasClassification classification = new AtlasClassification();
        assertNotNull(classification);
        assertNull(classification.getTypeName());

        // Test constructor with typeName
        AtlasClassification classificationWithType = new AtlasClassification("testType");
        assertEquals("testType", classificationWithType.getTypeName());

        // Test constructor with typeName and attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        AtlasClassification classificationWithAttrs = new AtlasClassification("testType", attributes);
        assertEquals("testType", classificationWithAttrs.getTypeName());
        assertEquals(attributes, classificationWithAttrs.getAttributes());

        // Test constructor with typeName, attrName, and attrValue
        AtlasClassification classificationWithSingleAttr = new AtlasClassification("testType", "attr1", "value1");
        assertEquals("testType", classificationWithSingleAttr.getTypeName());
        assertEquals("value1", classificationWithSingleAttr.getAttribute("attr1"));

        // Test constructor with Map
        Map<String, Object> map = new HashMap<>();
        map.put("typeName", "testType");
        map.put("attributes", attributes);
        AtlasClassification classificationFromMap = new AtlasClassification(map);
        assertEquals("testType", classificationFromMap.getTypeName());

        // Test copy constructor
        AtlasClassification originalClassification = new AtlasClassification("testType", "attr1", "value1");
        originalClassification.setEntityGuid("guid123");
        originalClassification.setEntityStatus(Status.ACTIVE);
        originalClassification.setPropagate(true);
        originalClassification.setRemovePropagationsOnEntityDelete(false);

        AtlasClassification copiedClassification = new AtlasClassification(originalClassification);
        assertEquals(originalClassification.getTypeName(), copiedClassification.getTypeName());
        assertEquals(originalClassification.getEntityGuid(), copiedClassification.getEntityGuid());
        assertEquals(originalClassification.getEntityStatus(), copiedClassification.getEntityStatus());
        assertEquals(originalClassification.isPropagate(), copiedClassification.isPropagate());
        assertEquals(originalClassification.getRemovePropagationsOnEntityDelete(), copiedClassification.getRemovePropagationsOnEntityDelete());
    }

    @Test
    public void testGettersAndSetters() {
        AtlasClassification classification = new AtlasClassification();

        // Test entityGuid
        classification.setEntityGuid("testGuid");
        assertEquals("testGuid", classification.getEntityGuid());

        // Test entityStatus
        classification.setEntityStatus(Status.DELETED);
        assertEquals(Status.DELETED, classification.getEntityStatus());

        // Test propagate
        classification.setPropagate(true);
        assertTrue(classification.isPropagate());
        assertTrue(classification.getPropagate());

        classification.setPropagate(false);
        assertFalse(classification.isPropagate());
        assertFalse(classification.getPropagate());

        // Test validityPeriods
        List<TimeBoundary> validityPeriods = new ArrayList<>();
        TimeBoundary timeBoundary = new TimeBoundary();
        timeBoundary.setStartTime("2023/01/01 10:00:00");
        timeBoundary.setEndTime("2023/12/31 10:00:00");
        validityPeriods.add(timeBoundary);

        classification.setValidityPeriods(validityPeriods);
        assertEquals(validityPeriods, classification.getValidityPeriods());

        // Test removePropagationsOnEntityDelete
        classification.setRemovePropagationsOnEntityDelete(true);
        assertTrue(classification.getRemovePropagationsOnEntityDelete());
    }

    @Test
    public void testAddValidityPeriod() {
        AtlasClassification classification = new AtlasClassification();

        // Test adding validity period when list is null
        TimeBoundary timeBoundary1 = new TimeBoundary();
        timeBoundary1.setStartTime("2023/01/01 10:00:00");
        classification.addValityPeriod(timeBoundary1);

        assertNotNull(classification.getValidityPeriods());
        assertEquals(1, classification.getValidityPeriods().size());
        assertEquals(timeBoundary1, classification.getValidityPeriods().get(0));

        // Test adding another validity period
        TimeBoundary timeBoundary2 = new TimeBoundary();
        timeBoundary2.setEndTime("2023/12/31 10:00:00");
        classification.addValityPeriod(timeBoundary2);

        assertEquals(2, classification.getValidityPeriods().size());
        assertEquals(timeBoundary2, classification.getValidityPeriods().get(1));
    }

    @Test
    public void testEqualsAndHashCode() {
        AtlasClassification classification1 = new AtlasClassification("testType");
        classification1.setEntityGuid("guid1");
        classification1.setEntityStatus(Status.ACTIVE);
        classification1.setPropagate(true);
        classification1.setRemovePropagationsOnEntityDelete(false);

        AtlasClassification classification2 = new AtlasClassification("testType");
        classification2.setEntityGuid("guid1");
        classification2.setEntityStatus(Status.ACTIVE);
        classification2.setPropagate(true);
        classification2.setRemovePropagationsOnEntityDelete(false);

        // Test equals
        assertEquals(classification1, classification2);
        assertEquals(classification1.hashCode(), classification2.hashCode());

        // Test self equality
        assertEquals(classification1, classification1);

        // Test null equality
        assertNotEquals(classification1, null);

        // Test different class equality
        assertNotEquals(classification1, "string");

        // Test different entityGuid
        classification2.setEntityGuid("guid2");
        assertNotEquals(classification1, classification2);

        // Reset and test different entityStatus
        classification2.setEntityGuid("guid1");
        classification2.setEntityStatus(Status.DELETED);
        assertNotEquals(classification1, classification2);

        // Reset and test different propagate
        classification2.setEntityStatus(Status.ACTIVE);
        classification2.setPropagate(false);
        assertNotEquals(classification1, classification2);

        // Reset and test different removePropagationsOnEntityDelete
        classification2.setPropagate(true);
        classification2.setRemovePropagationsOnEntityDelete(true);
        assertNotEquals(classification1, classification2);
    }

    @Test
    public void testToString() {
        AtlasClassification classification = new AtlasClassification("testType");
        classification.setEntityGuid("guid123");
        classification.setEntityStatus(Status.ACTIVE);
        classification.setPropagate(true);
        classification.setRemovePropagationsOnEntityDelete(false);

        String toString = classification.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("AtlasClassification"));
        assertTrue(toString.contains("guid123"));
        assertTrue(toString.contains("ACTIVE"));
        assertTrue(toString.contains("true"));
        assertTrue(toString.contains("false"));
    }

    @Test
    public void testAtlasClassificationsNestedClass() {
        // Test default constructor
        AtlasClassification.AtlasClassifications classifications = new AtlasClassification.AtlasClassifications();
        assertNotNull(classifications);

        // Test constructor with list
        List<AtlasClassification> classificationList = new ArrayList<>();
        classificationList.add(new AtlasClassification("type1"));
        classificationList.add(new AtlasClassification("type2"));

        AtlasClassification.AtlasClassifications classificationsWithList = new AtlasClassification.AtlasClassifications(classificationList);
        assertNotNull(classificationsWithList);
        assertEquals(classificationList, classificationsWithList.getList());

        // Test constructor with pagination parameters
        AtlasClassification.AtlasClassifications classificationsWithPagination = new AtlasClassification.AtlasClassifications(classificationList, 0, 10, 2, null, "name");
        assertNotNull(classificationsWithPagination);
        assertEquals(classificationList, classificationsWithPagination.getList());
        assertEquals(0, classificationsWithPagination.getStartIndex());
        assertEquals(10, classificationsWithPagination.getPageSize());
        assertEquals(2, classificationsWithPagination.getTotalCount());
    }

    @Test
    public void testNullValues() {
        AtlasClassification classification = new AtlasClassification();

        // Test setting null values
        classification.setEntityGuid(null);
        assertNull(classification.getEntityGuid());

        classification.setValidityPeriods(null);
        assertNull(classification.getValidityPeriods());

        classification.setPropagate(null);
        assertNull(classification.isPropagate());
        assertNull(classification.getPropagate());

        classification.setRemovePropagationsOnEntityDelete(null);
        assertNull(classification.getRemovePropagationsOnEntityDelete());
    }

    @Test
    public void testCopyConstructorWithNull() {
        AtlasClassification classificationFromNull = new AtlasClassification((AtlasClassification) null);
        assertNotNull(classificationFromNull);
        assertNull(classificationFromNull.getTypeName());
    }

    @Test
    public void testValidityPeriodsInEquals() {
        AtlasClassification classification1 = new AtlasClassification("testType");
        AtlasClassification classification2 = new AtlasClassification("testType");

        List<TimeBoundary> validityPeriods1 = new ArrayList<>();
        TimeBoundary timeBoundary1 = new TimeBoundary();
        timeBoundary1.setStartTime("2023/01/01 10:00:00");
        validityPeriods1.add(timeBoundary1);

        List<TimeBoundary> validityPeriods2 = new ArrayList<>();
        TimeBoundary timeBoundary2 = new TimeBoundary();
        timeBoundary2.setStartTime("2023/06/01 10:00:00");
        validityPeriods2.add(timeBoundary2);

        classification1.setValidityPeriods(validityPeriods1);
        classification2.setValidityPeriods(validityPeriods2);

        assertNotEquals(classification1, classification2);

        classification2.setValidityPeriods(validityPeriods1);
        assertEquals(classification1, classification2);
    }
}
