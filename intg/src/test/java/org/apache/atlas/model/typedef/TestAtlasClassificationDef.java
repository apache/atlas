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
package org.apache.atlas.model.typedef;

import org.apache.atlas.model.ModelTestUtil;
import org.apache.atlas.type.AtlasType;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasClassificationDef {
    @Test
    public void testClassificationDefSerDeEmpty() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef("emptyClassificationDef");

        String jsonString = AtlasType.toJson(classificationDef);

        AtlasClassificationDef classificationDef2 = AtlasType.fromJson(jsonString, AtlasClassificationDef.class);

        assertEquals(classificationDef2, classificationDef, "Incorrect serialization/deserialization of AtlasClassificationDef");
    }

    @Test
    public void testClassificationDefSerDe() {
        AtlasClassificationDef classificationDef = ModelTestUtil.getClassificationDef();

        String jsonString = AtlasType.toJson(classificationDef);

        AtlasClassificationDef classificationDef2 = AtlasType.fromJson(jsonString, AtlasClassificationDef.class);

        assertEquals(classificationDef2, classificationDef, "Incorrect serialization/deserialization of AtlasClassificationDef");
    }

    @Test
    public void testClassificationDefSerDeWithSuperType() {
        AtlasClassificationDef classificationDef = ModelTestUtil.getClassificationDefWithSuperType();

        String jsonString = AtlasType.toJson(classificationDef);

        AtlasClassificationDef classificationDef2 = AtlasType.fromJson(jsonString, AtlasClassificationDef.class);

        assertEquals(classificationDef2, classificationDef, "Incorrect serialization/deserialization of AtlasClassificationDef with superType");
    }

    @Test
    public void testClassificationDefSerDeWithSuperTypes() {
        AtlasClassificationDef classificationDef = ModelTestUtil.getClassificationDefWithSuperTypes();

        String jsonString = AtlasType.toJson(classificationDef);

        AtlasClassificationDef classificationDef2 = AtlasType.fromJson(jsonString, AtlasClassificationDef.class);

        assertEquals(classificationDef2, classificationDef, "Incorrect serialization/deserialization of AtlasClassificationDef with superTypes");
    }

    @Test
    public void testClassificationDefHasSuperTypeWithNoSuperType() {
        AtlasClassificationDef classificationDef = ModelTestUtil.getClassificationDef();

        for (String superType : classificationDef.getSuperTypes()) {
            assertTrue(classificationDef.hasSuperType(superType));
        }

        assertFalse(classificationDef.hasSuperType("01234-xyzabc-;''-)("));
    }

    @Test
    public void testClassificationDefHasSuperTypeWithSuperType() {
        AtlasClassificationDef classificationDef = ModelTestUtil.getClassificationDefWithSuperTypes();

        for (String superType : classificationDef.getSuperTypes()) {
            assertTrue(classificationDef.hasSuperType(superType));
        }

        assertFalse(classificationDef.hasSuperType("01234-xyzabc-;''-)("));
    }

    @Test
    public void testAllConstructors() {
        // Test default constructor
        AtlasClassificationDef def1 = new AtlasClassificationDef();
        assertNotNull(def1);
        assertEquals(def1.getSuperTypes().size(), 0);
        assertEquals(def1.getEntityTypes().size(), 0);

        // Test constructor with name
        AtlasClassificationDef def2 = new AtlasClassificationDef("TestClassification");
        assertEquals(def2.getName(), "TestClassification");

        // Test constructor with name and description
        AtlasClassificationDef def3 = new AtlasClassificationDef("TestClassification", "Test description");
        assertEquals(def3.getName(), "TestClassification");
        assertEquals(def3.getDescription(), "Test description");

        // Test constructor with name, description, and typeVersion
        AtlasClassificationDef def4 = new AtlasClassificationDef("TestClassification", "Test description", "1.0");
        assertEquals(def4.getName(), "TestClassification");
        assertEquals(def4.getDescription(), "Test description");
        assertEquals(def4.getTypeVersion(), "1.0");

        // Test constructor with attributeDefs
        List<AtlasStructDef.AtlasAttributeDef> attrs = Arrays.asList(new AtlasStructDef.AtlasAttributeDef("attr1", "string"));
        AtlasClassificationDef def5 = new AtlasClassificationDef("TestClassification", "Test description", "1.0", attrs);
        assertEquals(def5.getName(), "TestClassification");
        assertEquals(def5.getAttributeDefs().size(), 1);

        // Test constructor with superTypes
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperType1", "SuperType2"));
        AtlasClassificationDef def6 = new AtlasClassificationDef("TestClassification", "Test description", "1.0", attrs, superTypes);
        assertEquals(def6.getName(), "TestClassification");
        assertEquals(def6.getSuperTypes().size(), 2);
        assertTrue(def6.hasSuperType("SuperType1"));
        assertTrue(def6.hasSuperType("SuperType2"));

        // Test constructor with options
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        AtlasClassificationDef def7 = new AtlasClassificationDef("TestClassification", "Test description", "1.0", attrs, superTypes, options);
        assertEquals(def7.getName(), "TestClassification");
        assertEquals(def7.getOptions().get("option1"), "value1");

        // Test constructor with entityTypes
        Set<String> entityTypes = new HashSet<>(Arrays.asList("Entity1", "Entity2"));
        AtlasClassificationDef def8 = new AtlasClassificationDef("TestClassification", "Test description", "1.0", attrs, superTypes, entityTypes, options);
        assertEquals(def8.getName(), "TestClassification");
        assertEquals(def8.getEntityTypes().size(), 2);
        assertTrue(def8.hasEntityType("Entity1"));
        assertTrue(def8.hasEntityType("Entity2"));
    }

    @Test
    public void testCopyConstructor() {
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperType1"));
        Set<String> entityTypes = new HashSet<>(Arrays.asList("Entity1"));
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        List<AtlasStructDef.AtlasAttributeDef> attrs = Arrays.asList(new AtlasStructDef.AtlasAttributeDef("attr1", "string"));

        AtlasClassificationDef original = new AtlasClassificationDef("TestClassification", "Test description", "1.0", attrs, superTypes, entityTypes, options);
        original.setGuid("test-guid");

        AtlasClassificationDef copy = new AtlasClassificationDef(original);

        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getDescription(), original.getDescription());
        assertEquals(copy.getTypeVersion(), original.getTypeVersion());
        assertEquals(copy.getSuperTypes(), original.getSuperTypes());
        assertEquals(copy.getGuid(), original.getGuid());
        assertEquals(copy.getAttributeDefs().size(), original.getAttributeDefs().size());
    }

    @Test
    public void testCopyConstructorWithNull() {
        AtlasClassificationDef copy = new AtlasClassificationDef((String) null);
        assertNotNull(copy);
    }

    @Test
    public void testSuperTypeMethods() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef();

        // Test initial state
        assertEquals(classificationDef.getSuperTypes().size(), 0);

        // Test adding super types
        classificationDef.addSuperType("SuperType1");
        assertTrue(classificationDef.hasSuperType("SuperType1"));
        assertEquals(classificationDef.getSuperTypes().size(), 1);

        // Test adding duplicate super type (should not add again)
        classificationDef.addSuperType("SuperType1");
        assertEquals(classificationDef.getSuperTypes().size(), 1);

        // Test adding another super type
        classificationDef.addSuperType("SuperType2");
        assertTrue(classificationDef.hasSuperType("SuperType2"));
        assertEquals(classificationDef.getSuperTypes().size(), 2);

        // Test removing super type
        classificationDef.removeSuperType("SuperType1");
        assertFalse(classificationDef.hasSuperType("SuperType1"));
        assertTrue(classificationDef.hasSuperType("SuperType2"));
        assertEquals(classificationDef.getSuperTypes().size(), 1);

        // Test removing non-existent super type (should not change anything)
        classificationDef.removeSuperType("NonExistent");
        assertEquals(classificationDef.getSuperTypes().size(), 1);
    }

    @Test
    public void testSetSuperTypes() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef();

        // Test setting super types
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperType1", "SuperType2"));
        classificationDef.setSuperTypes(superTypes);
        assertEquals(classificationDef.getSuperTypes().size(), 2);
        assertTrue(classificationDef.hasSuperType("SuperType1"));
        assertTrue(classificationDef.hasSuperType("SuperType2"));

        // Test setting same reference (should not change)
        classificationDef.setSuperTypes(classificationDef.getSuperTypes());
        assertEquals(classificationDef.getSuperTypes().size(), 2);

        // Test setting null
        classificationDef.setSuperTypes(null);
        assertNotNull(classificationDef.getSuperTypes());
        assertEquals(classificationDef.getSuperTypes().size(), 0);

        // Test setting empty set
        classificationDef.setSuperTypes(Collections.emptySet());
        assertNotNull(classificationDef.getSuperTypes());
        assertEquals(classificationDef.getSuperTypes().size(), 0);
    }

    @Test
    public void testEntityTypeMethods() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef();

        // Test initial state
        assertEquals(classificationDef.getEntityTypes().size(), 0);

        // Test adding entity types
        classificationDef.addEntityType("Entity1");
        assertTrue(classificationDef.hasEntityType("Entity1"));
        assertEquals(classificationDef.getEntityTypes().size(), 1);

        // Test adding duplicate entity type (should not add again)
        classificationDef.addEntityType("Entity1");
        assertEquals(classificationDef.getEntityTypes().size(), 1);

        // Test adding another entity type
        classificationDef.addEntityType("Entity2");
        assertTrue(classificationDef.hasEntityType("Entity2"));
        assertEquals(classificationDef.getEntityTypes().size(), 2);

        // Test removing entity type
        classificationDef.removeEntityType("Entity1");
        assertFalse(classificationDef.hasEntityType("Entity1"));
        assertTrue(classificationDef.hasEntityType("Entity2"));
        assertEquals(classificationDef.getEntityTypes().size(), 1);

        // Test removing non-existent entity type (should not change anything)
        classificationDef.removeEntityType("NonExistent");
        assertEquals(classificationDef.getEntityTypes().size(), 1);
    }

    @Test
    public void testSetEntityTypes() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef();

        // Test setting entity types
        Set<String> entityTypes = new HashSet<>(Arrays.asList("Entity1", "Entity2"));
        classificationDef.setEntityTypes(entityTypes);
        assertEquals(classificationDef.getEntityTypes().size(), 2);
        assertTrue(classificationDef.hasEntityType("Entity1"));
        assertTrue(classificationDef.hasEntityType("Entity2"));

        // Test setting same reference (should not change)
        classificationDef.setEntityTypes(classificationDef.getEntityTypes());
        assertEquals(classificationDef.getEntityTypes().size(), 2);

        // Test setting null
        classificationDef.setEntityTypes(null);
        assertNotNull(classificationDef.getEntityTypes());
        assertEquals(classificationDef.getEntityTypes().size(), 0);

        // Test setting empty set
        classificationDef.setEntityTypes(Collections.emptySet());
        assertNotNull(classificationDef.getEntityTypes());
        assertEquals(classificationDef.getEntityTypes().size(), 0);
    }

    @Test
    public void testSubTypes() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef();

        // Test setting sub types
        Set<String> subTypes = new HashSet<>(Arrays.asList("SubType1", "SubType2"));
        classificationDef.setSubTypes(subTypes);
        assertEquals(classificationDef.getSubTypes(), subTypes);
    }

    @Test
    public void testToString() {
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperType1"));
        Set<String> entityTypes = new HashSet<>(Arrays.asList("Entity1"));
        AtlasClassificationDef classificationDef = new AtlasClassificationDef("TestClassification", "Test description", "1.0", null, superTypes, entityTypes, null);

        String toStringResult = classificationDef.toString();
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("AtlasClassificationDef"));
        assertTrue(toStringResult.contains("TestClassification"));
        assertTrue(toStringResult.contains("SuperType1"));
        assertTrue(toStringResult.contains("Entity1"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef("TestClassification");

        StringBuilder sb = new StringBuilder();
        StringBuilder result = classificationDef.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().contains("TestClassification"));
    }

    @Test
    public void testEquals() {
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperType1"));
        Set<String> entityTypes = new HashSet<>(Arrays.asList("Entity1"));

        AtlasClassificationDef def1 = new AtlasClassificationDef("TestClassification", "description", "1.0", null, superTypes, entityTypes, null);
        AtlasClassificationDef def2 = new AtlasClassificationDef("TestClassification", "description", "1.0", null, superTypes, entityTypes, null);

        assertEquals(def1, def2);
        assertEquals(def1.hashCode(), def2.hashCode());

        // Test self equality
        assertEquals(def1, def1);

        // Test null
        assertNotEquals(def1, null);

        // Test different class
        assertNotEquals(def1, "string");

        // Test different name
        AtlasClassificationDef def3 = new AtlasClassificationDef("DifferentName", "description", "1.0", null, superTypes, entityTypes, null);
        assertNotEquals(def1, def3);

        // Test different super types
        Set<String> differentSuperTypes = new HashSet<>(Arrays.asList("DifferentSuperType"));
        AtlasClassificationDef def4 = new AtlasClassificationDef("TestClassification", "description", "1.0", null, differentSuperTypes, entityTypes, null);
        assertNotEquals(def1, def4);

        // Test different entity types
        Set<String> differentEntityTypes = new HashSet<>(Arrays.asList("DifferentEntity"));
        AtlasClassificationDef def5 = new AtlasClassificationDef("TestClassification", "description", "1.0", null, superTypes, differentEntityTypes, null);
        assertNotEquals(def1, def5);
    }

    @Test
    public void testHashCode() {
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperType1"));
        AtlasClassificationDef def1 = new AtlasClassificationDef("TestClassification", "description", "1.0", null, superTypes);
        AtlasClassificationDef def2 = new AtlasClassificationDef("TestClassification", "description", "1.0", null, superTypes);

        assertEquals(def1.hashCode(), def2.hashCode());
    }

    @Test
    public void testHasSuperTypeWithNullAndEmpty() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef();

        // Test with null super types
        assertFalse(classificationDef.hasSuperType("AnyType"));

        // Test with null type name
        assertFalse(classificationDef.hasSuperType(null));
    }

    @Test
    public void testHasEntityTypeWithNullAndEmpty() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef();

        // Test with null entity types
        assertFalse(classificationDef.hasEntityType("AnyType"));

        // Test with null type name
        assertFalse(classificationDef.hasEntityType(null));
    }

    @Test
    public void testAtlasClassificationDefs() {
        // Test creating AtlasClassificationDefs list
        AtlasClassificationDef def1 = new AtlasClassificationDef("Classification1");
        AtlasClassificationDef def2 = new AtlasClassificationDef("Classification2");

        List<AtlasClassificationDef> classificationList = Arrays.asList(def1, def2);

        // Test default constructor
        AtlasClassificationDef.AtlasClassificationDefs defs1 = new AtlasClassificationDef.AtlasClassificationDefs();
        assertNotNull(defs1);

        // Test constructor with list
        AtlasClassificationDef.AtlasClassificationDefs defs2 = new AtlasClassificationDef.AtlasClassificationDefs(classificationList);
        assertEquals(defs2.getPageSize(), 2);

        // Test constructor with pagination
        AtlasClassificationDef.AtlasClassificationDefs defs3 = new AtlasClassificationDef.AtlasClassificationDefs(
                classificationList, 0, 10, 2, null, null);
        assertEquals(defs3.getPageSize(), 10);
        assertEquals(defs3.getStartIndex(), 0);
        assertEquals(defs3.getPageSize(), 10);
        assertEquals(defs3.getTotalCount(), 2);
    }

    @Test
    public void testComplexScenario() {
        // Create a comprehensive classification definition
        Set<String> superTypes = new HashSet<>(Arrays.asList("DataClassification", "SecurityClassification"));
        Set<String> entityTypes = new HashSet<>(Arrays.asList("hive_table", "hive_column", "hdfs_path"));

        List<AtlasStructDef.AtlasAttributeDef> attributes = Arrays.asList(
                new AtlasStructDef.AtlasAttributeDef("level", "string"),
                new AtlasStructDef.AtlasAttributeDef("category", "string"),
                new AtlasStructDef.AtlasAttributeDef("isActive", "boolean"));

        Map<String, String> options = new HashMap<>();
        options.put("propagate", "true");
        options.put("restrict", "false");

        AtlasClassificationDef piiClassification = new AtlasClassificationDef(
                "PII",
                "Personally Identifiable Information",
                "1.0",
                attributes,
                superTypes,
                entityTypes,
                options);

        // Verify all properties
        assertEquals(piiClassification.getName(), "PII");
        assertEquals(piiClassification.getDescription(), "Personally Identifiable Information");
        assertEquals(piiClassification.getTypeVersion(), "1.0");
        assertEquals(piiClassification.getAttributeDefs().size(), 3);
        assertEquals(piiClassification.getSuperTypes().size(), 2);
        assertEquals(piiClassification.getEntityTypes().size(), 3);

        assertTrue(piiClassification.hasSuperType("DataClassification"));
        assertTrue(piiClassification.hasSuperType("SecurityClassification"));
        assertTrue(piiClassification.hasEntityType("hive_table"));
        assertTrue(piiClassification.hasEntityType("hive_column"));
        assertTrue(piiClassification.hasEntityType("hdfs_path"));

        assertEquals(piiClassification.getOption("propagate"), "true");
        assertEquals(piiClassification.getOption("restrict"), "false");

        // Test serialization and deserialization
        String json = AtlasType.toJson(piiClassification);
        AtlasClassificationDef deserializedDef = AtlasType.fromJson(json, AtlasClassificationDef.class);

        assertEquals(deserializedDef.getName(), piiClassification.getName());
        assertEquals(deserializedDef.getSuperTypes().size(), piiClassification.getSuperTypes().size());
        assertEquals(deserializedDef.getEntityTypes().size(), piiClassification.getEntityTypes().size());
        assertEquals(deserializedDef.getAttributeDefs().size(), piiClassification.getAttributeDefs().size());
    }

    @Test
    public void testInheritedMethods() {
        AtlasClassificationDef classificationDef = new AtlasClassificationDef("TestClassification", "Test description", "1.0");

        // Test inherited attribute methods from AtlasStructDef
        AtlasStructDef.AtlasAttributeDef attr1 = new AtlasStructDef.AtlasAttributeDef("attr1", "string");
        AtlasStructDef.AtlasAttributeDef attr2 = new AtlasStructDef.AtlasAttributeDef("attr2", "int");

        classificationDef.addAttribute(attr1);
        assertTrue(classificationDef.hasAttribute("attr1"));
        assertEquals(classificationDef.getAttribute("attr1").getName(), "attr1");

        classificationDef.addAttribute(attr2);
        assertEquals(classificationDef.getAttributeDefs().size(), 2);

        classificationDef.removeAttribute("attr1");
        assertFalse(classificationDef.hasAttribute("attr1"));
        assertTrue(classificationDef.hasAttribute("attr2"));
    }
}
