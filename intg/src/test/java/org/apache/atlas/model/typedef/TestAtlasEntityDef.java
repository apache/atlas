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

public class TestAtlasEntityDef {
    @Test
    public void testEntityDefSerDeEmpty() {
        AtlasEntityDef entityDef = new AtlasEntityDef("emptyEntityDef");

        String jsonString = AtlasType.toJson(entityDef);

        AtlasEntityDef entityDef2 = AtlasType.fromJson(jsonString, AtlasEntityDef.class);

        assertEquals(entityDef2, entityDef, "Incorrect serialization/deserialization of AtlasEntityDef");
    }

    @Test
    public void testEntityDefSerDe() {
        AtlasEntityDef entityDef = ModelTestUtil.getEntityDef();

        String jsonString = AtlasType.toJson(entityDef);

        AtlasEntityDef entityDef2 = AtlasType.fromJson(jsonString, AtlasEntityDef.class);

        assertEquals(entityDef2, entityDef, "Incorrect serialization/deserialization of AtlasEntityDef");
    }

    @Test
    public void testEntityDefSerDeWithSuperType() {
        AtlasEntityDef entityDef = ModelTestUtil.getEntityDefWithSuperType();

        String jsonString = AtlasType.toJson(entityDef);

        AtlasEntityDef entityDef2 = AtlasType.fromJson(jsonString, AtlasEntityDef.class);

        assertEquals(entityDef2, entityDef, "Incorrect serialization/deserialization of AtlasEntityDef with superType");
    }

    @Test
    public void testEntityDefSerDeWithSuperTypes() {
        AtlasEntityDef entityDef = ModelTestUtil.getEntityDefWithSuperTypes();

        String jsonString = AtlasType.toJson(entityDef);

        AtlasEntityDef entityDef2 = AtlasType.fromJson(jsonString, AtlasEntityDef.class);

        assertEquals(entityDef2, entityDef, "Incorrect serialization/deserialization of AtlasEntityDef with superTypes");
    }

    @Test
    public void testEntityDefAddSuperType() {
        AtlasEntityDef entityDef = ModelTestUtil.newEntityDef();

        String newSuperType = "newType-abcd-1234";

        entityDef.addSuperType(newSuperType);

        assertTrue(entityDef.hasSuperType(newSuperType));

        entityDef.removeSuperType(newSuperType);
    }

    @Test
    public void testEntityDefRemoveElement() {
        AtlasEntityDef entityDef = ModelTestUtil.newEntityDefWithSuperTypes();

        for (String superType : entityDef.getSuperTypes()) {
            entityDef.removeSuperType(superType);
            assertFalse(entityDef.hasSuperType(superType));
        }
    }

    @Test
    public void testEntityDefSetSuperTypes() {
        AtlasEntityDef entityDef = ModelTestUtil.newEntityDefWithSuperTypes();

        Set<String> oldSuperTypes = entityDef.getSuperTypes();
        Set<String> newSuperTypes = new HashSet<>();

        newSuperTypes.add("newType-abcd-1234");

        entityDef.setSuperTypes(newSuperTypes);

        for (String superType : oldSuperTypes) {
            assertFalse(entityDef.hasSuperType(superType));
        }

        for (String superType : newSuperTypes) {
            assertTrue(entityDef.hasSuperType(superType));
        }

        // restore old sypertypes
        entityDef.setSuperTypes(oldSuperTypes);
    }

    @Test
    public void testEntityDefHasSuperTypeWithNoSuperType() {
        AtlasEntityDef entityDef = ModelTestUtil.getEntityDef();

        for (String superType : entityDef.getSuperTypes()) {
            assertTrue(entityDef.hasSuperType(superType));
        }

        assertFalse(entityDef.hasSuperType("01234-xyzabc-;''-)("));
    }

    @Test
    public void testEntityDefHasSuperTypeWithNoSuperTypes() {
        AtlasEntityDef entityDef = ModelTestUtil.getEntityDefWithSuperTypes();

        for (String superType : entityDef.getSuperTypes()) {
            assertTrue(entityDef.hasSuperType(superType));
        }

        assertFalse(entityDef.hasSuperType("01234-xyzabc-;''-)("));
    }

    @Test
    public void testAllConstructors() {
        // Test default constructor
        AtlasEntityDef def1 = new AtlasEntityDef();
        assertNotNull(def1);
        assertEquals(def1.getSuperTypes().size(), 0);

        // Test constructor with name
        AtlasEntityDef def2 = new AtlasEntityDef("TestEntity");
        assertEquals(def2.getName(), "TestEntity");

        // Test constructor with name and description
        AtlasEntityDef def3 = new AtlasEntityDef("TestEntity", "Test description");
        assertEquals(def3.getName(), "TestEntity");
        assertEquals(def3.getDescription(), "Test description");

        // Test constructor with name, description, and typeVersion
        AtlasEntityDef def4 = new AtlasEntityDef("TestEntity", "Test description", "1.0");
        assertEquals(def4.getName(), "TestEntity");
        assertEquals(def4.getDescription(), "Test description");
        assertEquals(def4.getTypeVersion(), "1.0");

        // Test constructor with serviceType
        AtlasEntityDef def5 = new AtlasEntityDef("TestEntity", "Test description", "1.0", "hive");
        assertEquals(def5.getName(), "TestEntity");
        assertEquals(def5.getServiceType(), "hive");

        // Test constructor with attributeDefs
        List<AtlasStructDef.AtlasAttributeDef> attrs = Arrays.asList(new AtlasStructDef.AtlasAttributeDef("attr1", "string"));
        AtlasEntityDef def6 = new AtlasEntityDef("TestEntity", "Test description", "1.0", attrs);
        assertEquals(def6.getName(), "TestEntity");
        assertEquals(def6.getAttributeDefs().size(), 1);

        // Test constructor with serviceType and attributeDefs
        AtlasEntityDef def7 = new AtlasEntityDef("TestEntity", "Test description", "1.0", "hdfs", attrs);
        assertEquals(def7.getName(), "TestEntity");
        assertEquals(def7.getServiceType(), "hdfs");
        assertEquals(def7.getAttributeDefs().size(), 1);

        // Test constructor with superTypes
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperEntity"));
        AtlasEntityDef def8 = new AtlasEntityDef("TestEntity", "Test description", "1.0", attrs, superTypes);
        assertEquals(def8.getName(), "TestEntity");
        assertTrue(def8.hasSuperType("SuperEntity"));

        // Test constructor with serviceType, attributeDefs, and superTypes
        AtlasEntityDef def9 = new AtlasEntityDef("TestEntity", "Test description", "1.0", "kafka", attrs, superTypes);
        assertEquals(def9.getName(), "TestEntity");
        assertEquals(def9.getServiceType(), "kafka");
        assertTrue(def9.hasSuperType("SuperEntity"));

        // Test constructor with options
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        AtlasEntityDef def10 = new AtlasEntityDef("TestEntity", "Test description", "1.0", attrs, superTypes, options);
        assertEquals(def10.getName(), "TestEntity");
        assertEquals(def10.getOptions().get("option1"), "value1");

        // Test constructor with all parameters
        AtlasEntityDef def11 = new AtlasEntityDef("TestEntity", "Test description", "1.0", "spark", attrs, superTypes, options);
        assertEquals(def11.getName(), "TestEntity");
        assertEquals(def11.getServiceType(), "spark");
        assertTrue(def11.hasSuperType("SuperEntity"));
        assertEquals(def11.getOptions().get("option1"), "value1");
    }

    @Test
    public void testCopyConstructor() {
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperEntity"));
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        List<AtlasStructDef.AtlasAttributeDef> attrs = Arrays.asList(new AtlasStructDef.AtlasAttributeDef("attr1", "string"));

        AtlasEntityDef original = new AtlasEntityDef("TestEntity", "Test description", "1.0", "hive", attrs, superTypes, options);
        original.setGuid("test-guid");

        // Set additional fields
        Set<String> subTypes = new HashSet<>(Arrays.asList("SubEntity"));
        original.setSubTypes(subTypes);

        List<AtlasEntityDef.AtlasRelationshipAttributeDef> relationshipAttrs = Arrays.asList(
                new AtlasEntityDef.AtlasRelationshipAttributeDef("relType", false, new AtlasStructDef.AtlasAttributeDef("relAttr", "string")));
        original.setRelationshipAttributeDefs(relationshipAttrs);

        Map<String, List<AtlasStructDef.AtlasAttributeDef>> businessAttrs = new HashMap<>();
        businessAttrs.put("namespace1", Arrays.asList(new AtlasStructDef.AtlasAttributeDef("bizAttr", "int")));
        original.setBusinessAttributeDefs(businessAttrs);

        AtlasEntityDef copy = new AtlasEntityDef(original);

        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getDescription(), original.getDescription());
        assertEquals(copy.getTypeVersion(), original.getTypeVersion());
        assertEquals(copy.getServiceType(), original.getServiceType());
        assertEquals(copy.getSuperTypes(), original.getSuperTypes());
        assertEquals(copy.getSubTypes(), original.getSubTypes());
        assertEquals(copy.getRelationshipAttributeDefs(), original.getRelationshipAttributeDefs());
        assertEquals(copy.getBusinessAttributeDefs(), original.getBusinessAttributeDefs());
        assertEquals(copy.getGuid(), original.getGuid());
    }

    @Test
    public void testCopyConstructorWithNull() {
        AtlasEntityDef copy = new AtlasEntityDef((String) null);
        assertNotNull(copy);
    }

    @Test
    public void testSuperTypeMethods() {
        AtlasEntityDef entityDef = new AtlasEntityDef();

        // Test initial state
        assertEquals(entityDef.getSuperTypes().size(), 0);

        // Test adding super types
        entityDef.addSuperType("SuperEntity1");
        assertTrue(entityDef.hasSuperType("SuperEntity1"));
        assertEquals(entityDef.getSuperTypes().size(), 1);

        // Test adding duplicate super type (should not add again)
        entityDef.addSuperType("SuperEntity1");
        assertEquals(entityDef.getSuperTypes().size(), 1);

        // Test adding another super type
        entityDef.addSuperType("SuperEntity2");
        assertTrue(entityDef.hasSuperType("SuperEntity2"));
        assertEquals(entityDef.getSuperTypes().size(), 2);

        // Test removing super type
        entityDef.removeSuperType("SuperEntity1");
        assertFalse(entityDef.hasSuperType("SuperEntity1"));
        assertTrue(entityDef.hasSuperType("SuperEntity2"));
        assertEquals(entityDef.getSuperTypes().size(), 1);

        // Test removing non-existent super type (should not change anything)
        entityDef.removeSuperType("NonExistent");
        assertEquals(entityDef.getSuperTypes().size(), 1);
    }

    @Test
    public void testSetSuperTypes() {
        AtlasEntityDef entityDef = new AtlasEntityDef();

        // Test setting super types
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperEntity1", "SuperEntity2"));
        entityDef.setSuperTypes(superTypes);
        assertEquals(entityDef.getSuperTypes().size(), 2);
        assertTrue(entityDef.hasSuperType("SuperEntity1"));
        assertTrue(entityDef.hasSuperType("SuperEntity2"));

        // Test setting same reference (should not change)
        entityDef.setSuperTypes(entityDef.getSuperTypes());
        assertEquals(entityDef.getSuperTypes().size(), 2);

        // Test setting null
        entityDef.setSuperTypes(null);
        assertNotNull(entityDef.getSuperTypes());
        assertEquals(entityDef.getSuperTypes().size(), 0);

        // Test setting empty set
        entityDef.setSuperTypes(Collections.emptySet());
        assertNotNull(entityDef.getSuperTypes());
        assertEquals(entityDef.getSuperTypes().size(), 0);
    }

    @Test
    public void testSubTypes() {
        AtlasEntityDef entityDef = new AtlasEntityDef();

        // Test setting sub types
        Set<String> subTypes = new HashSet<>(Arrays.asList("SubEntity1", "SubEntity2"));
        entityDef.setSubTypes(subTypes);
        assertEquals(entityDef.getSubTypes(), subTypes);
    }

    @Test
    public void testRelationshipAttributeDefs() {
        AtlasEntityDef entityDef = new AtlasEntityDef();

        // Test setting relationship attribute defs
        AtlasEntityDef.AtlasRelationshipAttributeDef relAttr = new AtlasEntityDef.AtlasRelationshipAttributeDef(
                "testRelType", false, new AtlasStructDef.AtlasAttributeDef("relAttr", "string"));
        List<AtlasEntityDef.AtlasRelationshipAttributeDef> relationshipAttrs = Arrays.asList(relAttr);
        entityDef.setRelationshipAttributeDefs(relationshipAttrs);
        assertEquals(entityDef.getRelationshipAttributeDefs(), relationshipAttrs);
    }

    @Test
    public void testBusinessAttributeDefs() {
        AtlasEntityDef entityDef = new AtlasEntityDef();

        // Test setting business attribute defs
        Map<String, List<AtlasStructDef.AtlasAttributeDef>> businessAttrs = new HashMap<>();
        businessAttrs.put("namespace1", Arrays.asList(new AtlasStructDef.AtlasAttributeDef("bizAttr1", "string")));
        businessAttrs.put("namespace2", Arrays.asList(new AtlasStructDef.AtlasAttributeDef("bizAttr2", "int")));

        entityDef.setBusinessAttributeDefs(businessAttrs);
        assertEquals(entityDef.getBusinessAttributeDefs(), businessAttrs);
        assertEquals(entityDef.getBusinessAttributeDefs().size(), 2);
    }

    @Test
    public void testDisplayTextAttributeOption() {
        AtlasEntityDef entityDef = new AtlasEntityDef();
        // Test setting display text attribute option
        entityDef.setOption(AtlasEntityDef.OPTION_DISPLAY_TEXT_ATTRIBUTE, "displayName");
        assertEquals(entityDef.getOption(AtlasEntityDef.OPTION_DISPLAY_TEXT_ATTRIBUTE), "displayName");
    }

    @Test
    public void testToString() {
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperEntity"));
        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "Test description", "1.0", null, superTypes);

        String toStringResult = entityDef.toString();
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("AtlasEntityDef"));
        assertTrue(toStringResult.contains("TestEntity"));
        assertTrue(toStringResult.contains("SuperEntity"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity");

        StringBuilder sb = new StringBuilder();
        StringBuilder result = entityDef.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().contains("TestEntity"));
    }

    @Test
    public void testEquals() {
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperEntity"));
        AtlasEntityDef def1 = new AtlasEntityDef("TestEntity", "description", "1.0", null, superTypes);
        AtlasEntityDef def2 = new AtlasEntityDef("TestEntity", "description", "1.0", null, superTypes);

        assertEquals(def1, def2);
        assertEquals(def1.hashCode(), def2.hashCode());

        // Test self equality
        assertEquals(def1, def1);

        // Test null
        assertNotEquals(def1, null);

        // Test different class
        assertNotEquals(def1, "string");

        // Test different name
        AtlasEntityDef def3 = new AtlasEntityDef("DifferentEntity", "description", "1.0", null, superTypes);
        assertNotEquals(def1, def3);

        // Test different super types
        Set<String> differentSuperTypes = new HashSet<>(Arrays.asList("DifferentSuperEntity"));
        AtlasEntityDef def4 = new AtlasEntityDef("TestEntity", "description", "1.0", null, differentSuperTypes);
        assertNotEquals(def1, def4);
    }

    @Test
    public void testHashCode() {
        Set<String> superTypes = new HashSet<>(Arrays.asList("SuperEntity"));
        AtlasEntityDef def1 = new AtlasEntityDef("TestEntity", "description", "1.0", null, superTypes);
        AtlasEntityDef def2 = new AtlasEntityDef("TestEntity", "description", "1.0", null, superTypes);

        assertEquals(def1.hashCode(), def2.hashCode());
    }

    @Test
    public void testAtlasRelationshipAttributeDef() {
        // Test default constructor
        AtlasEntityDef.AtlasRelationshipAttributeDef relAttr1 = new AtlasEntityDef.AtlasRelationshipAttributeDef();
        assertNotNull(relAttr1);

        // Test constructor with parameters
        AtlasStructDef.AtlasAttributeDef baseAttr = new AtlasStructDef.AtlasAttributeDef("testAttr", "string");
        AtlasEntityDef.AtlasRelationshipAttributeDef relAttr2 = new AtlasEntityDef.AtlasRelationshipAttributeDef(
                "testRelationship", true, baseAttr);

        assertEquals(relAttr2.getRelationshipTypeName(), "testRelationship");
        assertTrue(relAttr2.getIsLegacyAttribute());
        assertEquals(relAttr2.getName(), "testAttr");
        assertEquals(relAttr2.getTypeName(), "string");

        // Test setters
        relAttr2.setRelationshipTypeName("newRelationship");
        assertEquals(relAttr2.getRelationshipTypeName(), "newRelationship");

        relAttr2.setIsLegacyAttribute(false);
        assertFalse(relAttr2.getIsLegacyAttribute());

        // Test toString
        String toStringResult = relAttr2.toString();
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("AtlasRelationshipAttributeDef"));
        assertTrue(toStringResult.contains("newRelationship"));

        // Test toString with StringBuilder
        StringBuilder sb = new StringBuilder();
        StringBuilder result = relAttr2.toString(sb);
        assertNotNull(result);
        assertTrue(result.toString().contains("newRelationship"));

        // Test equals
        AtlasEntityDef.AtlasRelationshipAttributeDef relAttr3 = new AtlasEntityDef.AtlasRelationshipAttributeDef(
                "newRelationship", false, baseAttr);
        assertEquals(relAttr2, relAttr3);
        assertEquals(relAttr2.hashCode(), relAttr3.hashCode());

        // Test inequality
        AtlasEntityDef.AtlasRelationshipAttributeDef relAttr4 = new AtlasEntityDef.AtlasRelationshipAttributeDef(
                "differentRelationship", false, baseAttr);
        assertNotEquals(relAttr2, relAttr4);
    }

    @Test
    public void testAtlasEntityDefs() {
        // Test creating AtlasEntityDefs list
        AtlasEntityDef def1 = new AtlasEntityDef("Entity1");
        AtlasEntityDef def2 = new AtlasEntityDef("Entity2");

        List<AtlasEntityDef> entityList = Arrays.asList(def1, def2);

        // Test default constructor
        AtlasEntityDef.AtlasEntityDefs defs1 = new AtlasEntityDef.AtlasEntityDefs();
        assertNotNull(defs1);

        // Test constructor with list
        AtlasEntityDef.AtlasEntityDefs defs2 = new AtlasEntityDef.AtlasEntityDefs(entityList);
        assertEquals(defs2.getPageSize(), 2);

        // Test constructor with pagination
        AtlasEntityDef.AtlasEntityDefs defs3 = new AtlasEntityDef.AtlasEntityDefs(
                entityList, 0, 10, 2, null, null);
        assertEquals(defs3.getPageSize(), 10);
        assertEquals(defs3.getStartIndex(), 0);
        assertEquals(defs3.getPageSize(), 10);
        assertEquals(defs3.getTotalCount(), 2);
    }

    @Test
    public void testComplexScenario() {
        // Create a comprehensive entity definition
        Set<String> superTypes = new HashSet<>(Arrays.asList("DataSet", "Asset"));
        Map<String, String> options = new HashMap<>();
        options.put(AtlasEntityDef.OPTION_DISPLAY_TEXT_ATTRIBUTE, "name");
        options.put("schemaCompatible", "true");

        List<AtlasStructDef.AtlasAttributeDef> attributes = Arrays.asList(
                new AtlasStructDef.AtlasAttributeDef("name", "string"),
                new AtlasStructDef.AtlasAttributeDef("location", "string"),
                new AtlasStructDef.AtlasAttributeDef("createTime", "date"));

        AtlasEntityDef hiveTableDef = new AtlasEntityDef(
                "hive_table",
                "Hive Table Entity",
                "1.0",
                "hive",
                attributes,
                superTypes,
                options);

        // Set additional properties
        Set<String> subTypes = new HashSet<>(Arrays.asList("hive_external_table", "hive_managed_table"));
        hiveTableDef.setSubTypes(subTypes);

        List<AtlasEntityDef.AtlasRelationshipAttributeDef> relationshipAttrs = Arrays.asList(
                new AtlasEntityDef.AtlasRelationshipAttributeDef("table_db", false,
                        new AtlasStructDef.AtlasAttributeDef("db", "hive_db")),
                new AtlasEntityDef.AtlasRelationshipAttributeDef("table_columns", false,
                        new AtlasStructDef.AtlasAttributeDef("columns", "array<hive_column>")));
        hiveTableDef.setRelationshipAttributeDefs(relationshipAttrs);

        Map<String, List<AtlasStructDef.AtlasAttributeDef>> businessAttrs = new HashMap<>();
        businessAttrs.put("DataQuality", Arrays.asList(
                new AtlasStructDef.AtlasAttributeDef("score", "int"),
                new AtlasStructDef.AtlasAttributeDef("validated", "boolean")));
        hiveTableDef.setBusinessAttributeDefs(businessAttrs);

        // Verify all properties
        assertEquals(hiveTableDef.getName(), "hive_table");
        assertEquals(hiveTableDef.getDescription(), "Hive Table Entity");
        assertEquals(hiveTableDef.getTypeVersion(), "1.0");
        assertEquals(hiveTableDef.getServiceType(), "hive");
        assertEquals(hiveTableDef.getAttributeDefs().size(), 3);
        assertEquals(hiveTableDef.getSuperTypes().size(), 2);
        assertEquals(hiveTableDef.getSubTypes().size(), 2);
        assertEquals(hiveTableDef.getRelationshipAttributeDefs().size(), 2);
        assertEquals(hiveTableDef.getBusinessAttributeDefs().size(), 1);

        assertTrue(hiveTableDef.hasSuperType("DataSet"));
        assertTrue(hiveTableDef.hasSuperType("Asset"));
        assertEquals(hiveTableDef.getOption(AtlasEntityDef.OPTION_DISPLAY_TEXT_ATTRIBUTE), "name");

        // Test serialization and deserialization
        String json = AtlasType.toJson(hiveTableDef);
        AtlasEntityDef deserializedDef = AtlasType.fromJson(json, AtlasEntityDef.class);

        assertEquals(deserializedDef.getName(), hiveTableDef.getName());
        assertEquals(deserializedDef.getSuperTypes().size(), hiveTableDef.getSuperTypes().size());
        assertEquals(deserializedDef.getAttributeDefs().size(), hiveTableDef.getAttributeDefs().size());
        assertEquals(deserializedDef.getServiceType(), hiveTableDef.getServiceType());
    }
}
