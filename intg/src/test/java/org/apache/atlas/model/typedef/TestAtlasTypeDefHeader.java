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

import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.type.AtlasType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasTypeDefHeader {
    @Test
    public void testDefaultConstructor() {
        AtlasTypeDefHeader header = new AtlasTypeDefHeader();

        assertNull(header.getGuid());
        assertNull(header.getName());
        assertNull(header.getServiceType());
        assertNull(header.getCategory());
    }

    @Test
    public void testConstructorWithGuidNameCategory() {
        AtlasTypeDefHeader header = new AtlasTypeDefHeader("test-guid", "test-name", TypeCategory.ENTITY);

        assertEquals(header.getGuid(), "test-guid");
        assertEquals(header.getName(), "test-name");
        assertEquals(header.getCategory(), TypeCategory.ENTITY);
        assertNull(header.getServiceType());
    }

    @Test
    public void testConstructorWithAllFields() {
        AtlasTypeDefHeader header = new AtlasTypeDefHeader("test-guid", "test-name", TypeCategory.CLASSIFICATION, "test-service");

        assertEquals(header.getGuid(), "test-guid");
        assertEquals(header.getName(), "test-name");
        assertEquals(header.getCategory(), TypeCategory.CLASSIFICATION);
        assertEquals(header.getServiceType(), "test-service");
    }

    @Test
    public void testConstructorFromAtlasBaseTypeDef() {
        AtlasStructDef structDef = new AtlasStructDef("test-struct", "test description", "1.0");
        structDef.setGuid("struct-guid");
        structDef.setServiceType("test-service");

        AtlasTypeDefHeader header = new AtlasTypeDefHeader(structDef);

        assertEquals(header.getGuid(), "struct-guid");
        assertEquals(header.getName(), "test-struct");
        assertEquals(header.getCategory(), TypeCategory.STRUCT);
        assertEquals(header.getServiceType(), "test-service");
    }

    @Test
    public void testCopyConstructor() {
        AtlasTypeDefHeader original = new AtlasTypeDefHeader("guid1", "name1", TypeCategory.ENUM, "service1");
        AtlasTypeDefHeader copy = new AtlasTypeDefHeader(original);

        assertEquals(copy.getGuid(), original.getGuid());
        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getCategory(), original.getCategory());
        assertEquals(copy.getServiceType(), original.getServiceType());

        assertNotEquals(System.identityHashCode(copy), System.identityHashCode(original));
    }

    @Test
    public void testSettersAndGetters() {
        AtlasTypeDefHeader header = new AtlasTypeDefHeader();

        header.setGuid("new-guid");
        assertEquals(header.getGuid(), "new-guid");

        header.setName("new-name");
        assertEquals(header.getName(), "new-name");

        header.setCategory(TypeCategory.RELATIONSHIP);
        assertEquals(header.getCategory(), TypeCategory.RELATIONSHIP);

        header.setServiceType("new-service");
        assertEquals(header.getServiceType(), "new-service");
    }

    @Test
    public void testAllTypeCategories() {
        for (TypeCategory category : TypeCategory.values()) {
            AtlasTypeDefHeader header = new AtlasTypeDefHeader("guid", "name", category, "service");
            assertEquals(header.getCategory(), category);
        }
    }

    @Test
    public void testEquals() {
        AtlasTypeDefHeader header1 = new AtlasTypeDefHeader("guid1", "name1", TypeCategory.ENTITY, "service1");
        AtlasTypeDefHeader header2 = new AtlasTypeDefHeader("guid1", "name1", TypeCategory.ENTITY, "service1");

        assertEquals(header1, header2);
        assertEquals(header1.hashCode(), header2.hashCode());

        // Test self equality
        assertEquals(header1, header1);

        // Test null
        assertNotEquals(header1, null);

        // Test different class
        assertNotEquals(header1, "string");

        // Test different guid
        AtlasTypeDefHeader header3 = new AtlasTypeDefHeader("guid2", "name1", TypeCategory.ENTITY, "service1");
        assertNotEquals(header1, header3);

        // Test different name
        AtlasTypeDefHeader header4 = new AtlasTypeDefHeader("guid1", "name2", TypeCategory.ENTITY, "service1");
        assertNotEquals(header1, header4);

        // Test different category
        AtlasTypeDefHeader header5 = new AtlasTypeDefHeader("guid1", "name1", TypeCategory.STRUCT, "service1");
        assertNotEquals(header1, header5);

        // Test different service type
        AtlasTypeDefHeader header6 = new AtlasTypeDefHeader("guid1", "name1", TypeCategory.ENTITY, "service2");
        assertNotEquals(header1, header6);
    }

    @Test
    public void testEqualsWithNullValues() {
        AtlasTypeDefHeader header1 = new AtlasTypeDefHeader();
        AtlasTypeDefHeader header2 = new AtlasTypeDefHeader();

        assertEquals(header1, header2);

        header1.setGuid("guid1");
        assertNotEquals(header1, header2);

        header2.setGuid("guid1");
        assertEquals(header1, header2);

        header1.setName("name1");
        assertNotEquals(header1, header2);

        header2.setName("name1");
        assertEquals(header1, header2);

        header1.setCategory(TypeCategory.ENTITY);
        assertNotEquals(header1, header2);

        header2.setCategory(TypeCategory.ENTITY);
        assertEquals(header1, header2);

        header1.setServiceType("service1");
        assertNotEquals(header1, header2);

        header2.setServiceType("service1");
        assertEquals(header1, header2);
    }

    @Test
    public void testHashCode() {
        AtlasTypeDefHeader header1 = new AtlasTypeDefHeader("guid1", "name1", TypeCategory.ENTITY, "service1");
        AtlasTypeDefHeader header2 = new AtlasTypeDefHeader("guid1", "name1", TypeCategory.ENTITY, "service1");

        assertEquals(header1.hashCode(), header2.hashCode());

        // Different objects should likely have different hash codes
        AtlasTypeDefHeader header3 = new AtlasTypeDefHeader("guid2", "name1", TypeCategory.ENTITY, "service1");
        assertNotEquals(header1.hashCode(), header3.hashCode());
    }

    @Test
    public void testToString() {
        AtlasTypeDefHeader header = new AtlasTypeDefHeader("test-guid", "test-name", TypeCategory.CLASSIFICATION, "test-service");

        String toStringResult = header.toString();
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("AtlasTypeDefHeader"));
        assertTrue(toStringResult.contains("test-guid"));
        assertTrue(toStringResult.contains("test-name"));
        assertTrue(toStringResult.contains("CLASSIFICATION"));
        assertTrue(toStringResult.contains("test-service"));
    }

    @Test
    public void testToStringWithStringBuilder() {
        AtlasTypeDefHeader header = new AtlasTypeDefHeader("guid", "name", TypeCategory.ENUM, "service");

        StringBuilder sb = new StringBuilder();
        StringBuilder result = header.toString(sb);

        assertNotNull(result);
        assertTrue(result.toString().contains("guid"));
        assertTrue(result.toString().contains("name"));
        assertTrue(result.toString().contains("ENUM"));
        assertTrue(result.toString().contains("service"));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        AtlasTypeDefHeader header = new AtlasTypeDefHeader("guid", "name", TypeCategory.RELATIONSHIP);

        StringBuilder result = header.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("guid"));
        assertTrue(result.toString().contains("name"));
        assertTrue(result.toString().contains("RELATIONSHIP"));
    }

    @Test
    public void testToStringWithNullValues() {
        AtlasTypeDefHeader header = new AtlasTypeDefHeader();

        String toStringResult = header.toString();
        assertNotNull(toStringResult);
        assertTrue(toStringResult.contains("AtlasTypeDefHeader"));
        assertTrue(toStringResult.contains("null"));
    }

    @Test
    public void testSerializationDeserialization() {
        AtlasTypeDefHeader original = new AtlasTypeDefHeader("test-guid", "test-name", TypeCategory.BUSINESS_METADATA, "test-service");

        String jsonString = AtlasType.toJson(original);
        AtlasTypeDefHeader deserialized = AtlasType.fromJson(jsonString, AtlasTypeDefHeader.class);

        assertEquals(deserialized, original);
        assertEquals(deserialized.getGuid(), original.getGuid());
        assertEquals(deserialized.getName(), original.getName());
        assertEquals(deserialized.getCategory(), original.getCategory());
        assertEquals(deserialized.getServiceType(), original.getServiceType());
    }

    @Test
    public void testWithRealTypeDefScenarios() {
        // Test with AtlasEnumDef
        AtlasEnumDef enumDef = new AtlasEnumDef("TestEnum", "Test enum description");
        enumDef.setGuid("enum-guid");
        enumDef.setServiceType("hive");

        AtlasTypeDefHeader enumHeader = new AtlasTypeDefHeader(enumDef);
        assertEquals(enumHeader.getGuid(), "enum-guid");
        assertEquals(enumHeader.getName(), "TestEnum");
        assertEquals(enumHeader.getCategory(), TypeCategory.ENUM);
        assertEquals(enumHeader.getServiceType(), "hive");

        // Test with AtlasEntityDef
        AtlasEntityDef entityDef = new AtlasEntityDef("TestEntity", "Test entity description");
        entityDef.setGuid("entity-guid");
        entityDef.setServiceType("hdfs");

        AtlasTypeDefHeader entityHeader = new AtlasTypeDefHeader(entityDef);
        assertEquals(entityHeader.getGuid(), "entity-guid");
        assertEquals(entityHeader.getName(), "TestEntity");
        assertEquals(entityHeader.getCategory(), TypeCategory.ENTITY);
        assertEquals(entityHeader.getServiceType(), "hdfs");

        // Test with AtlasClassificationDef
        AtlasClassificationDef classificationDef = new AtlasClassificationDef("TestClassification", "Test classification description");
        classificationDef.setGuid("classification-guid");
        classificationDef.setServiceType("atlas");

        AtlasTypeDefHeader classificationHeader = new AtlasTypeDefHeader(classificationDef);
        assertEquals(classificationHeader.getGuid(), "classification-guid");
        assertEquals(classificationHeader.getName(), "TestClassification");
        assertEquals(classificationHeader.getCategory(), TypeCategory.CLASSIFICATION);
        assertEquals(classificationHeader.getServiceType(), "atlas");
    }

    @Test
    public void testEqualsHashCodeConsistency() {
        AtlasTypeDefHeader header1 = new AtlasTypeDefHeader("guid", "name", TypeCategory.STRUCT, "service");
        AtlasTypeDefHeader header2 = new AtlasTypeDefHeader("guid", "name", TypeCategory.STRUCT, "service");

        // If two objects are equal, their hash codes must be equal
        assertEquals(header1, header2);
        assertEquals(header1.hashCode(), header2.hashCode());

        // Test with all null values
        AtlasTypeDefHeader headerNull1 = new AtlasTypeDefHeader();
        AtlasTypeDefHeader headerNull2 = new AtlasTypeDefHeader();

        assertEquals(headerNull1, headerNull2);
        assertEquals(headerNull1.hashCode(), headerNull2.hashCode());
    }

    @Test
    public void testComplexEqualityScenarios() {
        // Test where only one field is different at a time
        AtlasTypeDefHeader base = new AtlasTypeDefHeader("guid", "name", TypeCategory.ENTITY, "service");

        // Different guid
        AtlasTypeDefHeader diffGuid = new AtlasTypeDefHeader("different-guid", "name", TypeCategory.ENTITY, "service");
        assertNotEquals(base, diffGuid);

        // Different name
        AtlasTypeDefHeader diffName = new AtlasTypeDefHeader("guid", "different-name", TypeCategory.ENTITY, "service");
        assertNotEquals(base, diffName);

        // Different category
        AtlasTypeDefHeader diffCategory = new AtlasTypeDefHeader("guid", "name", TypeCategory.CLASSIFICATION, "service");
        assertNotEquals(base, diffCategory);

        // Different service type
        AtlasTypeDefHeader diffService = new AtlasTypeDefHeader("guid", "name", TypeCategory.ENTITY, "different-service");
        assertNotEquals(base, diffService);

        // Null vs non-null fields
        AtlasTypeDefHeader nullGuid = new AtlasTypeDefHeader(null, "name", TypeCategory.ENTITY, "service");
        assertNotEquals(base, nullGuid);

        AtlasTypeDefHeader nullName = new AtlasTypeDefHeader("guid", null, TypeCategory.ENTITY, "service");
        assertNotEquals(base, nullName);

        AtlasTypeDefHeader nullCategory = new AtlasTypeDefHeader("guid", "name", null, "service");
        assertNotEquals(base, nullCategory);

        AtlasTypeDefHeader nullService = new AtlasTypeDefHeader("guid", "name", TypeCategory.ENTITY, null);
        assertNotEquals(base, nullService);
    }

    @Test
    public void testConstructorEdgeCases() {
        // Test with empty strings
        AtlasTypeDefHeader emptyStrings = new AtlasTypeDefHeader("", "", TypeCategory.PRIMITIVE, "");
        assertEquals(emptyStrings.getGuid(), "");
        assertEquals(emptyStrings.getName(), "");
        assertEquals(emptyStrings.getServiceType(), "");

        // Test with null values passed to constructor
        AtlasTypeDefHeader withNulls = new AtlasTypeDefHeader(null, null, null, null);
        assertNull(withNulls.getGuid());
        assertNull(withNulls.getName());
        assertNull(withNulls.getCategory());
        assertNull(withNulls.getServiceType());
    }
}
