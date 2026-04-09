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
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasBaseTypeDef {
    private static class TestAtlasBaseTypeDefImpl extends AtlasBaseTypeDef {
        public TestAtlasBaseTypeDefImpl(TypeCategory category, String name, String description, String typeVersion, String serviceType, Map<String, String> options) {
            super(category, name, description, typeVersion, serviceType, options);
        }

        public TestAtlasBaseTypeDefImpl(AtlasBaseTypeDef other) {
            super(other);
        }
    }

    @Test
    public void testAtlasBaseTypeDefConstructor() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "value2");

        TestAtlasBaseTypeDefImpl typeDef = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENUM,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                options);

        assertEquals(typeDef.getCategory(), TypeCategory.ENUM);
        assertEquals(typeDef.getName(), "testName");
        assertEquals(typeDef.getDescription(), "testDescription");
        assertEquals(typeDef.getTypeVersion(), "1.0");
        assertEquals(typeDef.getServiceType(), "testService");
        assertEquals(typeDef.getOptions().size(), 2);
        assertEquals(typeDef.getOption("key1"), "value1");
        assertEquals(typeDef.getOption("key2"), "value2");
    }

    @Test
    public void testAtlasBaseTypeDefCopyConstructor() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");

        TestAtlasBaseTypeDefImpl original = new TestAtlasBaseTypeDefImpl(
                TypeCategory.STRUCT,
                "originalName",
                "originalDescription",
                "2.0",
                "originalService",
                options);

        original.setGuid("test-guid");
        original.setCreatedBy("testUser");
        original.setUpdatedBy("updateUser");
        original.setCreateTime(new Date());
        original.setUpdateTime(new Date());
        original.setVersion(1L);

        TestAtlasBaseTypeDefImpl copy = new TestAtlasBaseTypeDefImpl(original);

        assertEquals(copy.getCategory(), original.getCategory());
        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getDescription(), original.getDescription());
        assertEquals(copy.getTypeVersion(), original.getTypeVersion());
        assertEquals(copy.getServiceType(), original.getServiceType());
        assertEquals(copy.getGuid(), original.getGuid());
        assertEquals(copy.getCreatedBy(), original.getCreatedBy());
        assertEquals(copy.getUpdatedBy(), original.getUpdatedBy());
        assertEquals(copy.getCreateTime(), original.getCreateTime());
        assertEquals(copy.getUpdateTime(), original.getUpdateTime());
        assertEquals(copy.getVersion(), original.getVersion());
        assertEquals(copy.getOptions(), original.getOptions());
    }

    @Test
    public void testAtlasBaseTypeDefCopyConstructorWithNull() {
        TestAtlasBaseTypeDefImpl copy = new TestAtlasBaseTypeDefImpl(null);

        assertEquals(copy.getCategory(), TypeCategory.PRIMITIVE);
        assertNull(copy.getName());
        assertNull(copy.getDescription());
        assertNull(copy.getTypeVersion());
        assertNull(copy.getServiceType());
        assertNull(copy.getGuid());
        assertNull(copy.getCreatedBy());
        assertNull(copy.getUpdatedBy());
        assertNull(copy.getCreateTime());
        assertNull(copy.getUpdateTime());
        assertNull(copy.getVersion());
        assertNull(copy.getOptions());
    }

    @Test
    public void testGetArrayTypeName() {
        String arrayTypeName = AtlasBaseTypeDef.getArrayTypeName("string");
        assertEquals(arrayTypeName, "array<string>");
    }

    @Test
    public void testGetMapTypeName() {
        String mapTypeName = AtlasBaseTypeDef.getMapTypeName("string", "int");
        assertEquals(mapTypeName, "map<string,int>");
    }

    @Test
    public void testGetAndSetOptions() {
        TestAtlasBaseTypeDefImpl typeDef = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENTITY,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                null);

        assertNull(typeDef.getOptions());

        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        typeDef.setOptions(options);

        assertNotNull(typeDef.getOptions());
        assertEquals(typeDef.getOptions().size(), 1);
        assertEquals(typeDef.getOption("option1"), "value1");

        // Test setting individual options
        typeDef.setOption("option2", "value2");
        assertEquals(typeDef.getOptions().size(), 2);
        assertEquals(typeDef.getOption("option2"), "value2");

        // Test null option
        assertNull(typeDef.getOption("nonExistentOption"));
    }

    @Test
    public void testSetOptionsWithNull() {
        Map<String, String> initialOptions = new HashMap<>();
        initialOptions.put("key1", "value1");

        TestAtlasBaseTypeDefImpl typeDef = new TestAtlasBaseTypeDefImpl(
                TypeCategory.CLASSIFICATION,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                initialOptions);

        assertNotNull(typeDef.getOptions());
        assertEquals(typeDef.getOptions().size(), 1);

        typeDef.setOptions(null);
        assertNull(typeDef.getOptions());
    }

    @Test
    public void testDateFormatter() {
        DateFormat formatter = AtlasBaseTypeDef.getDateFormatter();
        assertNotNull(formatter);

        // Test that multiple calls return different instances but format consistently
        DateFormat formatter2 = AtlasBaseTypeDef.getDateFormatter();
        assertNotNull(formatter2);

        Date testDate = new Date();
        String formatted1 = formatter.format(testDate);
        String formatted2 = formatter2.format(testDate);
        assertEquals(formatted1, formatted2);
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        TestAtlasBaseTypeDefImpl typeDef = new TestAtlasBaseTypeDefImpl(
                TypeCategory.BUSINESS_METADATA,
                "testName",
                null,
                null,
                null,
                null);
        StringBuilder result = typeDef.toString(null);
        assertNotNull(result);
        assertTrue(result.toString().contains("testName"));
    }

    @Test
    public void testEquals() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");

        TestAtlasBaseTypeDefImpl typeDef1 = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENUM,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                options);

        TestAtlasBaseTypeDefImpl typeDef2 = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENUM,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                options);

        assertEquals(typeDef1, typeDef2);
        assertEquals(typeDef1.hashCode(), typeDef2.hashCode());

        // Test self equality
        assertEquals(typeDef1, typeDef1);

        // Test null
        assertNotEquals(typeDef1, null);

        // Test different class
        assertNotEquals(typeDef1, "string");

        // Test different category
        TestAtlasBaseTypeDefImpl typeDef3 = new TestAtlasBaseTypeDefImpl(
                TypeCategory.STRUCT,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                options);
        assertNotEquals(typeDef1, typeDef3);

        // Test different name
        TestAtlasBaseTypeDefImpl typeDef4 = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENUM,
                "differentName",
                "testDescription",
                "1.0",
                "testService",
                options);
        assertNotEquals(typeDef1, typeDef4);
    }

    @Test
    public void testHashCode() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");

        TestAtlasBaseTypeDefImpl typeDef1 = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENUM,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                options);

        TestAtlasBaseTypeDefImpl typeDef2 = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENUM,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                options);

        assertEquals(typeDef1.hashCode(), typeDef2.hashCode());
    }

    @Test
    public void testBuiltinTypes() {
        // Test that ATLAS_BUILTIN_TYPES contains all primitive types
        assertTrue(Arrays.asList(AtlasBaseTypeDef.ATLAS_BUILTIN_TYPES).containsAll(
                Arrays.asList(AtlasBaseTypeDef.ATLAS_PRIMITIVE_TYPES)));

        // Test specific builtin types
        assertTrue(Arrays.asList(AtlasBaseTypeDef.ATLAS_BUILTIN_TYPES).contains(AtlasBaseTypeDef.ATLAS_TYPE_STRING));
        assertTrue(Arrays.asList(AtlasBaseTypeDef.ATLAS_BUILTIN_TYPES).contains(AtlasBaseTypeDef.ATLAS_TYPE_INT));
        assertTrue(Arrays.asList(AtlasBaseTypeDef.ATLAS_BUILTIN_TYPES).contains(AtlasBaseTypeDef.ATLAS_TYPE_DATE));
        assertTrue(Arrays.asList(AtlasBaseTypeDef.ATLAS_BUILTIN_TYPES).contains(AtlasBaseTypeDef.ATLAS_TYPE_OBJECT_ID));
    }

    @Test
    public void testRelationshipAttributeTypes() {
        // Test that ATLAS_RELATIONSHIP_ATTRIBUTE_TYPES contains primitive types plus date
        assertTrue(Arrays.asList(AtlasBaseTypeDef.ATLAS_RELATIONSHIP_ATTRIBUTE_TYPES).containsAll(
                Arrays.asList(AtlasBaseTypeDef.ATLAS_PRIMITIVE_TYPES)));
        assertTrue(Arrays.asList(AtlasBaseTypeDef.ATLAS_RELATIONSHIP_ATTRIBUTE_TYPES).contains(AtlasBaseTypeDef.ATLAS_TYPE_DATE));
    }

    @Test
    public void testConstants() {
        assertEquals(AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_PREFIX, "array<");
        assertEquals(AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_SUFFIX, ">");
        assertEquals(AtlasBaseTypeDef.ATLAS_TYPE_MAP_PREFIX, "map<");
        assertEquals(AtlasBaseTypeDef.ATLAS_TYPE_MAP_KEY_VAL_SEP, ",");
        assertEquals(AtlasBaseTypeDef.ATLAS_TYPE_MAP_SUFFIX, ">");
        assertEquals(AtlasBaseTypeDef.SERVICE_TYPE_ATLAS_CORE, "atlas_core");
        assertEquals(AtlasBaseTypeDef.SERIALIZED_DATE_FORMAT_STR, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    }

    @Test
    public void testDumpObjects() throws Exception {
        // Test using reflection to access static methods
        Method dumpObjectsCollection = AtlasBaseTypeDef.class.getDeclaredMethod("dumpObjects", java.util.Collection.class, StringBuilder.class);
        Method dumpObjectsMap = AtlasBaseTypeDef.class.getDeclaredMethod("dumpObjects", Map.class, StringBuilder.class);
        Method dumpDateField = AtlasBaseTypeDef.class.getDeclaredMethod("dumpDateField", String.class, Date.class, StringBuilder.class);

        dumpObjectsCollection.setAccessible(true);
        dumpObjectsMap.setAccessible(true);
        dumpDateField.setAccessible(true);

        // Test dumpObjects with collection
        StringBuilder sb = new StringBuilder();
        StringBuilder result = (StringBuilder) dumpObjectsCollection.invoke(null, Arrays.asList("item1", "item2"), sb);
        assertTrue(result.toString().contains("item1"));
        assertTrue(result.toString().contains("item2"));

        // Test dumpObjects with map
        sb = new StringBuilder();
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        testMap.put("key2", "value2");
        result = (StringBuilder) dumpObjectsMap.invoke(null, testMap, sb);
        assertTrue(result.toString().contains("key1:value1") || result.toString().contains("key2:value2"));

        // Test dumpDateField
        sb = new StringBuilder();
        Date testDate = new Date();
        result = (StringBuilder) dumpDateField.invoke(null, "Created: ", testDate, sb);
        assertTrue(result.toString().contains("Created: "));

        // Test dumpDateField with null
        sb = new StringBuilder();
        result = (StringBuilder) dumpDateField.invoke(null, "Created: ", null, sb);
        assertTrue(result.toString().contains("Created: null"));
    }

    @Test
    public void testGettersAndSetters() {
        TestAtlasBaseTypeDefImpl typeDef = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENTITY,
                "initialName",
                "initialDescription",
                "1.0",
                "initialService",
                null);

        // Test setters and getters
        typeDef.setGuid("new-guid");
        assertEquals(typeDef.getGuid(), "new-guid");

        typeDef.setCreatedBy("newCreator");
        assertEquals(typeDef.getCreatedBy(), "newCreator");

        typeDef.setUpdatedBy("newUpdater");
        assertEquals(typeDef.getUpdatedBy(), "newUpdater");

        Date createTime = new Date();
        typeDef.setCreateTime(createTime);
        assertEquals(typeDef.getCreateTime(), createTime);

        Date updateTime = new Date();
        typeDef.setUpdateTime(updateTime);
        assertEquals(typeDef.getUpdateTime(), updateTime);

        typeDef.setVersion(5L);
        assertEquals(typeDef.getVersion(), Long.valueOf(5L));

        typeDef.setName("newName");
        assertEquals(typeDef.getName(), "newName");

        typeDef.setDescription("newDescription");
        assertEquals(typeDef.getDescription(), "newDescription");

        typeDef.setTypeVersion("2.0");
        assertEquals(typeDef.getTypeVersion(), "2.0");

        typeDef.setServiceType("newService");
        assertEquals(typeDef.getServiceType(), "newService");
    }

    @Test
    public void testSetOptionWithNullOptions() {
        TestAtlasBaseTypeDefImpl typeDef = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENTITY,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                null);

        assertNull(typeDef.getOptions());

        typeDef.setOption("newOption", "newValue");
        assertNotNull(typeDef.getOptions());
        assertEquals(typeDef.getOptions().size(), 1);
        assertEquals(typeDef.getOption("newOption"), "newValue");
    }

    @Test
    public void testEqualsWithAllFields() {
        Date createTime = new Date();
        Date updateTime = new Date();
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");

        TestAtlasBaseTypeDefImpl typeDef1 = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENTITY,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                options);
        typeDef1.setGuid("test-guid");
        typeDef1.setCreatedBy("creator");
        typeDef1.setUpdatedBy("updater");
        typeDef1.setCreateTime(createTime);
        typeDef1.setUpdateTime(updateTime);
        typeDef1.setVersion(1L);

        TestAtlasBaseTypeDefImpl typeDef2 = new TestAtlasBaseTypeDefImpl(
                TypeCategory.ENTITY,
                "testName",
                "testDescription",
                "1.0",
                "testService",
                options);
        typeDef2.setGuid("test-guid");
        typeDef2.setCreatedBy("creator");
        typeDef2.setUpdatedBy("updater");
        typeDef2.setCreateTime(createTime);
        typeDef2.setUpdateTime(updateTime);
        typeDef2.setVersion(1L);

        assertEquals(typeDef1, typeDef2);

        // Test differences in each field
        typeDef2.setGuid("different-guid");
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setGuid("test-guid");
        typeDef2.setCreatedBy("different-creator");
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setCreatedBy("creator");
        typeDef2.setUpdatedBy("different-updater");
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setUpdatedBy("updater");
        typeDef2.setCreateTime(new Date(createTime.getTime() + 1000));
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setCreateTime(createTime);
        typeDef2.setUpdateTime(new Date(updateTime.getTime() + 1000));
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setUpdateTime(updateTime);
        typeDef2.setVersion(2L);
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setVersion(1L);
        typeDef2.setDescription("different-description");
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setDescription("testDescription");
        typeDef2.setTypeVersion("2.0");
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setTypeVersion("1.0");
        typeDef2.setServiceType("different-service");
        assertNotEquals(typeDef1, typeDef2);

        typeDef2.setServiceType("testService");
        Map<String, String> differentOptions = new HashMap<>();
        differentOptions.put("key2", "value2");
        typeDef2.setOptions(differentOptions);
        assertNotEquals(typeDef1, typeDef2);
    }
}
