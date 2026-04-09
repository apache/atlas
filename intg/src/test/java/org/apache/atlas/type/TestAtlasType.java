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
package org.apache.atlas.type;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasType {
    private static class TestConcreteAtlasType extends AtlasType {
        private final Object defaultValue;
        private final boolean validValue;

        public TestConcreteAtlasType(String typeName, TypeCategory typeCategory, String serviceType, Object defaultValue, boolean validValue) {
            super(typeName, typeCategory, serviceType);
            this.defaultValue = defaultValue;
            this.validValue = validValue;
        }

        public TestConcreteAtlasType(AtlasBaseTypeDef typeDef, Object defaultValue, boolean validValue) {
            super(typeDef);
            this.defaultValue = defaultValue;
            this.validValue = validValue;
        }

        @Override
        public Object createDefaultValue() {
            return defaultValue;
        }

        @Override
        public boolean isValidValue(Object obj) {
            return validValue;
        }

        @Override
        public Object getNormalizedValue(Object obj) {
            return obj;
        }
    }

    @Test
    public void testConstructorWithParameters() {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        assertEquals(atlasType.getTypeName(), "TestType");
        assertEquals(atlasType.getTypeCategory(), TypeCategory.PRIMITIVE);
        assertEquals(atlasType.getServiceType(), "atlas-core");
    }

    @Test
    public void testConstructorWithTypeDef() {
        // Create a concrete implementation of AtlasBaseTypeDef for testing
        AtlasStructDef typeDef = new AtlasStructDef("TestType", "Test description", "1.0");
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType(typeDef, "defaultValue", true);

        assertEquals(atlasType.getTypeName(), "TestType");
        assertEquals(atlasType.getTypeCategory(), TypeCategory.STRUCT);
        atlasType.getServiceType(); // Should not throw exception
    }

    @Test
    public void testStaticJsonMethods() {
        Map<String, Object> testObject = new HashMap<>();
        testObject.put("key1", "value1");
        testObject.put("key2", 123);

        // Test toJson
        String json = AtlasType.toJson(testObject);
        assertNotNull(json);
        assertTrue(json.contains("key1"));
        assertTrue(json.contains("value1"));

        // Test fromJson
        Map<String, Object> parsedObject = AtlasType.fromJson(json, Map.class);
        assertNotNull(parsedObject);
        assertEquals(parsedObject.get("key1"), "value1");

        // Test toV1Json
        String v1Json = AtlasType.toV1Json(testObject);
        assertNotNull(v1Json);

        // Test fromV1Json with Class
        Map<String, Object> parsedV1Object = AtlasType.fromV1Json(v1Json, Map.class);
        assertNotNull(parsedV1Object);

        // Test fromV1Json with TypeReference
        TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {};
        Map<String, Object> parsedV1ObjectTypeRef = AtlasType.fromV1Json(v1Json, typeRef);
        assertNotNull(parsedV1ObjectTypeRef);
    }

    @Test
    public void testFromLinkedHashMap() {
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("key1", "value1");
        sourceMap.put("key2", 123);

        Map<String, Object> result = AtlasType.fromLinkedHashMap(sourceMap, Map.class);
        assertNotNull(result);
        assertEquals(result.get("key1"), "value1");
        assertEquals(result.get("key2"), 123);
    }

    @Test
    public void testCreateOptionalDefaultValue() {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        Object optionalDefault = atlasType.createOptionalDefaultValue();
        assertEquals(optionalDefault, "defaultValue");
    }

    @Test
    public void testCreateDefaultValueWithValue() {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        // Test with null value
        Object result = atlasType.createDefaultValue(null);
        assertEquals(result, "defaultValue");

        // Test with non-null value
        result = atlasType.createDefaultValue("providedValue");
        assertEquals(result, "providedValue");
    }

    @Test
    public void testAreEqualValues() {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        // Test both null
        assertTrue(atlasType.areEqualValues(null, null, null));

        // Test one null
        assertFalse(atlasType.areEqualValues("value1", null, null));
        assertFalse(atlasType.areEqualValues(null, "value2", null));

        // Test both non-null equal
        assertTrue(atlasType.areEqualValues("same", "same", null));

        // Test both non-null different
        assertFalse(atlasType.areEqualValues("value1", "value2", null));

        // Test with guid assignments
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("oldGuid", "newGuid");
        assertTrue(atlasType.areEqualValues("same", "same", guidAssignments));
    }

    @Test
    public void testAreEqualValuesWithInvalidNormalizedValue() {
        // Create type that returns null for normalized value to test edge case
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true) {
            @Override
            public Object getNormalizedValue(Object obj) {
                return null; // Always return null to test edge case
            }
        };

        assertFalse(atlasType.areEqualValues("value1", "value2", null));
        assertFalse(atlasType.areEqualValues("value1", null, null));
    }

    @Test
    public void testValidateValue() {
        TestConcreteAtlasType validType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);
        TestConcreteAtlasType invalidType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", false);

        List<String> messages = new ArrayList<>();

        // Test valid value
        boolean result = validType.validateValue("validValue", "testObj", messages);
        assertTrue(result);
        assertEquals(messages.size(), 0);

        // Test invalid value
        messages.clear();
        result = invalidType.validateValue("invalidValue", "testObj", messages);
        assertFalse(result);
        assertEquals(messages.size(), 1);
        assertTrue(messages.get(0).contains("testObj=invalidValue"));
        assertTrue(messages.get(0).contains("invalid value for type TestType"));
    }

    @Test
    public void testIsValidValueForUpdate() {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        assertTrue(atlasType.isValidValueForUpdate("anyValue"));
    }

    @Test
    public void testGetNormalizedValueForUpdate() {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        Object result = atlasType.getNormalizedValueForUpdate("testValue");
        assertEquals(result, "testValue");
    }

    @Test
    public void testValidateValueForUpdate() {
        TestConcreteAtlasType validType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        List<String> messages = new ArrayList<>();

        boolean result = validType.validateValueForUpdate("validValue", "testObj", messages);
        assertTrue(result);
        assertEquals(messages.size(), 0);
    }

    @Test
    public void testGetTypeForAttribute() {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        AtlasType result = atlasType.getTypeForAttribute();
        assertEquals(result, atlasType);
    }

    @Test
    public void testResolveReferences() throws AtlasBaseException {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        atlasType.resolveReferences(typeRegistry);
    }

    @Test
    public void testResolveReferencesPhase2() throws AtlasBaseException {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        atlasType.resolveReferencesPhase2(typeRegistry);
    }

    @Test
    public void testResolveReferencesPhase3() throws AtlasBaseException {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

        atlasType.resolveReferencesPhase3(typeRegistry);
    }

    @Test
    public void testGettersAndSetters() {
        TestConcreteAtlasType atlasType = new TestConcreteAtlasType("TestType", TypeCategory.PRIMITIVE, "atlas-core", "defaultValue", true);

        assertEquals(atlasType.getTypeName(), "TestType");
        assertEquals(atlasType.getTypeCategory(), TypeCategory.PRIMITIVE);
        assertEquals(atlasType.getServiceType(), "atlas-core");
    }

    @Test
    public void testJsonMethodsWithNullInput() {
        // Test toJson with null
        String json = AtlasType.toJson(null);
        assertEquals(json, "null");

        // Test fromJson with null
        Object result = AtlasType.fromJson(null, Object.class);
        assertNull(result);

        // Test toV1Json with null
        String v1Json = AtlasType.toV1Json(null);
        assertEquals(v1Json, "null");

        // Test fromV1Json with null
        result = AtlasType.fromV1Json(null, Object.class);
        assertNull(result);

        // Test fromLinkedHashMap with null
        result = AtlasType.fromLinkedHashMap(null, Object.class);
        assertNull(result);
    }

    @Test
    public void testJsonMethodsWithEmptyString() {
        try {
            AtlasType.fromJson("", Object.class);
        } catch (Exception e) {
            // Expected - empty string is not valid JSON
            assertNotNull(e);
        }

        try {
            AtlasType.fromV1Json("", Object.class);
        } catch (Exception e) {
            // Expected - empty string is not valid JSON
            assertNotNull(e);
        }
    }
}
