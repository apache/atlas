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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasIntType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasMapType {
    private final AtlasMapType intIntMapType = new AtlasMapType(new AtlasIntType(), new AtlasIntType());
    private final Object[]     validValues;
    private final Object[]     invalidValues;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    private AtlasBuiltInTypes.AtlasStringType stringType;
    private AtlasBuiltInTypes.AtlasIntType intType;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        stringType = new AtlasBuiltInTypes.AtlasStringType();
        intType = new AtlasBuiltInTypes.AtlasIntType();
    }

    @Test
    public void testMapTypeDefaultValue() {
        Map<Object, Object> defValue = intIntMapType.createDefaultValue();

        assertEquals(defValue.size(), 1);
    }

    @Test
    public void testMapTypeIsValidValue() {
        for (Object value : validValues) {
            assertTrue(intIntMapType.isValidValue(value), "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(intIntMapType.isValidValue(value), "value=" + value);
        }
    }

    @Test
    public void testMapTypeGetNormalizedValue() {
        assertNull(intIntMapType.getNormalizedValue(null), "value=" + null);

        for (Object value : validValues) {
            if (value == null) {
                continue;
            }

            Map<Object, Object> normalizedValue = intIntMapType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertNull(intIntMapType.getNormalizedValue(value), "value=" + value);
        }
    }

    @Test
    public void testMapTypeValidateValue() {
        List<String> messages = new ArrayList<>();
        for (Object value : validValues) {
            assertTrue(intIntMapType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(intIntMapType.validateValue(value, "testObj", messages));
            assertFalse(messages.isEmpty(), "value=" + value);
            messages.clear();
        }
    }

    {
        Map<String, Integer>  strIntMap     = new HashMap<>();
        Map<String, Double>   strDoubleMap  = new HashMap<>();
        Map<String, String>   strStringMap  = new HashMap<>();
        Map<Integer, Integer> intIntMap     = new HashMap<>();
        Map<Object, Object>   objObjMap     = new HashMap<>();
        Map<Object, Object>   invObjObjMap1 = new HashMap<>();
        Map<Object, Object>   invObjObjMap2 = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            strIntMap.put(Integer.toString(i), i);
            strDoubleMap.put(Integer.toString(i), (double) i);
            strStringMap.put(Integer.toString(i), Integer.toString(i));
            intIntMap.put(i, i);
            objObjMap.put(i, i);
        }

        invObjObjMap1.put("xyz", "123"); // invalid key
        invObjObjMap2.put("123", "xyz"); // invalid value

        validValues = new Object[] {
                null, new HashMap<String, Integer>(), new HashMap<>(), strIntMap, strDoubleMap, strStringMap,
                intIntMap, objObjMap,
        };

        invalidValues = new Object[] {invObjObjMap1, invObjObjMap2, };
    }

    @Test
    public void testConstructorWithTypes() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        assertEquals(mapType.getKeyTypeName(), "string");
        assertEquals(mapType.getValueTypeName(), "int");
        assertEquals(mapType.getKeyType(), stringType);
        assertEquals(mapType.getValueType(), intType);
    }

    @Test
    public void testConstructorWithTypeRegistry() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(stringType);
        when(mockTypeRegistry.getType("int")).thenReturn(intType);

        AtlasMapType mapType = new AtlasMapType("string", "int", mockTypeRegistry);

        assertEquals(mapType.getKeyTypeName(), "string");
        assertEquals(mapType.getValueTypeName(), "int");
        assertEquals(mapType.getKeyType(), stringType);
        assertEquals(mapType.getValueType(), intType);
    }

    @Test
    public void testSetKeyType() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);
        AtlasBuiltInTypes.AtlasLongType longType = new AtlasBuiltInTypes.AtlasLongType();

        mapType.setKeyType(longType);
        assertEquals(mapType.getKeyType(), longType);
    }

    @Test
    public void testCreateDefaultValue() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        Map<Object, Object> defaultValue = mapType.createDefaultValue();
        assertNotNull(defaultValue);
        assertEquals(defaultValue.size(), 1);

        String defaultKey = stringType.createDefaultValue();
        Integer defaultValueInt = intType.createDefaultValue();

        assertTrue(defaultValue.containsKey(defaultKey));
        assertEquals(defaultValue.get(defaultKey), defaultValueInt);
    }

    @Test
    public void testCreateDefaultValueWithNullKey() {
        // Create a mock key type that returns null for default value
        AtlasType mockKeyType = new AtlasBuiltInTypes.AtlasStringType() {
            @Override
            public String createDefaultValue() {
                return null;
            }
        };

        AtlasMapType mapType = new AtlasMapType(mockKeyType, intType);
        Map<Object, Object> defaultValue = mapType.createDefaultValue();

        assertNotNull(defaultValue);
        assertEquals(defaultValue.size(), 0); // Should be empty when key is null
    }

    @Test
    public void testIsValidValueWithMap() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        // Valid cases
        assertTrue(mapType.isValidValue(null));

        Map<String, Integer> validMap = new HashMap<>();
        validMap.put("key1", 100);
        validMap.put("key2", 200);
        assertTrue(mapType.isValidValue(validMap));

        // Empty map
        assertTrue(mapType.isValidValue(new HashMap<>()));

        // Different key type (will be validated by key type)
        Map<Integer, Integer> intKeyMap = new HashMap<>();
        intKeyMap.put(123, 100);
        assertTrue(mapType.isValidValue(intKeyMap)); // Integer keys can be normalized to strings

        // Different value type (will be validated by value type)
        Map<String, String> stringValueMap = new HashMap<>();
        stringValueMap.put("key", "123");
        assertTrue(mapType.isValidValue(stringValueMap)); // String values can be normalized to integers
    }

    @Test
    public void testIsValidValueWithNonMap() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        assertFalse(mapType.isValidValue("not a map"));
        assertFalse(mapType.isValidValue(123));
        assertFalse(mapType.isValidValue(Arrays.asList("list", "not", "map")));
    }

    @Test
    public void testAreEqualValues() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        Map<String, Integer> map1 = new HashMap<>();
        map1.put("key1", 100);
        map1.put("key2", 200);

        Map<String, Integer> map2 = new HashMap<>();
        map2.put("key1", 100);
        map2.put("key2", 200);

        Map<String, Integer> map3 = new HashMap<>();
        map3.put("key1", 100);
        map3.put("key2", 300); // different value

        Map<String, Integer> map4 = new HashMap<>();
        map4.put("key1", 100); // missing key2

        assertTrue(mapType.areEqualValues(map1, map2, null));
        assertFalse(mapType.areEqualValues(map1, map3, null));
        assertFalse(mapType.areEqualValues(map1, map4, null));
        assertTrue(mapType.areEqualValues(null, null, null));
        assertFalse(mapType.areEqualValues(map1, null, null));
    }

    @Test
    public void testAreEqualValuesWithEmptyMaps() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        Map<String, Integer> emptyMap1 = new HashMap<>();
        Map<String, Integer> emptyMap2 = new HashMap<>();

        assertTrue(mapType.areEqualValues(emptyMap1, emptyMap2, null));
        assertTrue(mapType.areEqualValues(null, emptyMap1, null)); // null treated as empty
        assertTrue(mapType.areEqualValues(emptyMap1, null, null)); // empty treated as null
    }

    @Test
    public void testGetNormalizedValueWithMap() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("key1", "100"); // String that can be normalized to int
        inputMap.put("key2", 200);   // Already int

        Map<Object, Object> normalized = mapType.getNormalizedValue(inputMap);

        assertNotNull(normalized);
        assertEquals(normalized.size(), 2);
        assertEquals(normalized.get("key1"), 100);
        assertEquals(normalized.get("key2"), 200);
    }

    @Test
    public void testGetNormalizedValueWithJsonString() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        String jsonString = "{\"key1\": 100, \"key2\": 200}";
        Map<Object, Object> normalized = mapType.getNormalizedValue(jsonString);

        assertNotNull(normalized);
        // Note: The actual behavior depends on the JSON parsing implementation
    }

    @Test
    public void testGetNormalizedValueWithNullKey() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put(null, 100); // null key - should cause normalization to fail

        Map<Object, Object> normalized = mapType.getNormalizedValue(inputMap);

        assertNull(normalized); // Should return null due to invalid key
    }

    @Test
    public void testGetNormalizedValueWithInvalidKey() {
        // Create a mock key type that returns null for certain values
        AtlasType mockKeyType = new AtlasBuiltInTypes.AtlasStringType() {
            @Override
            public String getNormalizedValue(Object obj) {
                if ("invalid".equals(obj)) {
                    return null;
                }
                return super.getNormalizedValue(obj);
            }
        };

        AtlasMapType mapType = new AtlasMapType(mockKeyType, intType);

        Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put("invalid", 100);

        Map<Object, Object> normalized = mapType.getNormalizedValue(inputMap);

        assertNull(normalized); // Should return null due to invalid key
    }

    @Test
    public void testGetNormalizedValueWithNullValue() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put("key1", null); // null value should be preserved
        inputMap.put("key2", 200);

        Map<Object, Object> normalized = mapType.getNormalizedValue(inputMap);

        assertNotNull(normalized);
        assertEquals(normalized.size(), 2);
        assertNull(normalized.get("key1"));
        assertEquals(normalized.get("key2"), 200);
    }

    @Test
    public void testGetNormalizedValueWithInvalidValue() {
        // Create a mock value type that returns null for certain values
        AtlasType mockValueType = new AtlasBuiltInTypes.AtlasIntType() {
            @Override
            public Integer getNormalizedValue(Object obj) {
                if ("invalid".equals(obj)) {
                    return null;
                }
                return super.getNormalizedValue(obj);
            }
        };

        AtlasMapType mapType = new AtlasMapType(stringType, mockValueType);

        Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put("key1", "invalid");

        Map<Object, Object> normalized = mapType.getNormalizedValue(inputMap);

        assertNull(normalized); // Should return null due to invalid value
    }

    @Test
    public void testGetNormalizedValueWithNull() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        assertNull(mapType.getNormalizedValue(null));
    }

    @Test
    public void testGetNormalizedValueWithNonMap() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        assertNull(mapType.getNormalizedValue("not a map"));
        assertNull(mapType.getNormalizedValue(123));
    }

    @Test
    public void testValidateValue() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);
        List<String> messages = new ArrayList<>();

        // Valid case
        Map<String, Integer> validMap = new HashMap<>();
        validMap.put("key1", 100);
        assertTrue(mapType.validateValue(validMap, "testMap", messages));
        assertTrue(messages.isEmpty());

        // Different key type (validated individually)
        messages.clear();
        Map<Integer, Integer> intKeyMap = new HashMap<>();
        intKeyMap.put(123, 100);
        assertTrue(mapType.validateValue(intKeyMap, "testMap", messages)); // Integer keys normalized to string

        // Different value type (validated individually)
        messages.clear();
        Map<String, String> stringValueMap = new HashMap<>();
        stringValueMap.put("key", "123");
        assertTrue(mapType.validateValue(stringValueMap, "testMap", messages)); // String values normalized to int

        // Invalid type
        messages.clear();
        assertFalse(mapType.validateValue("not a map", "testMap", messages));
        assertFalse(messages.isEmpty());
    }

    @Test
    public void testIsValidValueForUpdate() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        assertTrue(mapType.isValidValueForUpdate(null));

        Map<String, Integer> validMap = new HashMap<>();
        validMap.put("key1", 100);
        assertTrue(mapType.isValidValueForUpdate(validMap));

        Map<Integer, Integer> intKeyMap = new HashMap<>();
        intKeyMap.put(123, 100);
        assertTrue(mapType.isValidValueForUpdate(intKeyMap)); // Integer keys can be normalized

        assertFalse(mapType.isValidValueForUpdate("not a map"));
    }

    @Test
    public void testGetNormalizedValueForUpdate() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("key1", "100");

        Map<Object, Object> normalized = mapType.getNormalizedValueForUpdate(inputMap);

        assertNotNull(normalized);
        assertEquals(normalized.get("key1"), 100);
    }

    @Test
    public void testValidateValueForUpdate() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);
        List<String> messages = new ArrayList<>();

        Map<String, Integer> validMap = new HashMap<>();
        validMap.put("key1", 100);
        assertTrue(mapType.validateValueForUpdate(validMap, "testMap", messages));
        assertTrue(messages.isEmpty());

        messages.clear();
        assertFalse(mapType.validateValueForUpdate("not a map", "testMap", messages));
        assertFalse(messages.isEmpty());
    }

    @Test
    public void testGetTypeForAttribute() {
        AtlasMapType mapType = new AtlasMapType(stringType, intType);

        AtlasType typeForAttribute = mapType.getTypeForAttribute();

        // If key and value types don't change, should return same instance
        assertEquals(typeForAttribute, mapType);
    }

    @Test
    public void testGetTypeForAttributeWithChangedTypes() {
        // Create mock types that return different types for attribute
        AtlasType mockKeyType = new AtlasBuiltInTypes.AtlasStringType() {
            @Override
            public AtlasType getTypeForAttribute() {
                return new AtlasBuiltInTypes.AtlasIntType();
            }
        };

        AtlasType mockValueType = new AtlasBuiltInTypes.AtlasIntType() {
            @Override
            public AtlasType getTypeForAttribute() {
                return new AtlasBuiltInTypes.AtlasStringType();
            }
        };

        AtlasMapType mapType = new AtlasMapType(mockKeyType, mockValueType);
        AtlasType typeForAttribute = mapType.getTypeForAttribute();

        // Should return new instance since types changed
        assertNotEquals(typeForAttribute, mapType);
        assertTrue(typeForAttribute instanceof AtlasMapType);
    }

    @Test
    public void testResolveReferences() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(stringType);
        when(mockTypeRegistry.getType("int")).thenReturn(intType);

        AtlasMapType mapType = new AtlasMapType("string", "int", mockTypeRegistry);

        assertEquals(mapType.getKeyType(), stringType);
        assertEquals(mapType.getValueType(), intType);
    }
}
