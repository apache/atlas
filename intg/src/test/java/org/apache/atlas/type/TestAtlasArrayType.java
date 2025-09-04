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
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasIntType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasArrayType {
    private final AtlasArrayType intArrayType = new AtlasArrayType(new AtlasIntType());
    private final Object[]       validValues;
    private final Object[]       invalidValues;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    private AtlasBuiltInTypes.AtlasStringType stringType;
    private AtlasBuiltInTypes.AtlasIntType intType;

    @Test
    public void testArrayTypeDefaultValue() {
        Collection defValue = intArrayType.createDefaultValue();

        assertEquals(defValue.size(), 1);
    }

    @Test
    public void testArrayTypeIsValidValue() {
        for (Object value : validValues) {
            assertTrue(intArrayType.isValidValue(value), "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(intArrayType.isValidValue(value), "value=" + value);
        }
    }

    @Test
    public void testArrayTypeGetNormalizedValue() {
        assertNull(intArrayType.getNormalizedValue(null), "value=" + null);

        for (Object value : validValues) {
            if (value == null) {
                continue;
            }

            Collection normalizedValue = intArrayType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertNull(intArrayType.getNormalizedValue(value), "value=" + value);
        }
    }

    @Test
    public void testArrayTypeValidateValue() {
        List<String> messages = new ArrayList<>();
        for (Object value : validValues) {
            assertTrue(intArrayType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(intArrayType.validateValue(value, "testObj", messages));
            assertTrue(messages.size() > 0, "value=" + value);
            messages.clear();
        }
    }

    {
        List<Integer> intList  = new ArrayList<>();
        Set<Integer>  intSet   = new HashSet<>();
        Integer[]     intArray = new Integer[] {1, 2, 3};
        List<Object>  objList  = new ArrayList<>();
        Set<Object>   objSet   = new HashSet<>();
        Object[]      objArray = new Object[] {1, 2, 3};
        List<String>  strList  = new ArrayList<>();
        Set<String>   strSet   = new HashSet<>();
        String[]      strArray = new String[] {"1", "2", "3"};

        for (int i = 0; i < 10; i++) {
            intList.add(i);
            intSet.add(i);
            objList.add(i);
            objSet.add(i);
            strList.add(Integer.toString(i));
            strSet.add(Integer.toString(i));
        }

        validValues = new Object[] {
                null, new Integer[] {}, intList, intSet, intArray, objList, objSet, objArray, strList, strSet, strArray,
                new byte[] {1}, new short[] {1}, new int[] {1}, new long[] {1}, new float[] {1},
                new double[] {1}, new BigInteger[] {BigInteger.valueOf(1)}, new BigDecimal[] {BigDecimal.valueOf(1)},
        };

        invalidValues = new Object[] {
                Byte.valueOf((byte) 1), Short.valueOf((short) 1),
                Integer.valueOf(1), Long.valueOf(1L), Float.valueOf(1), Double.valueOf(1), BigInteger.valueOf(1),
                BigDecimal.valueOf(1),
        };
    }

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        stringType = new AtlasBuiltInTypes.AtlasStringType();
        intType = new AtlasBuiltInTypes.AtlasIntType();
    }

    @Test
    public void testConstructorWithElementType() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType);

        assertEquals(arrayType.getElementTypeName(), "string");
        assertEquals(arrayType.getElementType(), stringType);
        assertEquals(arrayType.getMinCount(), -1); // COUNT_NOT_SET
        assertEquals(arrayType.getMaxCount(), -1); // COUNT_NOT_SET
        assertEquals(arrayType.getCardinality(), AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);
    }

    @Test
    public void testConstructorWithElementTypeAndCounts() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.SET);

        assertEquals(arrayType.getElementTypeName(), "string");
        assertEquals(arrayType.getElementType(), stringType);
        assertEquals(arrayType.getMinCount(), 1);
        assertEquals(arrayType.getMaxCount(), 5);
        assertEquals(arrayType.getCardinality(), AtlasStructDef.AtlasAttributeDef.Cardinality.SET);
    }

    @Test
    public void testConstructorWithTypeRegistry() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(stringType);

        AtlasArrayType arrayType = new AtlasArrayType("string", mockTypeRegistry);

        assertEquals(arrayType.getElementTypeName(), "string");
        assertEquals(arrayType.getElementType(), stringType);
        assertEquals(arrayType.getMinCount(), -1);
        assertEquals(arrayType.getMaxCount(), -1);
        assertEquals(arrayType.getCardinality(), AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);
    }

    @Test
    public void testConstructorWithTypeRegistryAndCounts() throws AtlasBaseException {
        when(mockTypeRegistry.getType("int")).thenReturn(intType);

        AtlasArrayType arrayType = new AtlasArrayType("int", 2, 10, AtlasStructDef.AtlasAttributeDef.Cardinality.SET, mockTypeRegistry);

        assertEquals(arrayType.getElementTypeName(), "int");
        assertEquals(arrayType.getElementType(), intType);
        assertEquals(arrayType.getMinCount(), 2);
        assertEquals(arrayType.getMaxCount(), 10);
        assertEquals(arrayType.getCardinality(), AtlasStructDef.AtlasAttributeDef.Cardinality.SET);
    }

    @Test
    public void testSettersAndGetters() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType);

        arrayType.setMinCount(3);
        arrayType.setMaxCount(7);
        arrayType.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SET);

        assertEquals(arrayType.getMinCount(), 3);
        assertEquals(arrayType.getMaxCount(), 7);
        assertEquals(arrayType.getCardinality(), AtlasStructDef.AtlasAttributeDef.Cardinality.SET);
    }

    @Test
    public void testCreateDefaultValue() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType);

        Collection<?> defaultValue = arrayType.createDefaultValue();
        assertNotNull(defaultValue);
        assertEquals(defaultValue.size(), 1);
        assertEquals(defaultValue.iterator().next(), stringType.createDefaultValue());
    }

    @Test
    public void testCreateDefaultValueWithMinCount() {
        AtlasArrayType arrayType = new AtlasArrayType(intType, 3, 10, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        Collection<?> defaultValue = arrayType.createDefaultValue();
        assertNotNull(defaultValue);
        assertEquals(defaultValue.size(), 3);
        for (Object value : defaultValue) {
            assertEquals(value, intType.createDefaultValue());
        }
    }

    @Test
    public void testIsValidValueWithList() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        // Valid cases
        assertTrue(arrayType.isValidValue(null));
        assertTrue(arrayType.isValidValue(Arrays.asList("test1", "test2")));
        assertTrue(arrayType.isValidValue(Arrays.asList("single")));

        // Invalid cases - count violations
        assertFalse(arrayType.isValidValue(Arrays.asList())); // below minCount
        assertFalse(arrayType.isValidValue(Arrays.asList("a", "b", "c", "d"))); // above maxCount

        // Invalid element values
        List<Object> invalidElementList = Arrays.asList("valid", 123);
        assertTrue(arrayType.isValidValue(invalidElementList)); // Mixed types are actually valid as they get normalized
    }

    @Test
    public void testIsValidValueWithSet() {
        AtlasArrayType arrayType = new AtlasArrayType(intType, 1, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.SET);

        Set<Integer> validSet = new HashSet<>(Arrays.asList(1, 2));
        assertTrue(arrayType.isValidValue(validSet));

        Set<Integer> invalidSet = new HashSet<>(Arrays.asList(1, 2, 3, 4));
        assertFalse(arrayType.isValidValue(invalidSet)); // above maxCount
    }

    @Test
    public void testIsValidValueWithArray() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        String[] validArray = {"test1", "test2"};
        assertTrue(arrayType.isValidValue(validArray));

        String[] invalidArray = {"a", "b", "c", "d"};
        assertFalse(arrayType.isValidValue(invalidArray)); // above maxCount

        Object[] mixedArray = {"valid", 123};
        assertTrue(arrayType.isValidValue(mixedArray)); // Mixed types are valid as they get normalized
    }

    @Test
    public void testIsValidValueWithInvalidType() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType);

        assertFalse(arrayType.isValidValue("not a collection"));
        assertFalse(arrayType.isValidValue(123));
        assertFalse(arrayType.isValidValue(new Object()));
    }

    @Test
    public void testAreEqualValuesWithLists() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 0, 10, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        List<String> list1 = Arrays.asList("a", "b", "c");
        List<String> list2 = Arrays.asList("a", "b", "c");
        List<String> list3 = Arrays.asList("a", "c", "b"); // different order
        List<String> list4 = Arrays.asList("a", "b");

        assertTrue(arrayType.areEqualValues(list1, list2, null));
        assertFalse(arrayType.areEqualValues(list1, list3, null)); // order matters for lists
        assertFalse(arrayType.areEqualValues(list1, list4, null)); // different size
        assertTrue(arrayType.areEqualValues(null, null, null));
        assertFalse(arrayType.areEqualValues(list1, null, null));
    }

    @Test
    public void testAreEqualValuesWithSets() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 0, 10, AtlasStructDef.AtlasAttributeDef.Cardinality.SET);

        Set<String> set1 = new HashSet<>(Arrays.asList("a", "b", "c"));
        Set<String> set2 = new HashSet<>(Arrays.asList("c", "a", "b")); // different order, but sets
        Set<String> set3 = new HashSet<>(Arrays.asList("a", "b"));

        assertTrue(arrayType.areEqualValues(set1, set2, null)); // order doesn't matter for sets
        assertFalse(arrayType.areEqualValues(set1, set3, null)); // different size
    }

    @Test
    public void testAreEqualValuesWithArrays() {
        AtlasArrayType arrayType = new AtlasArrayType(intType, 0, 10, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        int[] array1 = {1, 2, 3};
        int[] array2 = {1, 2, 3};
        int[] array3 = {1, 2, 3, 4};

        assertTrue(arrayType.areEqualValues(array1, array2, null));
        assertFalse(arrayType.areEqualValues(array1, array3, null));
    }

    @Test
    public void testGetNormalizedValueWithList() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        List<String> inputList = Arrays.asList("test1", "test2");
        Collection<?> normalized = arrayType.getNormalizedValue(inputList);

        assertNotNull(normalized);
        assertEquals(normalized.size(), 2);
        assertTrue(normalized.contains("test1"));
        assertTrue(normalized.contains("test2"));
    }

    @Test
    public void testGetNormalizedValueWithJsonString() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        String jsonString = "[\"value1\", \"value2\"]";
        Collection<?> normalized = arrayType.getNormalizedValue(jsonString);

        assertNotNull(normalized);
        assertEquals(normalized.size(), 2);
    }

    @Test
    public void testGetNormalizedValueWithArray() {
        AtlasArrayType arrayType = new AtlasArrayType(intType, 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        int[] inputArray = {1, 2, 3};
        Collection<?> normalized = arrayType.getNormalizedValue(inputArray);

        assertNotNull(normalized);
        assertEquals(normalized.size(), 3);
        assertTrue(normalized.contains(1));
        assertTrue(normalized.contains(2));
        assertTrue(normalized.contains(3));
    }

    @Test
    public void testGetNormalizedValueWithInvalidCount() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 2, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        List<String> tooSmall = Arrays.asList("single");
        assertNull(arrayType.getNormalizedValue(tooSmall));

        List<String> tooBig = Arrays.asList("a", "b", "c", "d");
        assertNull(arrayType.getNormalizedValue(tooBig));
    }

    @Test
    public void testGetNormalizedValueWithNullElements() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        List<String> listWithNull = Arrays.asList("valid", null, "also valid");
        Collection<?> normalized = arrayType.getNormalizedValue(listWithNull);

        assertNotNull(normalized);
        assertEquals(normalized.size(), 3);
        assertTrue(normalized.contains("valid"));
        assertTrue(normalized.contains(null));
        assertTrue(normalized.contains("also valid"));
    }

    @Test
    public void testGetNormalizedValueWithInvalidElement() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        // Create a mock element type that will return null for invalid values
        AtlasType mockElementType = new AtlasBuiltInTypes.AtlasStringType() {
            @Override
            public String getNormalizedValue(Object obj) {
                if ("invalid".equals(obj)) {
                    return null;
                }
                return super.getNormalizedValue(obj);
            }
        };

        AtlasArrayType arrayTypeWithMock = new AtlasArrayType(mockElementType, 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        List<String> listWithInvalid = Arrays.asList("valid", "invalid");
        assertNull(arrayTypeWithMock.getNormalizedValue(listWithInvalid));
    }

    @Test
    public void testValidateValue() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);
        List<String> messages = new ArrayList<>();

        // Valid case
        assertTrue(arrayType.validateValue(Arrays.asList("test1", "test2"), "testArray", messages));
        assertTrue(messages.isEmpty());

        // Invalid count
        messages.clear();
        assertFalse(arrayType.validateValue(Arrays.asList(), "testArray", messages));
        assertFalse(messages.isEmpty());
        assertTrue(messages.get(0).contains("incorrect number of values"));

        // Valid mixed types (will be normalized)
        messages.clear();
        List<Object> mixedList = Arrays.asList("valid", 123);
        assertTrue(arrayType.validateValue(mixedList, "testArray", messages));

        // Invalid type
        messages.clear();
        assertFalse(arrayType.validateValue("not an array", "testArray", messages));
        assertFalse(messages.isEmpty());
    }

    @Test
    public void testIsValidValueForUpdate() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        assertTrue(arrayType.isValidValueForUpdate(null));
        assertTrue(arrayType.isValidValueForUpdate(Arrays.asList("test1", "test2")));
        assertFalse(arrayType.isValidValueForUpdate(Arrays.asList())); // below minCount
        assertFalse(arrayType.isValidValueForUpdate("not an array"));
    }

    @Test
    public void testGetNormalizedValueForUpdate() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        List<String> inputList = Arrays.asList("test1", "test2");
        Collection<?> normalized = arrayType.getNormalizedValueForUpdate(inputList);

        assertNotNull(normalized);
        assertEquals(normalized.size(), 2);
    }

    @Test
    public void testValidateValueForUpdate() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);
        List<String> messages = new ArrayList<>();

        assertTrue(arrayType.validateValueForUpdate(Arrays.asList("test1", "test2"), "testArray", messages));
        assertTrue(messages.isEmpty());

        messages.clear();
        assertFalse(arrayType.validateValueForUpdate(Arrays.asList(), "testArray", messages));
        assertFalse(messages.isEmpty());
    }

    @Test
    public void testGetTypeForAttribute() {
        AtlasArrayType arrayType = new AtlasArrayType(stringType, 1, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);

        AtlasType typeForAttribute = arrayType.getTypeForAttribute();

        // If element type doesn't change, should return same instance
        assertEquals(typeForAttribute, arrayType);
    }

    @Test
    public void testGetTypeForAttributeWithChangedElementType() {
        // Create a mock element type that returns a different type for attribute
        AtlasType mockElementType = new AtlasBuiltInTypes.AtlasStringType() {
            @Override
            public AtlasType getTypeForAttribute() {
                return new AtlasBuiltInTypes.AtlasIntType(); // Return different type
            }
        };

        AtlasArrayType arrayType = new AtlasArrayType(mockElementType, 1, 3, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);
        AtlasType typeForAttribute = arrayType.getTypeForAttribute();

        // Should return new instance since element type changed
        assertNotEquals(typeForAttribute, arrayType);
        assertTrue(typeForAttribute instanceof AtlasArrayType);
    }

    @Test
    public void testResolveReferences() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(stringType);

        AtlasArrayType arrayType = new AtlasArrayType("string", 1, 5, AtlasStructDef.AtlasAttributeDef.Cardinality.LIST, mockTypeRegistry);

        assertEquals(arrayType.getElementType(), stringType);
    }
}
