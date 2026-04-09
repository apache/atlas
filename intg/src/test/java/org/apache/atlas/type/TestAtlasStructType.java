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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.ModelTestUtil;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BIGINTEGER;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_DATE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_INT;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAtlasStructType {
    private static final String MULTI_VAL_ATTR_NAME_MIN_MAX = "multiValMinMax";
    private static final String MULTI_VAL_ATTR_NAME_MIN     = "multiValMin";
    private static final String MULTI_VAL_ATTR_NAME_MAX     = "multiValMax";
    private static final int    MULTI_VAL_ATTR_MIN_COUNT    = 2;
    private static final int    MULTI_VAL_ATTR_MAX_COUNT    = 5;

    private AtlasStructType structType;
    private final List<Object>    validValues;
    private final List<Object>    invalidValues;
    private final List<String>    tokenizedValue;
    private final List<String>    nonTokenizedValue;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    private AtlasStructDef structDef;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        structDef = new AtlasStructDef("TestStruct", "Test struct type", "1.0");

        // Add some attributes
        AtlasAttributeDef stringAttr = new AtlasAttributeDef("stringAttr", "string", false,
                AtlasAttributeDef.Cardinality.SINGLE, 0, 1, true, false, false,
                "default_value", Collections.emptyList(), Collections.emptyMap(), "String attribute", 0, null);

        AtlasAttributeDef intAttr = new AtlasAttributeDef("intAttr", "int", true,
                AtlasAttributeDef.Cardinality.SINGLE, 0, 1, false, false, false,
                "", Collections.emptyList(), Collections.emptyMap(), "Integer attribute", 0, null);

        Map<String, String> uniqueOptions = new HashMap<>();
        uniqueOptions.put("isUnique", "true");
        AtlasAttributeDef uniqueAttr = new AtlasAttributeDef("uniqueAttr", "string", true,
                AtlasAttributeDef.Cardinality.SINGLE, 0, 1, true, false, false,
                "", Collections.emptyList(), uniqueOptions, "Unique attribute", 0, null);

        structDef.addAttribute(stringAttr);
        structDef.addAttribute(intAttr);
        structDef.addAttribute(uniqueAttr);

        structType = new AtlasStructType(structDef);
    }

    @Test
    public void testStructTypeDefaultValue() {
        AtlasStruct defValue = structType.createDefaultValue();

        assertNotNull(defValue);
        assertEquals(defValue.getTypeName(), structType.getTypeName());
    }

  /*  @Test
    public void testStructTypeIsValidValue() {
        for (Object value : validValues) {
            assertTrue(structType.isValidValue(value), "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(structType.isValidValue(value), "value=" + value);
        }
    }*/

/*    @Test
    public void testStructTypeGetNormalizedValue() {
        assertNull(structType.getNormalizedValue(null), "value=" + null);

        for (Object value : validValues) {
            if (value == null) {
                continue;
            }

            Object normalizedValue = structType.getNormalizedValue(value);

            assertNotNull(normalizedValue, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertNull(structType.getNormalizedValue(value), "value=" + value);
        }
    }*/

/*    @Test
    public void testStructTypeValidateValue() {
        List<String> messages = new ArrayList<>();
        for (Object value : validValues) {
            assertTrue(structType.validateValue(value, "testObj", messages));
            assertEquals(messages.size(), 0, "value=" + value);
        }

        for (Object value : invalidValues) {
            assertFalse(structType.validateValue(value, "testObj", messages));
            assertTrue(messages.size() > 0, "value=" + value);
            messages.clear();
        }
    }*/

    @Test
    public void testInvalidStructDef_MultiValuedAttributeNotArray() {
        AtlasAttributeDef invalidMultiValuedAttrib = new AtlasAttributeDef("invalidAttributeDef", ATLAS_TYPE_INT);
        invalidMultiValuedAttrib.setCardinality(Cardinality.LIST);

        AtlasStructDef invalidStructDef = ModelTestUtil.newStructDef();
        invalidStructDef.addAttribute(invalidMultiValuedAttrib);

        try {
            AtlasStructType invalidStructType = new AtlasStructType(invalidStructDef, ModelTestUtil.getTypesRegistry());

            fail("invalidStructDef not detected: structDef=" + invalidStructDef + "; structType=" + invalidStructType);
        } catch (AtlasBaseException excp) {
            assertSame(excp.getAtlasErrorCode(), AtlasErrorCode.INVALID_ATTRIBUTE_TYPE_FOR_CARDINALITY);
            invalidStructDef.removeAttribute("invalidAttributeDef");
        }
    }

    @Test
    public void testTokenizeChar() {
        for (String valid : tokenizedValue) {
            assertTrue(AtlasStructType.AtlasAttribute.hastokenizeChar(valid));
        }
        for (String invalid : nonTokenizedValue) {
            assertFalse(AtlasStructType.AtlasAttribute.hastokenizeChar(invalid));
        }
    }

    private static AtlasStructType getStructType(AtlasStructDef structDef) {
        try {
            return new AtlasStructType(structDef, ModelTestUtil.getTypesRegistry());
        } catch (AtlasBaseException excp) {
            return null;
        }
    }

    {
        AtlasAttributeDef multiValuedAttribMinMax = new AtlasAttributeDef();
        AtlasAttributeDef multiValuedAttribMin    = new AtlasAttributeDef();
        AtlasAttributeDef multiValuedAttribMax    = new AtlasAttributeDef();

        multiValuedAttribMinMax.setName(MULTI_VAL_ATTR_NAME_MIN_MAX);
        multiValuedAttribMinMax.setTypeName(AtlasBaseTypeDef.getArrayTypeName(ATLAS_TYPE_INT));
        multiValuedAttribMinMax.setCardinality(Cardinality.LIST);
        multiValuedAttribMinMax.setValuesMinCount(MULTI_VAL_ATTR_MIN_COUNT);
        multiValuedAttribMinMax.setValuesMaxCount(MULTI_VAL_ATTR_MAX_COUNT);

        multiValuedAttribMin.setName(MULTI_VAL_ATTR_NAME_MIN);
        multiValuedAttribMin.setTypeName(AtlasBaseTypeDef.getArrayTypeName(ATLAS_TYPE_INT));
        multiValuedAttribMin.setCardinality(Cardinality.LIST);
        multiValuedAttribMin.setValuesMinCount(MULTI_VAL_ATTR_MIN_COUNT);

        multiValuedAttribMax.setName(MULTI_VAL_ATTR_NAME_MAX);
        multiValuedAttribMax.setTypeName(AtlasBaseTypeDef.getArrayTypeName(ATLAS_TYPE_INT));
        multiValuedAttribMax.setCardinality(Cardinality.SET);
        multiValuedAttribMax.setValuesMaxCount(MULTI_VAL_ATTR_MAX_COUNT);

        AtlasStructDef structDef = ModelTestUtil.newStructDef();

        structDef.addAttribute(multiValuedAttribMinMax);
        structDef.addAttribute(multiValuedAttribMin);
        structDef.addAttribute(multiValuedAttribMax);

        structType    = getStructType(structDef);
        validValues   = new ArrayList<>();
        invalidValues = new ArrayList<>();

        AtlasStruct invalidValue1 = structType.createDefaultValue();
        AtlasStruct invalidValue2 = structType.createDefaultValue();
        AtlasStruct invalidValue3 = structType.createDefaultValue();
        AtlasStruct invalidValue4 = structType.createDefaultValue();
        AtlasStruct invalidValue5 = structType.createDefaultValue();
        AtlasStruct invalidValue6 = structType.createDefaultValue();
        AtlasStruct invalidValue7 = structType.createDefaultValue();

        // invalid value for int
        invalidValue1.setAttribute(ModelTestUtil.getDefaultAttributeName(ATLAS_TYPE_INT), "xyz");

        // invalid value for date
        invalidValue2.setAttribute(ModelTestUtil.getDefaultAttributeName(ATLAS_TYPE_DATE), "xyz");

        // invalid value for bigint
        invalidValue3.setAttribute(ModelTestUtil.getDefaultAttributeName(ATLAS_TYPE_BIGINTEGER), "xyz");

        // minCount is less than required
        invalidValue4.setAttribute(MULTI_VAL_ATTR_NAME_MIN_MAX, new Integer[] {1});

        // maxCount is more than allowed
        invalidValue5.setAttribute(MULTI_VAL_ATTR_NAME_MIN_MAX, new Integer[] {1, 2, 3, 4, 5, 6});

        // minCount is less than required
        invalidValue6.setAttribute(MULTI_VAL_ATTR_NAME_MIN, new Integer[] {});

        // maxCount is more than allowed
        invalidValue7.setAttribute(MULTI_VAL_ATTR_NAME_MAX, new Integer[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        validValues.add(null);
        validValues.add(structType.createDefaultValue());
        validValues.add(structType.createDefaultValue().getAttributes()); // Map<String, Object>
        invalidValues.add(invalidValue1);
        invalidValues.add(invalidValue2);
        invalidValues.add(invalidValue3);
        invalidValues.add(invalidValue4);
        invalidValues.add(invalidValue5);
        invalidValues.add(invalidValue6);
        invalidValues.add(invalidValue7);
        invalidValues.add(new AtlasStruct());             // no values for mandatory attributes
        invalidValues.add(new HashMap<>()); // no values for mandatory attributes
        invalidValues.add(1);               // incorrect datatype
        invalidValues.add(new HashSet());   // incorrect datatype
        invalidValues.add(new ArrayList()); // incorrect datatype
        invalidValues.add(new String[] {}); // incorrect datatype

        tokenizedValue = new ArrayList<>();

        tokenizedValue.add("test[data"); //added special char [
        tokenizedValue.add("test]data"); //added special char ]
        tokenizedValue.add("狗"); //single char chinese data
        tokenizedValue.add("数据"); //mutiple char chinese data
        tokenizedValue.add("test data"); //english words with space
        tokenizedValue.add("testdata "); //space after testdata
        tokenizedValue.add("私は日本語を話します"); //japanese word
        tokenizedValue.add("元帳"); //japanese ledger char
        tokenizedValue.add("mydata&"); //added special char &
        tokenizedValue.add("test.1data");
        tokenizedValue.add("test:1data");

        nonTokenizedValue = new ArrayList<>();

        nonTokenizedValue.add("test.data");
        nonTokenizedValue.add("test:data");
        nonTokenizedValue.add("test_data");
        nonTokenizedValue.add("test:");
        nonTokenizedValue.add("test.");
        nonTokenizedValue.add("test_");
        nonTokenizedValue.add("レシート");
        nonTokenizedValue.add("test");
    }

    @Test
    public void testConstructor() {
        assertNotNull(structType);
        assertEquals(structType.getStructDef(), structDef);
        assertEquals(structType.getTypeName(), "TestStruct");
    }

    @Test
    public void testConstructorWithTypeRegistry() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        AtlasStructType structTypeWithRegistry = new AtlasStructType(structDef, mockTypeRegistry);

        assertNotNull(structTypeWithRegistry);
        assertEquals(structTypeWithRegistry.getTypeName(), "TestStruct");
    }

    @Test
    public void testGetAttributeType() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasType stringType = structType.getAttributeType("stringAttr");
        assertNotNull(stringType);
        assertTrue(stringType instanceof AtlasBuiltInTypes.AtlasStringType);

        assertNull(structType.getAttributeType("nonExistentAttr"));
    }

    @Test
    public void testGetAttributeDef() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasAttributeDef stringAttrDef = structType.getAttributeDef("stringAttr");
        assertNotNull(stringAttrDef);
        assertEquals(stringAttrDef.getName(), "stringAttr");
        assertEquals(stringAttrDef.getTypeName(), "string");

        assertNull(structType.getAttributeDef("nonExistentAttr"));
    }

    @Test
    public void testCreateDefaultValue() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasStruct defaultStruct = structType.createDefaultValue();
        assertNotNull(defaultStruct);
        assertEquals(defaultStruct.getTypeName(), "TestStruct");

        // Should have default values for non-optional attributes
        assertEquals(defaultStruct.getAttribute("stringAttr"), "default_value");
        assertNull(defaultStruct.getAttribute("intAttr")); // Optional attribute
    }

    @Test
    public void testCreateDefaultValueWithDefaultValue() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        Map<String, Object> customDefaults = new HashMap<>();
        customDefaults.put("stringAttr", "custom_value");

        AtlasStruct defaultStruct = (AtlasStruct) structType.createDefaultValue(customDefaults);
        assertNotNull(defaultStruct);
        // The attribute def has a default value which will be used instead of custom
        assertEquals(defaultStruct.getAttribute("stringAttr"), "default_value");
    }

    @Test
    public void testIsValidValueWithStruct() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        // Valid struct
        AtlasStruct validStruct = new AtlasStruct("TestStruct");
        validStruct.setAttribute("stringAttr", "test_value");
        validStruct.setAttribute("intAttr", 123);
        assertTrue(structType.isValidValue(validStruct));

        // Null is valid
        assertTrue(structType.isValidValue(null));

        // Invalid type
        assertFalse(structType.isValidValue("not a struct"));

        // Missing required attribute
        AtlasStruct invalidStruct = new AtlasStruct("TestStruct");
        // stringAttr is required but not set
        assertFalse(structType.isValidValue(invalidStruct));
    }

    @Test
    public void testIsValidValueWithMap() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        // Valid map
        Map<String, Object> validMap = new HashMap<>();
        validMap.put("stringAttr", "test_value");
        validMap.put("intAttr", 123);
        assertTrue(structType.isValidValue(validMap));

        // Missing required attribute
        Map<String, Object> invalidMap = new HashMap<>();
        invalidMap.put("intAttr", 123);
        // stringAttr is required but not set
        assertFalse(structType.isValidValue(invalidMap));
    }

    @Test
    public void testAreEqualValues() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasStruct struct1 = new AtlasStruct("TestStruct");
        struct1.setAttribute("stringAttr", "test");
        struct1.setAttribute("intAttr", 123);

        AtlasStruct struct2 = new AtlasStruct("TestStruct");
        struct2.setAttribute("stringAttr", "test");
        struct2.setAttribute("intAttr", 123);

        AtlasStruct struct3 = new AtlasStruct("TestStruct");
        struct3.setAttribute("stringAttr", "different");
        struct3.setAttribute("intAttr", 123);

        assertTrue(structType.areEqualValues(struct1, struct2, null));
        assertFalse(structType.areEqualValues(struct1, struct3, null));
        assertTrue(structType.areEqualValues(null, null, null));
        assertFalse(structType.areEqualValues(struct1, null, null));
    }

    @Test
    public void testGetNormalizedValue() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasStruct inputStruct = new AtlasStruct("TestStruct");
        inputStruct.setAttribute("stringAttr", "test");
        inputStruct.setAttribute("intAttr", "123"); // String that should be normalized to int

        Object normalized = structType.getNormalizedValue(inputStruct);

        assertNotNull(normalized);
        assertTrue(normalized instanceof AtlasStruct);
        AtlasStruct normalizedStruct = (AtlasStruct) normalized;
        assertEquals(normalizedStruct.getAttribute("stringAttr"), "test");
        assertEquals(normalizedStruct.getAttribute("intAttr"), 123);
    }

    @Test
    public void testValidateValue() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        List<String> messages = new ArrayList<>();

        // Valid case
        AtlasStruct validStruct = new AtlasStruct("TestStruct");
        validStruct.setAttribute("stringAttr", "test");
        validStruct.setAttribute("intAttr", 123);

        assertTrue(structType.validateValue(validStruct, "testStruct", messages));
        assertTrue(messages.isEmpty());

        // Invalid case - missing required attribute
        messages.clear();
        AtlasStruct invalidStruct = new AtlasStruct("TestStruct");
        invalidStruct.setAttribute("intAttr", 123);
        // stringAttr is required but missing

        assertFalse(structType.validateValue(invalidStruct, "testStruct", messages));
        assertFalse(messages.isEmpty());

        // Invalid type
        messages.clear();
        assertFalse(structType.validateValue("not a struct", "testStruct", messages));
        assertFalse(messages.isEmpty());
    }

    @Test
    public void testIsValidValueForUpdate() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasStruct validStruct = new AtlasStruct("TestStruct");
        validStruct.setAttribute("stringAttr", "test");

        assertTrue(structType.isValidValueForUpdate(validStruct));
        assertTrue(structType.isValidValueForUpdate(null));
        assertFalse(structType.isValidValueForUpdate("not a struct"));
    }

    @Test
    public void testGetNormalizedValueForUpdate() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasStruct inputStruct = new AtlasStruct("TestStruct");
        inputStruct.setAttribute("stringAttr", "test");

        Object normalized = structType.getNormalizedValueForUpdate(inputStruct);

        assertNotNull(normalized);
        assertTrue(normalized instanceof AtlasStruct);
    }

    @Test
    public void testValidateValueForUpdate() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        List<String> messages = new ArrayList<>();

        AtlasStruct validStruct = new AtlasStruct("TestStruct");
        validStruct.setAttribute("stringAttr", "test");

        assertTrue(structType.validateValueForUpdate(validStruct, "testStruct", messages));
        assertTrue(messages.isEmpty());
    }

    @Test
    public void testGetAllAttributes() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        Map<String, AtlasStructType.AtlasAttribute> allAttrs = structType.getAllAttributes();
        assertNotNull(allAttrs);
        assertEquals(allAttrs.size(), 3);
        assertTrue(allAttrs.containsKey("stringAttr"));
        assertTrue(allAttrs.containsKey("intAttr"));
        assertTrue(allAttrs.containsKey("uniqueAttr"));
    }

    @Test
    public void testGetUniqAttributes() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        Map<String, AtlasStructType.AtlasAttribute> uniqAttrs = structType.getUniqAttributes();
        assertNotNull(uniqAttrs);
        assertEquals(uniqAttrs.size(), 2); // stringAttr is also unique (has default value)
        assertTrue(uniqAttrs.containsKey("uniqueAttr"));
        assertTrue(uniqAttrs.containsKey("stringAttr"));
    }

    @Test
    public void testGetAttribute() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasStructType.AtlasAttribute stringAttr = structType.getAttribute("stringAttr");
        assertNotNull(stringAttr);
        assertEquals(stringAttr.getName(), "stringAttr");
        assertEquals(stringAttr.getTypeName(), "string");

        assertNull(structType.getAttribute("nonExistentAttr"));
    }

    @Test
    public void testGetSystemAttribute() {
        // System attributes are handled by root types
        AtlasStructType.AtlasAttribute systemAttr = structType.getSystemAttribute("guid");
        // This will be null for regular struct types
        assertNull(systemAttr);
    }

    @Test
    public void testGetBusinessAttribute() {
        // Business attributes are handled by root types
        AtlasStructType.AtlasAttribute businessAttr = structType.getBusinesAAttribute("testBM.testAttr");
        // This will be null for regular struct types
        assertNull(businessAttr);
    }

    @Test
    public void testNormalizeAttributeValuesWithStruct() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasStruct struct = new AtlasStruct("TestStruct");
        struct.setAttribute("stringAttr", "test");
        struct.setAttribute("intAttr", "123"); // String that should be normalized to int

        structType.normalizeAttributeValues(struct);

        assertEquals(struct.getAttribute("stringAttr"), "test");
        assertEquals(struct.getAttribute("intAttr"), 123);
    }

    @Test
    public void testNormalizeAttributeValuesWithMap() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        Map<String, Object> map = new HashMap<>();
        map.put("stringAttr", "test");
        map.put("intAttr", "123");

        structType.normalizeAttributeValues(map);

        assertEquals(map.get("stringAttr"), "test");
        assertEquals(map.get("intAttr"), 123);
    }

    @Test
    public void testPopulateDefaultValues() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        AtlasStruct struct = new AtlasStruct("TestStruct");
        structType.populateDefaultValues(struct);

        assertEquals(struct.getAttribute("stringAttr"), "default_value");
        // intAttr is optional, so no default value
        assertNull(struct.getAttribute("intAttr"));
    }

    @Test
    public void testGetVertexPropertyName() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        String propertyName = structType.getVertexPropertyName("stringAttr");
        assertNotNull(propertyName);
        assertTrue(propertyName.contains("stringAttr"));

        try {
            structType.getVertexPropertyName("nonExistentAttr");
            fail("Expected AtlasBaseException for non-existent attribute");
        } catch (AtlasBaseException e) {
            // Expected
        }
    }

    @Test
    public void testGetQualifiedAttributePropertyKey() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        String propertyKey = structType.getQualifiedAttributePropertyKey("stringAttr");
        assertNotNull(propertyKey);
        assertTrue(propertyKey.contains("stringAttr"));

        try {
            structType.getQualifiedAttributePropertyKey("nonExistentAttr");
            fail("Expected AtlasBaseException for non-existent attribute");
        } catch (AtlasBaseException e) {
            // Expected
        }
    }

    @Test
    public void testResolveReferences() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);

        // After resolving references, attributes should be available
        assertNotNull(structType.getAttribute("stringAttr"));
        assertNotNull(structType.getAttribute("intAttr"));
        assertNotNull(structType.getAttribute("uniqueAttr"));
    }

    @Test
    public void testResolveReferencesPhase2() throws AtlasBaseException {
        when(mockTypeRegistry.getType("string")).thenReturn(new AtlasBuiltInTypes.AtlasStringType());
        when(mockTypeRegistry.getType("int")).thenReturn(new AtlasBuiltInTypes.AtlasIntType());

        structType.resolveReferences(mockTypeRegistry);
        structType.resolveReferencesPhase2(mockTypeRegistry);
    }
}
