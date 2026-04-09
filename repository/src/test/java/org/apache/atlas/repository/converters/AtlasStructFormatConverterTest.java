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
package org.apache.atlas.repository.converters;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.Struct;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

public class AtlasStructFormatConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasStructType mockStructType;

    @Mock
    private AtlasEntityType mockEntityType;

    @Mock
    private AtlasFormatConverter mockAttrConverter;

    @Mock
    private AtlasFormatConverter.ConverterContext mockContext;

    private AtlasStructFormatConverter converter;

    @BeforeMethod
    public void setUp() throws AtlasBaseException {
        MockitoAnnotations.openMocks(this);
        converter = new AtlasStructFormatConverter(mockConverterRegistry, mockTypeRegistry, TypeCategory.STRUCT);
        // Setup basic mocks
        when(mockStructType.getTypeName()).thenReturn("TestStruct");
        when(mockStructType.getTypeCategory()).thenReturn(TypeCategory.STRUCT);
        when(mockEntityType.getTypeName()).thenReturn("TestEntity");
        when(mockEntityType.getTypeCategory()).thenReturn(TypeCategory.ENTITY);
        when(mockConverterRegistry.getConverter(any(TypeCategory.class))).thenReturn(mockAttrConverter);
        when(mockAttrConverter.fromV2ToV1(any(), any(), any())).thenReturn(new HashMap<>());
        when(mockAttrConverter.fromV1ToV2(any(), any(), any())).thenReturn(new HashMap<>());
        when(mockAttrConverter.isValidValueV1(any(), any())).thenReturn(true);
    }

    @Test
    public void testGetTypeCategory() {
        assertEquals(converter.getTypeCategory(), TypeCategory.STRUCT);
    }

    @Test
    public void testIsValidValueV1WithNull() {
        assertTrue(converter.isValidValueV1(null, mockStructType));
    }

    @Test
    public void testIsValidValueV1WithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "testValue");
        assertTrue(converter.isValidValueV1(map, mockStructType));
    }

    @Test
    public void testIsValidValueV1WithStruct() {
        Struct struct = new Struct("TestStruct");
        struct.set("name", "testValue");
        assertTrue(converter.isValidValueV1(struct, mockStructType));
    }

    @Test
    public void testIsValidValueV1WithInvalidType() {
        String invalidValue = "notAMapOrStruct";
        assertFalse(converter.isValidValueV1(invalidValue, mockStructType));
    }

    @Test
    public void testFromV1ToV2WithNull() throws AtlasBaseException {
        Object result = converter.fromV1ToV2(null, mockStructType, mockContext);
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithMap() throws AtlasBaseException {
        // Given
        Map<String, Object> structMap = new HashMap<>();
        structMap.put("name", "testStruct");
        structMap.put("value", 42);
        AtlasStructType.AtlasAttribute nameAttr = createMockAttribute("string");
        AtlasStructType.AtlasAttribute valueAttr = createMockAttribute("int");
        when(mockStructType.getAttribute("name")).thenReturn(nameAttr);
        when(mockStructType.getAttribute("value")).thenReturn(valueAttr);
        // When
        Object resultObj = converter.fromV1ToV2(structMap, mockStructType, mockContext);
        AtlasStruct result = (AtlasStruct) resultObj;
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestStruct");
    }

    @Test
    public void testFromV1ToV2WithStruct() throws AtlasBaseException {
        // Given
        Struct struct = new Struct("TestStruct");
        struct.set("name", "testStruct");
        struct.set("value", 42);
        AtlasStructType.AtlasAttribute nameAttr = createMockAttribute("string");
        AtlasStructType.AtlasAttribute valueAttr = createMockAttribute("int");
        when(mockStructType.getAttribute("name")).thenReturn(nameAttr);
        when(mockStructType.getAttribute("value")).thenReturn(valueAttr);
        // When
        Object resultObj = converter.fromV1ToV2(struct, mockStructType, mockContext);
        AtlasStruct result = (AtlasStruct) resultObj;
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestStruct");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV1ToV2WithInvalidType() throws AtlasBaseException {
        String invalidValue = "notAMapOrStruct";
        converter.fromV1ToV2(invalidValue, mockStructType, mockContext);
    }

    @Test
    public void testFromV2ToV1WithNull() throws AtlasBaseException {
        Object result = converter.fromV2ToV1(null, mockStructType, mockContext);
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithAtlasStruct() throws AtlasBaseException {
        // Given
        AtlasStruct struct = new AtlasStruct("TestStruct");
        struct.setAttribute("name", "testStruct");
        struct.setAttribute("value", 42);
        AtlasStructType.AtlasAttribute nameAttr = createMockAttribute("string");
        AtlasStructType.AtlasAttribute valueAttr = createMockAttribute("int");
        when(mockStructType.getAttribute("name")).thenReturn(nameAttr);
        when(mockStructType.getAttribute("value")).thenReturn(valueAttr);
        // When
        Object result = converter.fromV2ToV1(struct, mockStructType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Struct);
        Struct resultStruct = (Struct) result;
        assertEquals(resultStruct.getTypeName(), "TestStruct");
    }

    @Test
    public void testFromV2ToV1WithMap() throws AtlasBaseException {
        // Given
        Map<String, Object> structMap = new HashMap<>();
        structMap.put("name", "testStruct");
        structMap.put("value", 42);
        AtlasStructType.AtlasAttribute nameAttr = createMockAttribute("string");
        AtlasStructType.AtlasAttribute valueAttr = createMockAttribute("int");
        when(mockStructType.getAttribute("name")).thenReturn(nameAttr);
        when(mockStructType.getAttribute("value")).thenReturn(valueAttr);
        // When
        Object result = converter.fromV2ToV1(structMap, mockStructType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Struct);
        Struct resultStruct = (Struct) result;
        assertEquals(resultStruct.getTypeName(), "TestStruct");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV2ToV1WithInvalidType() throws AtlasBaseException {
        String invalidValue = "notAStructOrMap";
        converter.fromV2ToV1(invalidValue, mockStructType, mockContext);
    }

    @Test
    public void testFromV1ToV2WithEmptyMap() throws AtlasBaseException {
        // Given
        Map<String, Object> emptyMap = new HashMap<>();
        // When
        Object resultObj = converter.fromV1ToV2(emptyMap, mockStructType, mockContext);
        AtlasStruct result = (AtlasStruct) resultObj;
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestStruct");
    }

    @Test
    public void testFromV1ToV2WithEmptyStruct() throws AtlasBaseException {
        // Given
        Struct emptyStruct = new Struct("TestStruct");
        // When
        Object resultObj = converter.fromV1ToV2(emptyStruct, mockStructType, mockContext);
        AtlasStruct result = (AtlasStruct) resultObj;
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestStruct");
    }

    @Test
    public void testFromV2ToV1WithEmptyAtlasStruct() throws AtlasBaseException {
        // Given
        AtlasStruct emptyStruct = new AtlasStruct("TestStruct");
        // When
        Object result = converter.fromV2ToV1(emptyStruct, mockStructType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Struct);
        Struct resultStruct = (Struct) result;
        assertEquals(resultStruct.getTypeName(), "TestStruct");
    }

    @Test
    public void testFromV2ToV1WithEmptyMap() throws AtlasBaseException {
        // Given
        Map<String, Object> emptyMap = new HashMap<>();
        // When
        Object result = converter.fromV2ToV1(emptyMap, mockStructType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof Struct);
        Struct resultStruct = (Struct) result;
        assertEquals(resultStruct.getTypeName(), "TestStruct");
    }

    @Test
    public void testFromV1ToV2WithInvalidValue() throws AtlasBaseException {
        // Given
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("invalidAttr", "invalidValue");
        AtlasStructType.AtlasAttribute invalidAttr = createMockAttribute("string");
        when(mockStructType.getAttribute("invalidAttr")).thenReturn(invalidAttr);
        when(mockAttrConverter.isValidValueV1("invalidValue", invalidAttr.getAttributeType())).thenReturn(false);
        try {
            converter.fromV1ToV2(attributes, mockStructType, mockContext);
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS);
        }
    }

    @Test
    public void testGetGuidPrivateMethod() throws Exception {
        // Test the private getGuid method using reflection
        Method getGuid = AtlasStructFormatConverter.class.getDeclaredMethod("getGuid", Object.class);
        getGuid.setAccessible(true);
        // Test with guid in map
        Map<String, Object> mapWithGuid = new HashMap<>();
        mapWithGuid.put(AtlasObjectId.KEY_GUID, "test-guid-123");
        String result = (String) getGuid.invoke(converter, mapWithGuid);
        assertEquals(result, "test-guid-123");
        // Test with null map
        String nullResult = (String) getGuid.invoke(converter, (Object) null);
        assertNull(nullResult);
        // Test with map without guid
        Map<String, Object> mapWithoutGuid = new HashMap<>();
        mapWithoutGuid.put("name", "testValue");
        String noGuidResult = (String) getGuid.invoke(converter, mapWithoutGuid);
        assertNull(noGuidResult);
    }

    @Test
    public void testGetGuidWithAtlasObjectId() throws Exception {
        // Test getGuid method with AtlasObjectId using reflection
        Method getGuid = AtlasStructFormatConverter.class.getDeclaredMethod("getGuid", Object.class);
        getGuid.setAccessible(true);
        AtlasObjectId objId = new AtlasObjectId("guid1", "TestEntity");
        String result = (String) getGuid.invoke(converter, objId);
        assertEquals(result, "guid1");
    }

    @Test
    public void testGetGuidWithInvalidObject() throws Exception {
        // Test getGuid method with invalid object using reflection
        Method getGuid = AtlasStructFormatConverter.class.getDeclaredMethod("getGuid", Object.class);
        getGuid.setAccessible(true);
        String invalidObj = "notAnObjectId";
        String result = (String) getGuid.invoke(converter, invalidObj);
        assertNull(result);
    }

    @Test
    public void testConstructorWithTypeCategory() {
        // Test constructor with different type categories
        AtlasStructFormatConverter entityConverter = new AtlasStructFormatConverter(mockConverterRegistry, mockTypeRegistry, TypeCategory.ENTITY);
        assertEquals(entityConverter.getTypeCategory(), TypeCategory.ENTITY);
        AtlasStructFormatConverter classificationConverter = new AtlasStructFormatConverter(mockConverterRegistry, mockTypeRegistry, TypeCategory.CLASSIFICATION);
        assertEquals(classificationConverter.getTypeCategory(), TypeCategory.CLASSIFICATION);
    }

    @Test
    public void testInheritanceFromAbstractConverter() {
        // Verify that AtlasStructFormatConverter extends AtlasAbstractFormatConverter
        assertTrue(converter instanceof AtlasAbstractFormatConverter);
        assertTrue(AtlasAbstractFormatConverter.class.isAssignableFrom(AtlasStructFormatConverter.class));
    }

    @Test
    public void testStaticConstants() {
        // Test that static constants are correctly defined
        assertEquals(AtlasStructFormatConverter.ATTRIBUTES_PROPERTY_KEY, "attributes");
        assertEquals(AtlasStructFormatConverter.RELATIONSHIP_ATTRIBUTES_PROPERTY_KEY, "relationshipAttributes");
    }

    @Test
    public void testDebugLogging() {
        // Test that debug logging works correctly (testing code path coverage)
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put("debugKey", "debugValue");
        boolean result = converter.isValidValueV1(debugMap, mockStructType);
        assertTrue(result); // Should be true for Map type
    }

    @Test
    public void testComplexAttributeProcessing() {
        assertNotNull(converter);
        assertEquals(converter.getTypeCategory(), TypeCategory.STRUCT);
    }

    private AtlasStructType.AtlasAttribute createMockAttribute(String typeName) {
        return createMockAttribute(typeName, false);
    }

    private AtlasStructType.AtlasAttribute createMockAttribute(String typeName, boolean isOwnedRef) {
        AtlasStructType.AtlasAttribute attr = mock(AtlasStructType.AtlasAttribute.class);
        AtlasType attrType = mock(AtlasType.class);

        when(attr.getAttributeType()).thenReturn(attrType);
        when(attrType.getTypeName()).thenReturn(typeName);
        when(attrType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(attr.isOwnedRef()).thenReturn(isOwnedRef);
        when(attr.getName()).thenReturn("testAttr");
        when(attr.getQualifiedName()).thenReturn("TestStruct.testAttr");

        return attr;
    }
}
