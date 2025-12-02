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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.type.AtlasEnumType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.typedef.EnumTypeDefinition.EnumValue;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasEnumFormatConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasEnumType mockEnumType;

    @Mock
    private AtlasType mockNonEnumType;

    @Mock
    private AtlasEnumDef mockEnumDef;

    @Mock
    private AtlasFormatConverter.ConverterContext mockContext;

    private AtlasEnumFormatConverter converter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new AtlasEnumFormatConverter(mockConverterRegistry, mockTypeRegistry);
        // Setup basic mocks
        when(mockEnumType.getEnumDef()).thenReturn(mockEnumDef);
        when(mockEnumDef.hasElement(anyString())).thenReturn(true);
        when(mockEnumType.getEnumElementDef(any(Number.class))).thenReturn(new AtlasEnumElementDef("TEST_VALUE", null, 0));
        when(mockEnumType.getEnumElementDef(anyString())).thenReturn(new AtlasEnumElementDef("TEST_VALUE", null, 0));
        when(mockNonEnumType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
    }

    @Test
    public void testGetTypeCategory() {
        assertEquals(converter.getTypeCategory(), TypeCategory.ENUM);
    }

    @Test
    public void testIsValidValueV1WithNull() {
        assertTrue(converter.isValidValueV1(null, mockEnumType));
    }

    @Test
    public void testIsValidValueV1WithNonEnumType() {
        boolean result = converter.isValidValueV1("someValue", mockNonEnumType);
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithEnumValueValid() {
        // Given
        EnumValue enumValue = new EnumValue("TEST_VALUE", 0);
        when(mockEnumDef.hasElement("TEST_VALUE")).thenReturn(true);
        // When
        boolean result = converter.isValidValueV1(enumValue, mockEnumType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithEnumValueInvalid() {
        // Given
        EnumValue enumValue = new EnumValue("INVALID_VALUE", 0);
        when(mockEnumDef.hasElement("INVALID_VALUE")).thenReturn(false);
        // When
        boolean result = converter.isValidValueV1(enumValue, mockEnumType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithMapValidValue() {
        // Given
        Map<String, Object> map = new HashMap<>();
        map.put("value", "TEST_VALUE");
        when(mockEnumDef.hasElement("TEST_VALUE")).thenReturn(true);
        // When
        boolean result = converter.isValidValueV1(map, mockEnumType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithMapValidOrdinal() {
        // Given
        Map<String, Object> map = new HashMap<>();
        map.put("ordinal", 0);
        // When
        boolean result = converter.isValidValueV1(map, mockEnumType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithMapInvalid() {
        // Given
        Map<String, Object> map = new HashMap<>();
        map.put("value", "INVALID_VALUE");
        when(mockEnumDef.hasElement("INVALID_VALUE")).thenReturn(false);
        // When
        boolean result = converter.isValidValueV1(map, mockEnumType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithNumberValid() {
        // When
        boolean result = converter.isValidValueV1(0, mockEnumType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithNumberInvalid() {
        // Given
        when(mockEnumType.getEnumElementDef(1)).thenReturn(null);
        // When
        boolean result = converter.isValidValueV1(1, mockEnumType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithStringValid() {
        // When
        boolean result = converter.isValidValueV1("TEST_VALUE", mockEnumType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithStringInvalid() {
        // Given
        when(mockEnumType.getEnumElementDef("INVALID_VALUE")).thenReturn(null);
        // When
        boolean result = converter.isValidValueV1("INVALID_VALUE", mockEnumType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testFromV1ToV2WithNull() throws AtlasBaseException {
        Object result = converter.fromV1ToV2(null, mockEnumType, mockContext);
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithNonEnumType() throws AtlasBaseException {
        Object result = converter.fromV1ToV2("someValue", mockNonEnumType, mockContext);
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithEnumValue() throws AtlasBaseException {
        // Given
        EnumValue enumValue = new EnumValue("TEST_VALUE", 1);
        // When
        Object result = converter.fromV1ToV2(enumValue, mockEnumType, mockContext);
        // Then
        assertEquals(result, "TEST_VALUE");
    }

    @Test
    public void testFromV1ToV2WithMapValue() throws AtlasBaseException {
        // Given
        Map<String, Object> map = new HashMap<>();
        map.put("value", "TEST_VALUE");
        // When
        Object result = converter.fromV1ToV2(map, mockEnumType, mockContext);
        // Then
        assertEquals(result, "TEST_VALUE");
    }

    @Test
    public void testFromV1ToV2WithMapOrdinal() throws AtlasBaseException {
        // Given
        Map<String, Object> map = new HashMap<>();
        map.put("ordinal", 0);
        // When
        Object result = converter.fromV1ToV2(map, mockEnumType, mockContext);
        // Then
        assertEquals(result, "TEST_VALUE");
    }

    @Test
    public void testFromV1ToV2WithNumber() throws AtlasBaseException {
        // When
        Object result = converter.fromV1ToV2(0, mockEnumType, mockContext);
        // Then
        assertEquals(result, "TEST_VALUE");
    }

    @Test
    public void testFromV1ToV2WithString() throws AtlasBaseException {
        // When
        Object result = converter.fromV1ToV2("TEST_VALUE", mockEnumType, mockContext);
        // Then
        assertEquals(result, "TEST_VALUE");
    }

    @Test
    public void testFromV1ToV2WithInvalidOrdinal() throws AtlasBaseException {
        // Given
        when(mockEnumType.getEnumElementDef(999)).thenReturn(null);
        // When
        Object result = converter.fromV1ToV2(999, mockEnumType, mockContext);
        // Then
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithNull() {
        Object result = converter.fromV2ToV1(null, mockEnumType, mockContext);
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithNonEnumType() {
        Object result = converter.fromV2ToV1("someValue", mockNonEnumType, mockContext);
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithValidValue() {
        // Given
        String v2Value = "TEST_VALUE";
        AtlasEnumElementDef elementDef = new AtlasEnumElementDef("TEST_VALUE", null, 1);
        when(mockEnumType.getEnumElementDef(v2Value)).thenReturn(elementDef);
        // When
        Object result = converter.fromV2ToV1(v2Value, mockEnumType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result instanceof EnumValue);
        EnumValue enumValue = (EnumValue) result;
        assertEquals(enumValue.getValue(), "TEST_VALUE");
        assertEquals((Object) enumValue.getOrdinal(), 1);
    }

    @Test
    public void testFromV2ToV1WithInvalidValue() {
        // Given
        String v2Value = "INVALID_VALUE";
        when(mockEnumType.getEnumElementDef(v2Value)).thenReturn(null);
        // When
        Object result = converter.fromV2ToV1(v2Value, mockEnumType, mockContext);
        // Then
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithMapEmptyValue() throws AtlasBaseException {
        // Given - map without value or ordinal
        Map<String, Object> map = new HashMap<>();
        map.put("other", "someOtherValue");
        Object result = converter.fromV1ToV2(map, mockEnumType, mockContext);
        // Then
        assertEquals(result, "TEST_VALUE");
    }

    @Test
    public void testIsValidValueV1WithMapEmptyValue() {
        // Given - map without value or ordinal
        Map<String, Object> map = new HashMap<>();
        map.put("other", "someOtherValue");
        // When
        boolean result = converter.isValidValueV1(map, mockEnumType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testFromV1ToV2WithFloatNumber() throws AtlasBaseException {
        // Given
        Float floatValue = 0.0f;
        // When
        Object result = converter.fromV1ToV2(floatValue, mockEnumType, mockContext);
        // Then
        assertEquals(result, "TEST_VALUE");
    }

    @Test
    public void testFromV1ToV2WithDoubleNumber() throws AtlasBaseException {
        // Given
        Double doubleValue = 0.0;
        // When
        Object result = converter.fromV1ToV2(doubleValue, mockEnumType, mockContext);
        // Then
        assertEquals(result, "TEST_VALUE");
    }

    @Test
    public void testIsValidValueV1WithFloatNumber() {
        // Given
        Float floatValue = 0.0f;
        // When
        boolean result = converter.isValidValueV1(floatValue, mockEnumType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testFromV1ToV2WithInvalidString() throws AtlasBaseException {
        // Given
        when(mockEnumType.getEnumElementDef("INVALID_STRING")).thenReturn(null);
        // When
        Object result = converter.fromV1ToV2("INVALID_STRING", mockEnumType, mockContext);
        // Then
        assertNull(result);
    }

    @Test
    public void testConstructorInheritance() {
        // Verify the constructor properly calls parent with correct parameters
        AtlasEnumFormatConverter testConverter = new AtlasEnumFormatConverter(mockConverterRegistry, mockTypeRegistry);
        assertNotNull(testConverter);
        assertEquals(testConverter.getTypeCategory(), TypeCategory.ENUM);
        assertTrue(testConverter instanceof AtlasAbstractFormatConverter);
    }
}
