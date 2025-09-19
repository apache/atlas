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
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit test class for AtlasMapFormatConverter.
 * Tests map format conversion functionality including validation and V1/V2 conversions.
 */
public class AtlasMapFormatConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasMapType mockMapType;

    @Mock
    private AtlasType mockKeyType;

    @Mock
    private AtlasType mockValueType;

    @Mock
    private AtlasFormatConverter mockKeyConverter;

    @Mock
    private AtlasFormatConverter mockValueConverter;

    @Mock
    private AtlasFormatConverter.ConverterContext mockContext;

    private AtlasMapFormatConverter converter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new AtlasMapFormatConverter(mockConverterRegistry, mockTypeRegistry);
    }

    @Test
    public void testGetTypeCategory() {
        assertEquals(converter.getTypeCategory(), TypeCategory.MAP);
    }

    @Test
    public void testIsValidValueV1WithNull() {
        // When
        boolean result = converter.isValidValueV1(null, mockMapType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithNonMapType() {
        // Given
        AtlasType nonMapType = mockKeyType;
        Map<String, String> testMap = new HashMap<>();
        // When
        boolean result = converter.isValidValueV1(testMap, nonMapType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithNonMapObject() {
        // Given
        String nonMapObject = "notAMap";
        // When
        boolean result = converter.isValidValueV1(nonMapObject, mockMapType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithValidMap() throws AtlasBaseException {
        // Given
        Map<String, Integer> testMap = new HashMap<>();
        testMap.put("key1", 1);
        testMap.put("key2", 2);
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.isValidValueV1(any(), eq(mockKeyType))).thenReturn(true);
        when(mockValueConverter.isValidValueV1(any(), eq(mockValueType))).thenReturn(true);
        // When
        boolean result = converter.isValidValueV1(testMap, mockMapType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithInvalidKey() throws AtlasBaseException {
        // Given
        Map<String, Integer> testMap = new HashMap<>();
        testMap.put("invalidKey", 1);
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.isValidValueV1(eq("invalidKey"), eq(mockKeyType))).thenReturn(false);
        // When
        boolean result = converter.isValidValueV1(testMap, mockMapType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithInvalidValue() throws AtlasBaseException {
        // Given
        Map<String, Integer> testMap = new HashMap<>();
        testMap.put("key1", 999);
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.isValidValueV1(eq("key1"), eq(mockKeyType))).thenReturn(true);
        when(mockValueConverter.isValidValueV1(eq(999), eq(mockValueType))).thenReturn(false);
        // When
        boolean result = converter.isValidValueV1(testMap, mockMapType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithConverterException() throws AtlasBaseException {
        // Given
        Map<String, Integer> testMap = new HashMap<>();
        testMap.put("key1", 1);
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenThrow(new AtlasBaseException("Converter not found"));
        // When
        boolean result = converter.isValidValueV1(testMap, mockMapType);
        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithEmptyMap() throws AtlasBaseException {
        // Given
        Map<String, Integer> emptyMap = new HashMap<>();
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        // When
        boolean result = converter.isValidValueV1(emptyMap, mockMapType);
        // Then
        assertTrue(result);
    }

    @Test
    public void testFromV1ToV2WithNull() throws AtlasBaseException {
        // When
        Map<?, ?> result = converter.fromV1ToV2(null, mockMapType, mockContext);
        // Then
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithValidMap() throws AtlasBaseException {
        // Given
        Map<String, Integer> inputMap = new HashMap<>();
        inputMap.put("key1", 1);
        inputMap.put("key2", 2);
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.fromV1ToV2(eq("key1"), eq(mockKeyType), eq(mockContext))).thenReturn("convertedKey1");
        when(mockKeyConverter.fromV1ToV2(eq("key2"), eq(mockKeyType), eq(mockContext))).thenReturn("convertedKey2");
        when(mockValueConverter.fromV1ToV2(eq(1), eq(mockValueType), eq(mockContext))).thenReturn("convertedValue1");
        when(mockValueConverter.fromV1ToV2(eq(2), eq(mockValueType), eq(mockContext))).thenReturn("convertedValue2");
        // When
        Map<?, ?> result = converter.fromV1ToV2(inputMap, mockMapType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get("convertedKey1"), "convertedValue1");
        assertEquals(result.get("convertedKey2"), "convertedValue2");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV1ToV2WithNonMapObject() throws AtlasBaseException {
        // Given
        String nonMapObject = "notAMap";
        // When/Then
        converter.fromV1ToV2(nonMapObject, mockMapType, mockContext);
    }

    @Test
    public void testFromV1ToV2WithEmptyMap() throws AtlasBaseException {
        // Given
        Map<String, Integer> emptyMap = new HashMap<>();
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        // When
        Map<?, ?> result = converter.fromV1ToV2(emptyMap, mockMapType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFromV2ToV1WithNull() throws AtlasBaseException {
        // When
        Map<?, ?> result = converter.fromV2ToV1(null, mockMapType, mockContext);
        // Then
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithValidMap() throws AtlasBaseException {
        // Given
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put("key1", "value1");
        inputMap.put("key2", "value2");
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.fromV2ToV1(eq("key1"), eq(mockKeyType), eq(mockContext))).thenReturn("convertedKey1");
        when(mockKeyConverter.fromV2ToV1(eq("key2"), eq(mockKeyType), eq(mockContext))).thenReturn("convertedKey2");
        when(mockValueConverter.fromV2ToV1(eq("value1"), eq(mockValueType), eq(mockContext))).thenReturn("convertedValue1");
        when(mockValueConverter.fromV2ToV1(eq("value2"), eq(mockValueType), eq(mockContext))).thenReturn("convertedValue2");
        // When
        Map<?, ?> result = converter.fromV2ToV1(inputMap, mockMapType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get("convertedKey1"), "convertedValue1");
        assertEquals(result.get("convertedKey2"), "convertedValue2");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV2ToV1WithNonMapObject() throws AtlasBaseException {
        // Given
        String nonMapObject = "notAMap";
        // When/Then
        converter.fromV2ToV1(nonMapObject, mockMapType, mockContext);
    }

    @Test
    public void testFromV2ToV1WithEmptyMap() throws AtlasBaseException {
        // Given
        Map<String, String> emptyMap = new HashMap<>();
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        // When
        Map<?, ?> result = converter.fromV2ToV1(emptyMap, mockMapType, mockContext);
        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFromV1ToV2WithNullValues() throws AtlasBaseException {
        // Given
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("key1", null);
        inputMap.put("key2", "value2");
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.fromV1ToV2(eq("key1"), eq(mockKeyType), eq(mockContext))).thenReturn("convertedKey1");
        when(mockKeyConverter.fromV1ToV2(eq("key2"), eq(mockKeyType), eq(mockContext))).thenReturn("convertedKey2");
        when(mockValueConverter.fromV1ToV2(eq(null), eq(mockValueType), eq(mockContext))).thenReturn(null);
        when(mockValueConverter.fromV1ToV2(eq("value2"), eq(mockValueType), eq(mockContext))).thenReturn("convertedValue2");
        // When
        Map<?, ?> result = converter.fromV1ToV2(inputMap, mockMapType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertNull(result.get("convertedKey1"));
        assertEquals(result.get("convertedKey2"), "convertedValue2");
    }

    @Test
    public void testFromV2ToV1WithNullValues() throws AtlasBaseException {
        // Given
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("key1", null);
        inputMap.put("key2", "value2");
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.fromV2ToV1(eq("key1"), eq(mockKeyType), eq(mockContext))).thenReturn("convertedKey1");
        when(mockKeyConverter.fromV2ToV1(eq("key2"), eq(mockKeyType), eq(mockContext))).thenReturn("convertedKey2");
        when(mockValueConverter.fromV2ToV1(eq(null), eq(mockValueType), eq(mockContext))).thenReturn(null);
        when(mockValueConverter.fromV2ToV1(eq("value2"), eq(mockValueType), eq(mockContext))).thenReturn("convertedValue2");
        // When
        Map<?, ?> result = converter.fromV2ToV1(inputMap, mockMapType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertNull(result.get("convertedKey1"));
        assertEquals(result.get("convertedKey2"), "convertedValue2");
    }

    @Test
    public void testFromV1ToV2WithComplexTypes() throws AtlasBaseException {
        // Given
        Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put(new Object(), new Object());
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.fromV1ToV2(any(), eq(mockKeyType), eq(mockContext))).thenReturn("complexKey");
        when(mockValueConverter.fromV1ToV2(any(), eq(mockValueType), eq(mockContext))).thenReturn("complexValue");
        // When
        Map<?, ?> result = converter.fromV1ToV2(inputMap, mockMapType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.size(), 1);
        assertEquals(result.get("complexKey"), "complexValue");
    }

    @Test
    public void testFromV2ToV1WithComplexTypes() throws AtlasBaseException {
        // Given
        Map<Object, Object> inputMap = new HashMap<>();
        inputMap.put(new Object(), new Object());
        when(mockMapType.getKeyType()).thenReturn(mockKeyType);
        when(mockMapType.getValueType()).thenReturn(mockValueType);
        when(mockKeyType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockValueType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockKeyConverter).thenReturn(mockValueConverter);
        when(mockKeyConverter.fromV2ToV1(any(), eq(mockKeyType), eq(mockContext))).thenReturn("complexKey");
        when(mockValueConverter.fromV2ToV1(any(), eq(mockValueType), eq(mockContext))).thenReturn("complexValue");
        // When
        Map<?, ?> result = converter.fromV2ToV1(inputMap, mockMapType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.size(), 1);
        assertEquals(result.get("complexKey"), "complexValue");
    }
}
