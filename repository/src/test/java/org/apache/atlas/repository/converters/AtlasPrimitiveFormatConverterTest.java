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
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class AtlasPrimitiveFormatConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasType mockAtlasType;

    @Mock
    private AtlasFormatConverter.ConverterContext mockContext;

    private AtlasPrimitiveFormatConverter converter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new AtlasPrimitiveFormatConverter(mockConverterRegistry, mockTypeRegistry);
    }

    @Test
    public void testGetTypeCategory() {
        assertEquals(converter.getTypeCategory(), TypeCategory.PRIMITIVE);
    }

    @Test
    public void testFromV1ToV2WithString() throws AtlasBaseException {
        // Given
        String input = "testString";
        String normalizedValue = "normalizedTestString";
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithInteger() throws AtlasBaseException {
        // Given
        Integer input = 42;
        Integer normalizedValue = 42;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithBoolean() throws AtlasBaseException {
        // Given
        Boolean input = true;
        Boolean normalizedValue = true;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithDouble() throws AtlasBaseException {
        // Given
        Double input = 3.14;
        Double normalizedValue = 3.14;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithFloat() throws AtlasBaseException {
        // Given
        Float input = 2.71f;
        Float normalizedValue = 2.71f;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithLong() throws AtlasBaseException {
        // Given
        Long input = 12345L;
        Long normalizedValue = 12345L;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithByte() throws AtlasBaseException {
        // Given
        Byte input = (byte) 255;
        Byte normalizedValue = (byte) 255;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithShort() throws AtlasBaseException {
        // Given
        Short input = (short) 1000;
        Short normalizedValue = (short) 1000;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithNull() throws AtlasBaseException {
        // Given
        when(mockAtlasType.getNormalizedValue(null)).thenReturn(null);

        // When
        Object result = converter.fromV1ToV2(null, mockAtlasType, mockContext);

        // Then
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithString() {
        // Given
        String input = "testString";
        String normalizedValue = "normalizedTestString";
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithInteger() {
        // Given
        Integer input = 42;
        Integer normalizedValue = 42;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithBoolean() {
        // Given
        Boolean input = false;
        Boolean normalizedValue = false;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithDouble() {
        // Given
        Double input = 3.14159;
        Double normalizedValue = 3.14159;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithFloat() {
        // Given
        Float input = 2.718f;
        Float normalizedValue = 2.718f;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithLong() {
        // Given
        Long input = 9876543210L;
        Long normalizedValue = 9876543210L;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithByte() {
        // Given
        Byte input = (byte) 127;
        Byte normalizedValue = (byte) 127;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithShort() {
        // Given
        Short input = (short) 32767;
        Short normalizedValue = (short) 32767;
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithNull() {
        // Given
        when(mockAtlasType.getNormalizedValue(null)).thenReturn(null);

        // When
        Object result = converter.fromV2ToV1(null, mockAtlasType, mockContext);

        // Then
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithTypeNormalization() throws AtlasBaseException {
        // Given - test type normalization behavior
        String input = "123";
        Integer normalizedValue = 123; // Type normalizes string to integer
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV1ToV2(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV2ToV1WithTypeNormalization() {
        // Given - test type normalization behavior
        String input = "true";
        Boolean normalizedValue = true; // Type normalizes string to boolean
        when(mockAtlasType.getNormalizedValue(input)).thenReturn(normalizedValue);

        // When
        Object result = converter.fromV2ToV1(input, mockAtlasType, mockContext);

        // Then
        assertEquals(result, normalizedValue);
    }

    @Test
    public void testFromV1ToV2WithZeroValues() throws AtlasBaseException {
        // Given
        Integer zeroInput = 0;
        when(mockAtlasType.getNormalizedValue(zeroInput)).thenReturn(zeroInput);

        // When
        Object result = converter.fromV1ToV2(zeroInput, mockAtlasType, mockContext);

        // Then
        assertEquals(result, zeroInput);
    }

    @Test
    public void testFromV2ToV1WithZeroValues() {
        // Given
        Double zeroInput = 0.0;
        when(mockAtlasType.getNormalizedValue(zeroInput)).thenReturn(zeroInput);

        // When
        Object result = converter.fromV2ToV1(zeroInput, mockAtlasType, mockContext);

        // Then
        assertEquals(result, zeroInput);
    }

    @Test
    public void testFromV1ToV2WithEmptyString() throws AtlasBaseException {
        // Given
        String emptyString = "";
        when(mockAtlasType.getNormalizedValue(emptyString)).thenReturn(emptyString);

        // When
        Object result = converter.fromV1ToV2(emptyString, mockAtlasType, mockContext);

        // Then
        assertEquals(result, emptyString);
    }

    @Test
    public void testFromV2ToV1WithEmptyString() {
        // Given
        String emptyString = "";
        when(mockAtlasType.getNormalizedValue(emptyString)).thenReturn(emptyString);

        // When
        Object result = converter.fromV2ToV1(emptyString, mockAtlasType, mockContext);

        // Then
        assertEquals(result, emptyString);
    }
}
