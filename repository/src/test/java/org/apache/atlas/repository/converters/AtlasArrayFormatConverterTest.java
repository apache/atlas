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
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasArrayFormatConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasArrayType mockArrayType;

    @Mock
    private AtlasType mockElementType;

    @Mock
    private AtlasFormatConverter mockElementConverter;

    @Mock
    private AtlasFormatConverter.ConverterContext mockContext;

    private AtlasArrayFormatConverter converter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new AtlasArrayFormatConverter(mockConverterRegistry, mockTypeRegistry);
    }

    @Test
    public void testGetTypeCategory() {
        assertEquals(converter.getTypeCategory(), TypeCategory.ARRAY);
    }

    @Test
    public void testIsValidValueV1WithNull() {
        // When
        boolean result = converter.isValidValueV1(null, mockArrayType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithNonArrayType() {
        // Given
        AtlasType nonArrayType = mockElementType;

        // When
        boolean result = converter.isValidValueV1(new ArrayList<>(), nonArrayType);

        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithValidCollection() throws AtlasBaseException {
        // Given
        List<String> testList = new ArrayList<>();
        testList.add("item1");
        testList.add("item2");

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);
        when(mockElementConverter.isValidValueV1(any(), eq(mockElementType))).thenReturn(true);

        // When
        boolean result = converter.isValidValueV1(testList, mockArrayType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithInvalidCollectionElement() throws AtlasBaseException {
        // Given
        List<String> testList = new ArrayList<>();
        testList.add("item1");
        testList.add("invalidItem");

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);
        when(mockElementConverter.isValidValueV1(eq("item1"), eq(mockElementType))).thenReturn(true);
        when(mockElementConverter.isValidValueV1(eq("invalidItem"), eq(mockElementType))).thenReturn(false);

        // When
        boolean result = converter.isValidValueV1(testList, mockArrayType);

        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithSingleElement() throws AtlasBaseException {
        // Given
        String singleElement = "singleItem";

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);
        when(mockElementConverter.isValidValueV1(eq(singleElement), eq(mockElementType))).thenReturn(true);

        // When
        boolean result = converter.isValidValueV1(singleElement, mockArrayType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithConverterException() throws AtlasBaseException {
        // Given
        List<String> testList = new ArrayList<>();
        testList.add("item1");

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenThrow(new AtlasBaseException("Converter not found"));

        // When
        boolean result = converter.isValidValueV1(testList, mockArrayType);

        // Then
        assertFalse(result);
    }

    @Test
    public void testFromV1ToV2WithNull() throws AtlasBaseException {
        // When
        Collection<Object> result = converter.fromV1ToV2(null, mockArrayType, mockContext);

        // Then
        assertNull(result);
    }

    @Test
    public void testFromV1ToV2WithSet() throws AtlasBaseException {
        // Given
        Set<String> inputSet = new LinkedHashSet<>();
        inputSet.add("item1");
        inputSet.add("item2");

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);
        when(mockElementConverter.fromV1ToV2(eq("item1"), eq(mockElementType), eq(mockContext))).thenReturn("converted1");
        when(mockElementConverter.fromV1ToV2(eq("item2"), eq(mockElementType), eq(mockContext))).thenReturn("converted2");

        // When
        Collection<Object> result = converter.fromV1ToV2(inputSet, mockArrayType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof LinkedHashSet);
        assertEquals(result.size(), 2);
        assertTrue(result.contains("converted1"));
        assertTrue(result.contains("converted2"));
    }

    @Test
    public void testFromV1ToV2WithList() throws AtlasBaseException {
        // Given
        List<String> inputList = new ArrayList<>();
        inputList.add("item1");
        inputList.add("item2");

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);
        when(mockElementConverter.fromV1ToV2(eq("item1"), eq(mockElementType), eq(mockContext))).thenReturn("converted1");
        when(mockElementConverter.fromV1ToV2(eq("item2"), eq(mockElementType), eq(mockContext))).thenReturn("converted2");

        // When
        Collection<Object> result = converter.fromV1ToV2(inputList, mockArrayType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof ArrayList);
        assertEquals(result.size(), 2);
        assertTrue(result.contains("converted1"));
        assertTrue(result.contains("converted2"));
    }

    @Test
    public void testFromV1ToV2WithSingleElement() throws AtlasBaseException {
        // Given
        String singleElement = "singleItem";

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);
        when(mockElementConverter.fromV1ToV2(eq(singleElement), eq(mockElementType), eq(mockContext))).thenReturn("converted");

        // When
        Collection<Object> result = converter.fromV1ToV2(singleElement, mockArrayType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof ArrayList);
        assertEquals(result.size(), 1);
        assertTrue(result.contains("converted"));
    }

    @Test
    public void testFromV2ToV1WithNull() throws AtlasBaseException {
        // When
        Collection<Object> result = converter.fromV2ToV1(null, mockArrayType, mockContext);

        // Then
        assertNull(result);
    }

    @Test
    public void testFromV2ToV1WithList() throws AtlasBaseException {
        // Given
        List<String> inputList = new ArrayList<>();
        inputList.add("item1");
        inputList.add("item2");

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);
        when(mockElementConverter.fromV2ToV1(eq("item1"), eq(mockElementType), eq(mockContext))).thenReturn("converted1");
        when(mockElementConverter.fromV2ToV1(eq("item2"), eq(mockElementType), eq(mockContext))).thenReturn("converted2");

        // When
        Collection<Object> result = converter.fromV2ToV1(inputList, mockArrayType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof ArrayList);
        assertEquals(result.size(), 2);
        assertTrue(result.contains("converted1"));
        assertTrue(result.contains("converted2"));
    }

    @Test
    public void testFromV2ToV1WithSet() throws AtlasBaseException {
        // Given
        Set<String> inputSet = new LinkedHashSet<>();
        inputSet.add("item1");
        inputSet.add("item2");

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);
        when(mockElementConverter.fromV2ToV1(eq("item1"), eq(mockElementType), eq(mockContext))).thenReturn("converted1");
        when(mockElementConverter.fromV2ToV1(eq("item2"), eq(mockElementType), eq(mockContext))).thenReturn("converted2");

        // When
        Collection<Object> result = converter.fromV2ToV1(inputSet, mockArrayType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result instanceof LinkedHashSet);
        assertEquals(result.size(), 2);
        assertTrue(result.contains("converted1"));
        assertTrue(result.contains("converted2"));
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV2ToV1WithInvalidType() throws AtlasBaseException {
        // Given
        String invalidInput = "notACollection";

        // When/Then
        converter.fromV2ToV1(invalidInput, mockArrayType, mockContext);
    }

    @Test
    public void testFromV1ToV2WithEmptyCollection() throws AtlasBaseException {
        // Given
        List<String> emptyList = new ArrayList<>();

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);

        // When
        Collection<Object> result = converter.fromV1ToV2(emptyList, mockArrayType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertTrue(result instanceof ArrayList);
    }

    @Test
    public void testFromV2ToV1WithEmptyCollection() throws AtlasBaseException {
        // Given
        Set<String> emptySet = new LinkedHashSet<>();

        when(mockArrayType.getElementType()).thenReturn(mockElementType);
        when(mockElementType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockConverterRegistry.getConverter(TypeCategory.PRIMITIVE)).thenReturn(mockElementConverter);

        // When
        Collection<Object> result = converter.fromV2ToV1(emptySet, mockArrayType, mockContext);

        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertTrue(result instanceof LinkedHashSet);
    }
}
