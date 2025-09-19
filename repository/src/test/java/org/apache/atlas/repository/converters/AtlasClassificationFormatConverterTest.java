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
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.Struct;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasClassificationFormatConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasClassificationType mockClassificationType;

    @Mock
    private AtlasType mockNonClassificationType;

    @Mock
    private AtlasFormatConverter.ConverterContext mockContext;

    private AtlasClassificationFormatConverter converter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new AtlasClassificationFormatConverter(mockConverterRegistry, mockTypeRegistry);
        // Setup basic mocks
        when(mockClassificationType.getTypeName()).thenReturn("TestClassification");
        when(mockClassificationType.getTypeCategory()).thenReturn(TypeCategory.CLASSIFICATION);
        when(mockNonClassificationType.getTypeCategory()).thenReturn(TypeCategory.PRIMITIVE);
        when(mockNonClassificationType.getTypeName()).thenReturn("NonClassificationType");
    }

    @Test
    public void testGetTypeCategory() {
        assertEquals(converter.getTypeCategory(), TypeCategory.CLASSIFICATION);
    }

    @Test
    public void testFromV1ToV2WithNull() throws AtlasBaseException {
        // When
        AtlasClassification result = converter.fromV1ToV2(null, mockClassificationType, mockContext);
        // Then
        assertNull(result);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testFromV1ToV2WithInvalidType() throws AtlasBaseException {
        // Given
        String invalidObject = "notAMapOrStruct";
        // When/Then
        converter.fromV1ToV2(invalidObject, mockClassificationType, mockContext);
    }

    @Test
    public void testFromV1ToV2WithMapEmptyAttributes() throws AtlasBaseException {
        // Given
        Map<String, Object> v1Map = new HashMap<>();
        v1Map.put("attributes", new HashMap<>());
        // When
        AtlasClassification result = converter.fromV1ToV2(v1Map, mockClassificationType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestClassification");
    }

    @Test
    public void testFromV1ToV2WithMapNoAttributes() throws AtlasBaseException {
        // Given
        Map<String, Object> v1Map = new HashMap<>();
        // No attributes key
        // When
        AtlasClassification result = converter.fromV1ToV2(v1Map, mockClassificationType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestClassification");
    }

    @Test
    public void testFromV1ToV2WithMapNullAttributes() throws AtlasBaseException {
        // Given
        Map<String, Object> v1Map = new HashMap<>();
        v1Map.put("attributes", null);
        // When
        AtlasClassification result = converter.fromV1ToV2(v1Map, mockClassificationType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestClassification");
    }

    @Test
    public void testFromV1ToV2WithStructEmpty() throws AtlasBaseException {
        // Given
        Struct struct = new Struct("TestClassification");
        // When
        AtlasClassification result = converter.fromV1ToV2(struct, mockClassificationType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestClassification");
    }

    @Test
    public void testFromV1ToV2WithStructEmptyValues() throws AtlasBaseException {
        // Given
        Struct struct = new Struct("TestClassification");
        struct.setValues(new HashMap<>());
        // When
        AtlasClassification result = converter.fromV1ToV2(struct, mockClassificationType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestClassification");
    }

    @Test
    public void testFromV1ToV2WithStructNullValues() throws AtlasBaseException {
        // Given
        Struct struct = new Struct("TestClassification");
        struct.setValues(null);
        // When
        AtlasClassification result = converter.fromV1ToV2(struct, mockClassificationType, mockContext);
        // Then
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestClassification");
    }

    @Test
    public void testFromV1ToV2InheritsFromStructConverter() {
        // Verify that AtlasClassificationFormatConverter extends AtlasStructFormatConverter
        assertTrue(converter instanceof AtlasStructFormatConverter);
        assertTrue(AtlasStructFormatConverter.class.isAssignableFrom(AtlasClassificationFormatConverter.class));
    }

    @Test
    public void testConstructorSetsCorrectTypeCategory() {
        // Given/When
        AtlasClassificationFormatConverter newConverter = new AtlasClassificationFormatConverter(mockConverterRegistry, mockTypeRegistry);
        // Then
        assertEquals(newConverter.getTypeCategory(), TypeCategory.CLASSIFICATION);
    }

    @Test
    public void testConverterImplementsExpectedInterface() {
        // Verify that the converter implements the expected interface
        assertTrue(converter instanceof AtlasFormatConverter);
    }

    @Test
    public void testTypeSpecificBehavior() {
        // Test that this converter is specifically for CLASSIFICATION types
        assertEquals(converter.getTypeCategory(), TypeCategory.CLASSIFICATION);
        // Verify it inherits from the struct converter
        assertTrue(converter instanceof AtlasStructFormatConverter);
    }

    @Test
    public void testConstructorWithParameters() {
        // Test that constructor properly initializes the converter
        AtlasClassificationFormatConverter testConverter = new AtlasClassificationFormatConverter(mockConverterRegistry, mockTypeRegistry);
        assertNotNull(testConverter);
        assertEquals(testConverter.getTypeCategory(), TypeCategory.CLASSIFICATION);
    }

    @Test
    public void testInheritanceChain() {
        // Verify the inheritance chain is correct
        assertTrue(converter instanceof AtlasStructFormatConverter);
        assertTrue(converter instanceof AtlasAbstractFormatConverter);
        assertTrue(converter instanceof AtlasFormatConverter);
    }

    @Test
    public void testTypeCategory() {
        // Verify that the type category is correctly set and returned
        TypeCategory category = converter.getTypeCategory();
        assertEquals(category, TypeCategory.CLASSIFICATION);
        assertNotNull(category);
    }

    @Test
    public void testBasicFunctionality() {
        // Test basic functionality without complex conversion
        assertNotNull(converter);
        assertEquals(converter.getTypeCategory(), TypeCategory.CLASSIFICATION);
        // Test inheritance
        assertTrue(converter instanceof AtlasStructFormatConverter);
    }

    @Test
    public void testFromV1ToV2WithMapWithTypeName() throws AtlasBaseException {
        // Given
        Map<String, Object> v1Map = new HashMap<>();
        v1Map.put("typeName", "DifferentTypeName");
        v1Map.put("attributes", new HashMap<>());
        // When
        AtlasClassification result = converter.fromV1ToV2(v1Map, mockClassificationType, mockContext);
        // Then
        assertNotNull(result);
        // Should use the type from the classificationType parameter, not from the map
        assertEquals(result.getTypeName(), "TestClassification");
    }

    @Test
    public void testFromV1ToV2WithStructWithTypeName() throws AtlasBaseException {
        // Given
        Struct struct = new Struct("DifferentStructType");
        struct.setValues(new HashMap<>());
        // When
        AtlasClassification result = converter.fromV1ToV2(struct, mockClassificationType, mockContext);
        // Then
        assertNotNull(result);
        // Should use the type from the classificationType parameter
        assertEquals(result.getTypeName(), "TestClassification");
    }
}
