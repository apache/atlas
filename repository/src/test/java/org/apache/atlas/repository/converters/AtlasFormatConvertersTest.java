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
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.mockito.Mockito.mockConstruction;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AtlasFormatConvertersTest {
    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    private AtlasFormatConverters formatConverters;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        formatConverters = new AtlasFormatConverters(mockTypeRegistry);
    }

    @Test
    public void testConstructorRegistersAllConverters() throws Exception {
        // Use reflection to access the private registry field
        Field registryField = AtlasFormatConverters.class.getDeclaredField("registry");
        registryField.setAccessible(true);
        Map<TypeCategory, AtlasFormatConverter> registry = (Map<TypeCategory, AtlasFormatConverter>) registryField.get(formatConverters);
        // Verify all expected converters are registered
        assertTrue(registry.containsKey(TypeCategory.PRIMITIVE));
        assertTrue(registry.containsKey(TypeCategory.ENUM));
        assertTrue(registry.containsKey(TypeCategory.STRUCT));
        assertTrue(registry.containsKey(TypeCategory.CLASSIFICATION));
        assertTrue(registry.containsKey(TypeCategory.ENTITY));
        assertTrue(registry.containsKey(TypeCategory.ARRAY));
        assertTrue(registry.containsKey(TypeCategory.MAP));
        assertTrue(registry.containsKey(TypeCategory.OBJECT_ID_TYPE));
        // Verify converter types
        assertTrue(registry.get(TypeCategory.PRIMITIVE) instanceof AtlasPrimitiveFormatConverter);
        assertTrue(registry.get(TypeCategory.ENUM) instanceof AtlasEnumFormatConverter);
        assertTrue(registry.get(TypeCategory.STRUCT) instanceof AtlasStructFormatConverter);
        assertTrue(registry.get(TypeCategory.CLASSIFICATION) instanceof AtlasClassificationFormatConverter);
        assertTrue(registry.get(TypeCategory.ENTITY) instanceof AtlasEntityFormatConverter);
        assertTrue(registry.get(TypeCategory.ARRAY) instanceof AtlasArrayFormatConverter);
        assertTrue(registry.get(TypeCategory.MAP) instanceof AtlasMapFormatConverter);
        assertTrue(registry.get(TypeCategory.OBJECT_ID_TYPE) instanceof AtlasObjectIdConverter);
    }

    @Test
    public void testGetConverterForPrimitive() throws AtlasBaseException {
        // When
        AtlasFormatConverter converter = formatConverters.getConverter(TypeCategory.PRIMITIVE);
        // Then
        assertNotNull(converter);
        assertTrue(converter instanceof AtlasPrimitiveFormatConverter);
        assertEquals(converter.getTypeCategory(), TypeCategory.PRIMITIVE);
    }

    @Test
    public void testGetConverterForEnum() throws AtlasBaseException {
        // When
        AtlasFormatConverter converter = formatConverters.getConverter(TypeCategory.ENUM);
        // Then
        assertNotNull(converter);
        assertTrue(converter instanceof AtlasEnumFormatConverter);
        assertEquals(converter.getTypeCategory(), TypeCategory.ENUM);
    }

    @Test
    public void testGetConverterForStruct() throws AtlasBaseException {
        // When
        AtlasFormatConverter converter = formatConverters.getConverter(TypeCategory.STRUCT);
        // Then
        assertNotNull(converter);
        assertTrue(converter instanceof AtlasStructFormatConverter);
        assertEquals(converter.getTypeCategory(), TypeCategory.STRUCT);
    }

    @Test
    public void testGetConverterForClassification() throws AtlasBaseException {
        // When
        AtlasFormatConverter converter = formatConverters.getConverter(TypeCategory.CLASSIFICATION);
        // Then
        assertNotNull(converter);
        assertTrue(converter instanceof AtlasClassificationFormatConverter);
        assertEquals(converter.getTypeCategory(), TypeCategory.CLASSIFICATION);
    }

    @Test
    public void testGetConverterForEntity() throws AtlasBaseException {
        // When
        AtlasFormatConverter converter = formatConverters.getConverter(TypeCategory.ENTITY);
        // Then
        assertNotNull(converter);
        assertTrue(converter instanceof AtlasEntityFormatConverter);
        assertEquals(converter.getTypeCategory(), TypeCategory.ENTITY);
    }

    @Test
    public void testGetConverterForArray() throws AtlasBaseException {
        // When
        AtlasFormatConverter converter = formatConverters.getConverter(TypeCategory.ARRAY);
        // Then
        assertNotNull(converter);
        assertTrue(converter instanceof AtlasArrayFormatConverter);
        assertEquals(converter.getTypeCategory(), TypeCategory.ARRAY);
    }

    @Test
    public void testGetConverterForMap() throws AtlasBaseException {
        // When
        AtlasFormatConverter converter = formatConverters.getConverter(TypeCategory.MAP);
        // Then
        assertNotNull(converter);
        assertTrue(converter instanceof AtlasMapFormatConverter);
        assertEquals(converter.getTypeCategory(), TypeCategory.MAP);
    }

    @Test
    public void testGetConverterForObjectIdType() throws AtlasBaseException {
        // When
        AtlasFormatConverter converter = formatConverters.getConverter(TypeCategory.OBJECT_ID_TYPE);
        // Then
        assertNotNull(converter);
        assertTrue(converter instanceof AtlasObjectIdConverter);
        assertEquals(converter.getTypeCategory(), TypeCategory.OBJECT_ID_TYPE);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testGetConverterForUnknownType() throws AtlasBaseException {
        TypeCategory unknownCategory = null;
        // When/Then
        formatConverters.getConverter(unknownCategory);
    }

    @Test
    public void testEntityConverterRegisteredForObjectIdType() throws Exception {
        Field registryField = AtlasFormatConverters.class.getDeclaredField("registry");
        registryField.setAccessible(true);
        Map<TypeCategory, AtlasFormatConverter> registry = (Map<TypeCategory, AtlasFormatConverter>) registryField.get(formatConverters);
        AtlasFormatConverter entityConverter = registry.get(TypeCategory.ENTITY);
        AtlasFormatConverter objectIdConverter = registry.get(TypeCategory.OBJECT_ID_TYPE);
        assertNotNull(entityConverter);
        assertNotNull(objectIdConverter);
        assertTrue(entityConverter instanceof AtlasEntityFormatConverter);
        assertTrue(objectIdConverter instanceof AtlasObjectIdConverter);
    }

    @Test
    public void testConverterRegistrationOrder() throws Exception {
        Field registryField = AtlasFormatConverters.class.getDeclaredField("registry");
        registryField.setAccessible(true);
        Map<TypeCategory, AtlasFormatConverter> registry = (Map<TypeCategory, AtlasFormatConverter>) registryField.get(formatConverters);
        // Verify all converters are properly initialized
        for (TypeCategory category : new TypeCategory[] {
                TypeCategory.PRIMITIVE, TypeCategory.ENUM, TypeCategory.STRUCT,
                TypeCategory.CLASSIFICATION, TypeCategory.ENTITY, TypeCategory.ARRAY,
                TypeCategory.MAP, TypeCategory.OBJECT_ID_TYPE}) {
            AtlasFormatConverter converter = registry.get(category);
            assertNotNull(converter, "Converter for " + category + " should not be null");
        }
    }

    @Test
    public void testConstructorWithMockRegistry() {
        AtlasFormatConverters newFormatConverters = new AtlasFormatConverters(mockTypeRegistry);
        // Then - Verify it can get converters without throwing exceptions
        try {
            assertNotNull(newFormatConverters.getConverter(TypeCategory.PRIMITIVE));
            assertNotNull(newFormatConverters.getConverter(TypeCategory.ENUM));
            assertNotNull(newFormatConverters.getConverter(TypeCategory.STRUCT));
            assertNotNull(newFormatConverters.getConverter(TypeCategory.CLASSIFICATION));
            assertNotNull(newFormatConverters.getConverter(TypeCategory.ENTITY));
            assertNotNull(newFormatConverters.getConverter(TypeCategory.ARRAY));
            assertNotNull(newFormatConverters.getConverter(TypeCategory.MAP));
            assertNotNull(newFormatConverters.getConverter(TypeCategory.OBJECT_ID_TYPE));
        } catch (AtlasBaseException e) {
            throw new RuntimeException("Unexpected exception during converter retrieval", e);
        }
    }

    @Test
    public void testConverterConsistency() throws AtlasBaseException {
        // Verify that getting the same converter type multiple times returns the same instance
        AtlasFormatConverter converter1 = formatConverters.getConverter(TypeCategory.PRIMITIVE);
        AtlasFormatConverter converter2 = formatConverters.getConverter(TypeCategory.PRIMITIVE);
        assertEquals(converter1, converter2);
    }

    @Test
    public void testAllTypeCategoriesSupported() throws AtlasBaseException {
        // Test that all important type categories have converters
        TypeCategory[] supportedCategories = {
            TypeCategory.PRIMITIVE,
            TypeCategory.ENUM,
            TypeCategory.STRUCT,
            TypeCategory.CLASSIFICATION,
            TypeCategory.ENTITY,
            TypeCategory.ARRAY,
            TypeCategory.MAP,
            TypeCategory.OBJECT_ID_TYPE
        };
        for (TypeCategory category : supportedCategories) {
            AtlasFormatConverter converter = formatConverters.getConverter(category);
            assertNotNull(converter, "Converter for " + category + " should exist");
        }
    }

    @Test
    public void testRegistrationWithMockedConverters() {
        try (MockedConstruction<AtlasPrimitiveFormatConverter> primitiveConverterMock = mockConstruction(AtlasPrimitiveFormatConverter.class); MockedConstruction<AtlasEnumFormatConverter> enumConverterMock = mockConstruction(AtlasEnumFormatConverter.class)) {
            // Create new instance which should trigger converter construction
            AtlasFormatConverters testFormatConverters = new AtlasFormatConverters(mockTypeRegistry);
            // Verify constructors were called
            assertEquals(primitiveConverterMock.constructed().size(), 1);
            assertEquals(enumConverterMock.constructed().size(), 1);
        }
    }

    @Test
    public void testGetConverterPerformance() throws AtlasBaseException {
        long startTime = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            formatConverters.getConverter(TypeCategory.PRIMITIVE);
            formatConverters.getConverter(TypeCategory.ENUM);
            formatConverters.getConverter(TypeCategory.ENTITY);
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        assertTrue(durationMs < 100, "Converter lookup should be fast, took " + durationMs + "ms");
    }
}
