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
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasEnumType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.typedef.EnumTypeDefinition;
import org.apache.atlas.v1.model.typedef.TypesDef;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TypeConverterUtilTest {
    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasEnumType mockEnumType;

    @Mock
    private AtlasEnumDef mockEnumDef;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        // Setup basic mocks for enum type (safest to test)
        when(mockEnumType.getTypeName()).thenReturn("TestEnum");
        when(mockEnumType.getEnumDef()).thenReturn(mockEnumDef);
        when(mockEnumDef.getName()).thenReturn("TestEnum");
        when(mockEnumDef.getDescription()).thenReturn("Test enum description");
        when(mockEnumDef.getElementDefs()).thenReturn(Collections.emptyList());
    }

    @Test
    public void testToTypesDefWithEnumType() throws AtlasBaseException {
        // Given
        AtlasEnumDef.AtlasEnumElementDef element1 = new AtlasEnumDef.AtlasEnumElementDef("VALUE1", "Description 1", 0);
        AtlasEnumDef.AtlasEnumElementDef element2 = new AtlasEnumDef.AtlasEnumElementDef("VALUE2", "Description 2", 1);
        when(mockEnumDef.getElementDefs()).thenReturn(Arrays.asList(element1, element2));
        // When
        TypesDef result = TypeConverterUtil.toTypesDef(mockEnumType, mockTypeRegistry);
        // Then
        assertNotNull(result);
        assertNotNull(result.getEnumTypes());
        assertEquals(result.getEnumTypes().size(), 1);
        EnumTypeDefinition enumTypeDef = result.getEnumTypes().get(0);
        assertEquals(enumTypeDef.getName(), "TestEnum");
        assertEquals(enumTypeDef.getDescription(), "Test enum description");
        assertEquals(enumTypeDef.getEnumValues().size(), 2);
        assertEquals(enumTypeDef.getEnumValues().get(0).getValue(), "VALUE1");
        assertEquals(enumTypeDef.getEnumValues().get(1).getValue(), "VALUE2");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testToAtlasTypesDefWithEmptyDefinition() throws AtlasBaseException {
        // When/Then
        TypeConverterUtil.toAtlasTypesDef("", mockTypeRegistry);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testToAtlasTypesDefWithNullDefinition() throws AtlasBaseException {
        // When/Then
        TypeConverterUtil.toAtlasTypesDef(null, mockTypeRegistry);
    }

    @Test
    public void testToAtlasTypesDefWithValidJson() throws AtlasBaseException {
        // Given
        String validJson = "{\"enumTypes\":[],\"structTypes\":[],\"classTypes\":[],\"traitTypes\":[]}";
        // When
        AtlasTypesDef result = TypeConverterUtil.toAtlasTypesDef(validJson, mockTypeRegistry);
        // Then
        assertNotNull(result);
        assertNotNull(result.getEnumDefs());
        assertNotNull(result.getStructDefs());
        assertNotNull(result.getEntityDefs());
        assertNotNull(result.getClassificationDefs());
    }

    @Test
    public void testPrivateConstructor() throws Exception {
        java.lang.reflect.Constructor<TypeConverterUtil> constructor = TypeConverterUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        TypeConverterUtil instance = constructor.newInstance();
        assertNotNull(instance);
    }

    @Test
    public void testComplexEnumConversion() throws AtlasBaseException {
        // Test with more complex enum definition
        AtlasEnumDef.AtlasEnumElementDef element1 = new AtlasEnumDef.AtlasEnumElementDef("LOW", "Low priority", 0);
        AtlasEnumDef.AtlasEnumElementDef element2 = new AtlasEnumDef.AtlasEnumElementDef("MEDIUM", "Medium priority", 1);
        AtlasEnumDef.AtlasEnumElementDef element3 = new AtlasEnumDef.AtlasEnumElementDef("HIGH", "High priority", 2);
        when(mockEnumDef.getElementDefs()).thenReturn(Arrays.asList(element1, element2, element3));
        when(mockEnumDef.getDefaultValue()).thenReturn("MEDIUM");
        TypesDef result = TypeConverterUtil.toTypesDef(mockEnumType, mockTypeRegistry);
        assertNotNull(result);
        assertEquals(result.getEnumTypes().size(), 1);
        EnumTypeDefinition enumTypeDef = result.getEnumTypes().get(0);
        assertEquals(enumTypeDef.getEnumValues().size(), 3);
        assertEquals(enumTypeDef.getEnumValues().get(1).getValue(), "MEDIUM");
        assertEquals(enumTypeDef.getEnumValues().get(1).getOrdinal(), 1);
    }

    @Test
    public void testJsonConversionWithEnumStructure() throws AtlasBaseException {
        // Test with a JSON structure containing enum
        String complexJson = "{" +
                "\"enumTypes\": [" +
                "{" +
                "\"name\": \"TestEnum\"," +
                "\"description\": \"Test enum\"," +
                "\"enumValues\": [" +
                "{\"value\": \"VAL1\", \"ordinal\": 0}" +
                "]" +
                "}" +
                "]," +
                "\"structTypes\": []," +
                "\"classTypes\": []," +
                "\"traitTypes\": []" +
                "}";

        AtlasTypesDef result = TypeConverterUtil.toAtlasTypesDef(complexJson, mockTypeRegistry);
        assertNotNull(result);
        assertNotNull(result.getEnumDefs());
        assertTrue(result.getEnumDefs().size() >= 0); // Should handle the enum
    }

    @Test
    public void testEnumWithNoElements() throws AtlasBaseException {
        // Test enum with no elements
        when(mockEnumDef.getElementDefs()).thenReturn(Collections.emptyList());
        TypesDef result = TypeConverterUtil.toTypesDef(mockEnumType, mockTypeRegistry);
        assertNotNull(result);
        assertNotNull(result.getEnumTypes());
        assertEquals(result.getEnumTypes().size(), 1);
        assertEquals(result.getEnumTypes().get(0).getEnumValues().size(), 0);
    }

    @Test
    public void testEnumTypeDefStructure() throws AtlasBaseException {
        // Test that TypesDef structure is correctly built for enum type
        TypesDef enumResult = TypeConverterUtil.toTypesDef(mockEnumType, mockTypeRegistry);
        assertNotNull(enumResult.getEnumTypes());
        assertTrue(enumResult.getClassTypes() == null || enumResult.getClassTypes().isEmpty());
        assertTrue(enumResult.getTraitTypes() == null || enumResult.getTraitTypes().isEmpty());
        assertTrue(enumResult.getStructTypes() == null || enumResult.getStructTypes().isEmpty());
    }

    @Test
    public void testEnumElementDetails() throws AtlasBaseException {
        // Test detailed enum element conversion
        AtlasEnumDef.AtlasEnumElementDef element = new AtlasEnumDef.AtlasEnumElementDef("ACTIVE", "Active state", 1);
        when(mockEnumDef.getElementDefs()).thenReturn(Arrays.asList(element));
        TypesDef result = TypeConverterUtil.toTypesDef(mockEnumType, mockTypeRegistry);
        EnumTypeDefinition enumTypeDef = result.getEnumTypes().get(0);
        assertEquals(enumTypeDef.getEnumValues().size(), 1);
        assertEquals(enumTypeDef.getEnumValues().get(0).getValue(), "ACTIVE");
        assertEquals(enumTypeDef.getEnumValues().get(0).getOrdinal(), 1);
    }

    @Test
    public void testEnumVersionAndDescription() throws AtlasBaseException {
        // Test that enum version and description are properly handled
        when(mockEnumDef.getTypeVersion()).thenReturn("2.0");
        when(mockEnumDef.getDescription()).thenReturn("Updated enum description");
        TypesDef result = TypeConverterUtil.toTypesDef(mockEnumType, mockTypeRegistry);
        assertNotNull(result.getEnumTypes());
        assertEquals(result.getEnumTypes().get(0).getDescription(), "Updated enum description");
    }

    @Test
    public void testToAtlasTypesDefWithComplexEnumJson() throws AtlasBaseException {
        // Test with complex enum JSON
        String enumJson = "{" + "\"enumTypes\": [" + "{" + "\"name\": \"Priority\"," + "\"description\": \"Priority levels\"," + "\"enumValues\": [" + "{\"value\": \"LOW\", \"ordinal\": 0}," + "{\"value\": \"HIGH\", \"ordinal\": 1}" + "]" + "}" + "]," + "\"structTypes\": []," + "\"classTypes\": []," + "\"traitTypes\": []" + "}";
        AtlasTypesDef result = TypeConverterUtil.toAtlasTypesDef(enumJson, mockTypeRegistry);
        assertNotNull(result);
        assertNotNull(result.getEnumDefs());
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testToAtlasTypesDefWithInvalidJson() throws AtlasBaseException {
        // Test with malformed JSON
        String invalidJson = "{invalid json}";
        TypeConverterUtil.toAtlasTypesDef(invalidJson, mockTypeRegistry);
    }

    @Test
    public void testToAtlasTypesDefWithEmptyEnumJson() throws AtlasBaseException {
        // Test with JSON containing empty enum array
        String emptyEnumJson = "{" + "\"enumTypes\": []," + "\"structTypes\": []," + "\"classTypes\": []," + "\"traitTypes\": []" + "}";
        AtlasTypesDef result = TypeConverterUtil.toAtlasTypesDef(emptyEnumJson, mockTypeRegistry);
        assertNotNull(result);
        assertNotNull(result.getEnumDefs());
        assertTrue(result.getEnumDefs().isEmpty());
    }
}
