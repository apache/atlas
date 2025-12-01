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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AtlasAbstractFormatConverterTest {
    @Mock
    private AtlasFormatConverters mockConverterRegistry;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasType mockAtlasType;

    private TestableAtlasAbstractFormatConverter converter;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new TestableAtlasAbstractFormatConverter(
            mockConverterRegistry, mockTypeRegistry, TypeCategory.PRIMITIVE);
    }

    @Test
    public void testIsValidValueV1WithValidValue() {
        // Given
        String testValue = "testValue";
        when(mockAtlasType.isValidValue(testValue)).thenReturn(true);

        // When
        boolean result = converter.isValidValueV1(testValue, mockAtlasType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithInvalidValue() {
        // Given
        String testValue = "invalidValue";
        when(mockAtlasType.isValidValue(testValue)).thenReturn(false);

        // When
        boolean result = converter.isValidValueV1(testValue, mockAtlasType);

        // Then
        assertFalse(result);
    }

    @Test
    public void testIsValidValueV1WithNullValue() {
        // Given
        when(mockAtlasType.isValidValue(null)).thenReturn(true);

        // When
        boolean result = converter.isValidValueV1(null, mockAtlasType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testGetTypeCategory() {
        // When
        TypeCategory result = converter.getTypeCategory();

        // Then
        assertEquals(result, TypeCategory.PRIMITIVE);
    }

    @Test
    public void testIsValidValueV1WithComplexObject() {
        // Given
        Object complexObject = new Object() {
            @Override
            public String toString() {
                return "ComplexObject";
            }
        };
        when(mockAtlasType.isValidValue(complexObject)).thenReturn(true);

        // When
        boolean result = converter.isValidValueV1(complexObject, mockAtlasType);

        // Then
        assertTrue(result);
    }

    @Test
    public void testIsValidValueV1WithNullType() {
        // Given
        String testValue = "testValue";

        try {
            boolean result = converter.isValidValueV1(testValue, null);
            // If we reach here, the method handled null gracefully
            assertFalse(result);
        } catch (NullPointerException e) {
            // This is also acceptable behavior for null type
            assertTrue(true);
        }
    }

    private static class TestableAtlasAbstractFormatConverter extends AtlasAbstractFormatConverter {
        protected TestableAtlasAbstractFormatConverter(AtlasFormatConverters converterRegistry,
                                                       AtlasTypeRegistry typeRegistry,
                                                       TypeCategory typeCategory) {
            super(converterRegistry, typeRegistry, typeCategory);
        }

        @Override
        public Object fromV1ToV2(Object v1Obj, AtlasType type, ConverterContext ctx) throws AtlasBaseException {
            return v1Obj;
        }

        @Override
        public Object fromV2ToV1(Object v2Obj, AtlasType type, ConverterContext ctx) throws AtlasBaseException {
            return v2Obj;
        }
    }
}
