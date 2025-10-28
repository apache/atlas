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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasEnumType {
    private AtlasEnumType enumTypeWithDefaultValue;
    private AtlasEnumType enumTypeWithoutDefaultValue;
    private AtlasEnumType emptyEnumType;

    @BeforeClass
    public void setUp() {
        // Create enum with default value
        AtlasEnumElementDef elem1 = new AtlasEnumElementDef("ACTIVE", "Active state", 1);
        AtlasEnumElementDef elem2 = new AtlasEnumElementDef("INACTIVE", "Inactive state", 2);
        AtlasEnumElementDef elem3 = new AtlasEnumElementDef("PENDING", "Pending state", 3);

        AtlasEnumDef enumDefWithDefault = new AtlasEnumDef("StatusEnum", "Status enumeration", "1.0",
                Arrays.asList(elem1, elem2, elem3), "ACTIVE");
        enumTypeWithDefaultValue = new AtlasEnumType(enumDefWithDefault);

        // Create enum without explicit default value
        AtlasEnumDef enumDefWithoutDefault = new AtlasEnumDef("TypeEnum", "Type enumeration", "1.0",
                Arrays.asList(elem1, elem2, elem3));
        enumTypeWithoutDefaultValue = new AtlasEnumType(enumDefWithoutDefault);

        // Create empty enum
        AtlasEnumDef emptyEnumDef = new AtlasEnumDef("EmptyEnum", "Empty enumeration", "1.0", Collections.emptyList());
        emptyEnumType = new AtlasEnumType(emptyEnumDef);
    }

    @Test
    public void testGetEnumDef() {
        AtlasEnumDef enumDef = enumTypeWithDefaultValue.getEnumDef();
        assertNotNull(enumDef);
        assertEquals(enumDef.getName(), "StatusEnum");
        assertEquals(enumDef.getDescription(), "Status enumeration");
        assertEquals(enumDef.getTypeVersion(), "1.0");
        assertEquals(enumDef.getElementDefs().size(), 3);
        assertEquals(enumDef.getDefaultValue(), "ACTIVE");
    }

    @Test
    public void testCreateDefaultValueWithExplicitDefault() {
        Object defaultValue = enumTypeWithDefaultValue.createDefaultValue();
        assertEquals(defaultValue, "ACTIVE");
    }

    @Test
    public void testCreateDefaultValueWithoutExplicitDefault() {
        Object defaultValue = enumTypeWithoutDefaultValue.createDefaultValue();
        assertEquals(defaultValue, "ACTIVE"); // Should use first element
    }

    @Test
    public void testCreateDefaultValueEmptyEnum() {
        Object defaultValue = emptyEnumType.createDefaultValue();
        assertNull(defaultValue);
    }

    @Test
    public void testIsValidValueWithValidValues() {
        assertTrue(enumTypeWithDefaultValue.isValidValue("ACTIVE"));
        assertTrue(enumTypeWithDefaultValue.isValidValue("INACTIVE"));
        assertTrue(enumTypeWithDefaultValue.isValidValue("PENDING"));
        // Case insensitive
        assertTrue(enumTypeWithDefaultValue.isValidValue("active"));
        assertTrue(enumTypeWithDefaultValue.isValidValue("Active"));
        assertTrue(enumTypeWithDefaultValue.isValidValue("ACTIVE"));
    }

    @Test
    public void testIsValidValueWithInvalidValues() {
        assertFalse(enumTypeWithDefaultValue.isValidValue("INVALID"));
        assertFalse(enumTypeWithDefaultValue.isValidValue(""));
        assertFalse(enumTypeWithDefaultValue.isValidValue("random"));
    }

    @Test
    public void testIsValidValueWithNull() {
        assertTrue(enumTypeWithDefaultValue.isValidValue(null));
    }

    @Test
    public void testGetNormalizedValueWithValidValues() {
        assertEquals(enumTypeWithDefaultValue.getNormalizedValue("ACTIVE"), "ACTIVE");
        assertEquals(enumTypeWithDefaultValue.getNormalizedValue("active"), "ACTIVE");
        assertEquals(enumTypeWithDefaultValue.getNormalizedValue("Active"), "ACTIVE");
        assertEquals(enumTypeWithDefaultValue.getNormalizedValue("INACTIVE"), "INACTIVE");
        assertEquals(enumTypeWithDefaultValue.getNormalizedValue("inactive"), "INACTIVE");
    }

    @Test
    public void testGetNormalizedValueWithInvalidValues() {
        assertNull(enumTypeWithDefaultValue.getNormalizedValue("INVALID"));
        assertNull(enumTypeWithDefaultValue.getNormalizedValue(""));
        assertNull(enumTypeWithDefaultValue.getNormalizedValue("random"));
    }

    @Test
    public void testGetNormalizedValueWithNull() {
        assertNull(enumTypeWithDefaultValue.getNormalizedValue(null));
    }

    @Test
    public void testResolveReferences() throws AtlasBaseException {
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        // Should not throw exception
        enumTypeWithDefaultValue.resolveReferences(typeRegistry);
    }

    @Test
    public void testGetEnumElementDefByValue() {
        AtlasEnumElementDef elementDef = enumTypeWithDefaultValue.getEnumElementDef("ACTIVE");
        assertNotNull(elementDef);
        assertEquals(elementDef.getValue(), "ACTIVE");
        assertEquals(elementDef.getDescription(), "Active state");
        assertEquals(elementDef.getOrdinal(), Integer.valueOf(1));

        // Case insensitive
        elementDef = enumTypeWithDefaultValue.getEnumElementDef("active");
        assertNotNull(elementDef);
        assertEquals(elementDef.getValue(), "ACTIVE");
    }

    @Test
    public void testGetEnumElementDefByValueNotFound() {
        AtlasEnumElementDef elementDef = enumTypeWithDefaultValue.getEnumElementDef("INVALID");
        assertNull(elementDef);
    }

    @Test
    public void testGetEnumElementDefByValueNull() {
        AtlasEnumElementDef elementDef = enumTypeWithDefaultValue.getEnumElementDef((String) null);
        assertNull(elementDef);
    }

    @Test
    public void testGetEnumElementDefByOrdinal() {
        AtlasEnumElementDef elementDef = enumTypeWithDefaultValue.getEnumElementDef(1);
        assertNotNull(elementDef);
        assertEquals(elementDef.getValue(), "ACTIVE");
        assertEquals(elementDef.getOrdinal(), Integer.valueOf(1));

        elementDef = enumTypeWithDefaultValue.getEnumElementDef(2);
        assertNotNull(elementDef);
        assertEquals(elementDef.getValue(), "INACTIVE");
        assertEquals(elementDef.getOrdinal(), Integer.valueOf(2));
    }

    @Test
    public void testGetEnumElementDefByOrdinalNotFound() {
        AtlasEnumElementDef elementDef = enumTypeWithDefaultValue.getEnumElementDef(999);
        assertNull(elementDef);
    }

    @Test
    public void testGetEnumElementDefByOrdinalNull() {
        AtlasEnumElementDef elementDef = enumTypeWithDefaultValue.getEnumElementDef((Number) null);
        assertNull(elementDef);
    }

    @Test
    public void testGetEnumElementDefByOrdinalDifferentNumberTypes() {
        AtlasEnumElementDef elementDef = enumTypeWithDefaultValue.getEnumElementDef(1L); // Long
        assertNotNull(elementDef);
        assertEquals(elementDef.getValue(), "ACTIVE");

        elementDef = enumTypeWithDefaultValue.getEnumElementDef(2.0); // Double
        assertNotNull(elementDef);
        assertEquals(elementDef.getValue(), "INACTIVE");

        elementDef = enumTypeWithDefaultValue.getEnumElementDef((byte) 3); // Byte
        assertNotNull(elementDef);
        assertEquals(elementDef.getValue(), "PENDING");
    }

    @Test
    public void testEnumTypeWithSingleElement() {
        AtlasEnumElementDef singleElement = new AtlasEnumElementDef("SINGLE", "Single element", 1);
        AtlasEnumDef singleEnumDef = new AtlasEnumDef("SingleEnum", "Single element enum", "1.0",
                Collections.singletonList(singleElement));
        AtlasEnumType singleEnumType = new AtlasEnumType(singleEnumDef);

        assertEquals(singleEnumType.createDefaultValue(), "SINGLE");
        assertTrue(singleEnumType.isValidValue("SINGLE"));
        assertFalse(singleEnumType.isValidValue("OTHER"));
        assertEquals(singleEnumType.getNormalizedValue("single"), "SINGLE");
    }

    @Test
    public void testEnumTypeBasicProperties() {
        assertEquals(enumTypeWithDefaultValue.getTypeName(), "StatusEnum");
        assertNotNull(enumTypeWithDefaultValue.getTypeCategory());
        // Service type might be null for enum types, so just check that it's accessible
        enumTypeWithDefaultValue.getServiceType(); // Should not throw exception
    }
}
