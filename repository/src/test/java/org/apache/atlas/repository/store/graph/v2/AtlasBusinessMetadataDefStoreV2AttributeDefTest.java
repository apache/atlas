/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for AtlasBusinessMetadataDefStoreV2 attributeDef validation
 * Tests validateBusinessAttributeDef and validateOptionsMap methods
 */
public class AtlasBusinessMetadataDefStoreV2AttributeDefTest {
    private TestableAtlasBusinessMetadataDefStoreV2 createStoreInstance() {
        return new TestableAtlasBusinessMetadataDefStoreV2();
    }

    private AtlasStructDef.AtlasAttributeDef createValidAttributeDef() {
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef();
        attributeDef.setName("testAttribute");
        attributeDef.setTypeName("string");
        return attributeDef;
    }

    // =====================================================================================
    // Tests for validateBusinessAttributeDef
    // =====================================================================================

    @Test
    public void testValidateBusinessAttributeDefValid() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        AtlasStructDef.AtlasAttributeDef attributeDef = createValidAttributeDef();
        
        // Should not throw exception
        store.validateBusinessAttributeDef(attributeDef);
    }

    @Test
    public void testValidateBusinessAttributeDefInvalidName() {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef();
        attributeDef.setName("invalid-name-with-dashes");
        attributeDef.setTypeName("string");
        
        try {
            store.validateBusinessAttributeDef(attributeDef);
            fail("Expected AtlasBaseException for invalid attribute name");
        } catch (AtlasBaseException e) {
            assertTrue(e.getMessage().contains("invalid-name-with-dashes"));
        }
    }

    @Test
    public void testValidateBusinessAttributeDefWithValidOptions() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        AtlasStructDef.AtlasAttributeDef attributeDef = createValidAttributeDef();
        
        Map<String, String> options = new HashMap<>();
        options.put("maxLength", "100");
        options.put("description", "Test description");
        options.put("isRichText", "true");
        attributeDef.setOptions(options);
        
        // Should not throw exception
        store.validateBusinessAttributeDef(attributeDef);
    }

    @Test
    public void testValidateBusinessAttributeDefWithNullOptions() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        AtlasStructDef.AtlasAttributeDef attributeDef = createValidAttributeDef();
        attributeDef.setOptions(null);
        
        // Should not throw exception - null options are allowed
        store.validateBusinessAttributeDef(attributeDef);
    }

    @Test
    public void testValidateBusinessAttributeDefWithEmptyOptions() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        AtlasStructDef.AtlasAttributeDef attributeDef = createValidAttributeDef();
        attributeDef.setOptions(new HashMap<>());
        
        // Should not throw exception - empty options are allowed
        store.validateBusinessAttributeDef(attributeDef);
    }

    // =====================================================================================
    // Tests for validateOptionsMap - JSON serialization
    // =====================================================================================

    @Test
    public void testValidateOptionsMapValidJsonSerialization() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "value2");
        
        // Should not throw exception
        store.validateOptionsMap(options, "testAttribute");
    }

    // =====================================================================================
    // Tests for validateOptionsMap - null/empty values
    // =====================================================================================

    @Test
    public void testValidateOptionsMapNullValue() {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", null);
        
        try {
            store.validateOptionsMap(options, "testAttribute");
            fail("Expected AtlasBaseException for null option value");
        } catch (AtlasBaseException e) {
            assertTrue(e.getMessage().contains("has null/empty value"));
            assertTrue(e.getMessage().contains("testAttribute"));
        }
    }

    @Test
    public void testValidateOptionsMapEmptyValue() {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "");
        
        try {
            store.validateOptionsMap(options, "testAttribute");
            fail("Expected AtlasBaseException for empty option value");
        } catch (AtlasBaseException e) {
            assertTrue(e.getMessage().contains("has null/empty value"));
            assertTrue(e.getMessage().contains("testAttribute"));
        }
    }

    @Test
    public void testValidateOptionsMapBlankValue() {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "   ");
        
        try {
            store.validateOptionsMap(options, "testAttribute");
            fail("Expected AtlasBaseException for blank option value");
        } catch (AtlasBaseException e) {
            assertTrue(e.getMessage().contains("has null/empty value"));
            assertTrue(e.getMessage().contains("testAttribute"));
        }
    }

    // =====================================================================================
    // Tests for validateOptionsMap - enumType field (ATTR_OPTION_FIELDS_TO_SKIP)
    // =====================================================================================

    @Test
    public void testValidateOptionsMapEnumTypeEmptyValue() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("enumType", "");  // Empty string is allowed for enumType
        
        // Should not throw exception - enumType can be empty
        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapEnumTypeBlankValue() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("enumType", "   ");  // Blank string is allowed for enumType
        
        // Should not throw exception - enumType can be blank
        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapEnumTypeNullValue() {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("enumType", null);  // null is NOT allowed for enumType
        
        try {
            store.validateOptionsMap(options, "testAttribute");
            fail("Expected AtlasBaseException for null enumType value");
        } catch (AtlasBaseException e) {
            assertTrue(e.getMessage().contains("cannot be null"));
            assertTrue(e.getMessage().contains("enumType"));
            assertTrue(e.getMessage().contains("testAttribute"));
        }
    }

    // =====================================================================================
    // Tests for validateOptionsMap - JSON string validation
    // =====================================================================================

    @Test
    public void testValidateOptionsMapValidJsonArray() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("applicableEntityTypes", "[\"DataSet\", \"Table\"]");
        
        // Should not throw exception
        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapValidJsonObject() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("config", "{\"maxSize\": 1000, \"enabled\": true}");
        
        // Should not throw exception
        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapValidEmptyJsonArray() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("emptyArray", "[]");
        
        // Should not throw exception
        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapValidEmptyJsonObject() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("emptyObject", "{}");
        
        // Should not throw exception
        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapValidNestedJson() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("nested", "{\"outer\": {\"inner\": [1, 2, 3]}}");
        
        // Should not throw exception
        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapInvalidJsonArray() {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("applicableEntityTypes", "[\"DataSet\", \"Table\"");  // Missing closing bracket
        
        try {
            store.validateOptionsMap(options, "testAttribute");
            fail("Expected AtlasBaseException for invalid JSON array");
        } catch (AtlasBaseException e) {
            assertTrue(e.getMessage().contains("invalid JSON string"));
            assertTrue(e.getMessage().contains("testAttribute"));
        }
    }

    @Test
    public void testValidateOptionsMapInvalidJsonObject() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("config", "{\"maxSize\": 1000, \"enabled\": true");  // Missing closing brace - but not validated

        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapMalformedJson() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("badJson", "{this is not valid json}");

        store.validateOptionsMap(options, "testAttribute");
    }

    // =====================================================================================
    // Tests for validateOptionsMap - JSON primitives (should be accepted)
    // =====================================================================================

    @Test
    public void testValidateOptionsMapJsonPrimitives() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("boolValue", "true");
        options.put("numberValue", "123");
        options.put("stringValue", "simple text");
        options.put("nullValue", "null");
        
        // Should not throw exception - these are valid as string values
        store.validateOptionsMap(options, "testAttribute");
    }

    // =====================================================================================
    // Tests for validateOptionsMap - edge cases
    // =====================================================================================

    @Test
    public void testValidateOptionsMapJsonWithWhitespace() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("jsonWithSpaces", "  {\"key\": \"value\"}  ");  // JSON with leading/trailing whitespace
        
        // Should not throw exception - whitespace is trimmed
        store.validateOptionsMap(options, "testAttribute");
    }

    @Test
    public void testValidateOptionsMapMultipleErrors() {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("key1", null);  // null value
        options.put("key2", "");    // empty value
        options.put("key3", "{\"invalid\": json}");  // invalid JSON
        
        try {
            store.validateOptionsMap(options, "testAttribute");
            fail("Expected AtlasBaseException for multiple validation errors");
        } catch (AtlasBaseException e) {
            // Should catch the first error (null value)
            assertTrue(e.getMessage().contains("has null/empty value"));
            assertTrue(e.getMessage().contains("testAttribute"));
        }
    }

    @Test
    public void testValidateOptionsMapComplexScenario() throws AtlasBaseException {
        TestableAtlasBusinessMetadataDefStoreV2 store = createStoreInstance();
        Map<String, String> options = new HashMap<>();
        options.put("maxLength", "100");
        options.put("description", "Test description");
        options.put("applicableEntityTypes", "[\"DataSet\", \"Table\", \"Column\"]");
        options.put("config", "{\"maxSize\": 1000, \"enabled\": true, \"nested\": {\"key\": \"value\"}}");
        options.put("enumType", "");  // Empty enumType is allowed
        options.put("isRichText", "true");
        
        // Should not throw exception - all valid
        store.validateOptionsMap(options, "testAttribute");
    }

    /**
     * Test subclass that exposes validation methods for unit testing
     * Uses reflection to access private methods, following the pattern from ElasticsearchDslOptimizerTest
     */
    private static class TestableAtlasBusinessMetadataDefStoreV2 extends AtlasBusinessMetadataDefStoreV2 {
        public TestableAtlasBusinessMetadataDefStoreV2() {
            super(null, null, null);
        }

        /**
         * Public wrapper to access private validateBusinessAttributeDef method
         */
        public void validateBusinessAttributeDef(AtlasStructDef.AtlasAttributeDef attributeDef) throws AtlasBaseException {
            try {
                java.lang.reflect.Method method = AtlasBusinessMetadataDefStoreV2.class.getDeclaredMethod(
                    "validateBusinessAttributeDef", AtlasStructDef.AtlasAttributeDef.class);
                method.setAccessible(true);
                method.invoke(this, attributeDef);
            } catch (Exception e) {
                if (e.getCause() instanceof AtlasBaseException) {
                    throw (AtlasBaseException) e.getCause();
                }
                throw new RuntimeException("Failed to invoke validateBusinessAttributeDef", e);
            }
        }

        /**
         * Public wrapper to access private validateOptionsMap method
         */
        public void validateOptionsMap(Map<String, String> options, String attrName) throws AtlasBaseException {
            try {
                java.lang.reflect.Method method = AtlasBusinessMetadataDefStoreV2.class.getDeclaredMethod(
                    "validateOptionsMap", Map.class, String.class);
                method.setAccessible(true);
                method.invoke(this, options, attrName);
            } catch (Exception e) {
                if (e.getCause() instanceof AtlasBaseException) {
                    throw (AtlasBaseException) e.getCause();
                }
                throw new RuntimeException("Failed to invoke validateOptionsMap", e);
            }
        }
    }
}

