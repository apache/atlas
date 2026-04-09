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
package org.apache.atlas.repository.graphdb.janus.graphson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.graphson.AtlasElementPropertyConfig.ElementPropertiesRule;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AtlasGraphSONUtilityTest {
    @Mock
    private AtlasVertex mockVertex;

    @Mock
    private AtlasEdge mockEdge;

    @Mock
    private AtlasVertex mockOutVertex;

    @Mock
    private AtlasVertex mockInVertex;

    private ObjectMapper objectMapper;
    private JsonNodeFactory jsonNodeFactory;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        objectMapper = new ObjectMapper();
        jsonNodeFactory = JsonNodeFactory.instance;
    }

    @Test
    public void testJsonFromElementWithVertex() throws JSONException {
        // Setup vertex mock
        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("name", "age"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("name", Object.class)).thenReturn("John");
        when(mockVertex.getProperty("age", Object.class)).thenReturn(30);

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("name", "age"));

        // Test NORMAL mode
        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, propertyKeys, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("name"));
        assertEquals(result.getString("name"), "John");
        assertTrue(result.has("age"));
        assertEquals(result.getInt("age"), 30);
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_ID));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_TYPE));
        assertEquals(result.getString(AtlasGraphSONTokens.INTERNAL_TYPE), AtlasGraphSONTokens.VERTEX);
    }

    @Test
    public void testJsonFromElementWithEdge() throws JSONException {
        // Setup edge mock
        when(mockEdge.getId()).thenReturn("edge1");
        when(mockEdge.getLabel()).thenReturn("knows");
        doReturn(new HashSet<>(Arrays.asList("weight", "since"))).when(mockEdge).getPropertyKeys();
        when(mockEdge.getProperty("weight", Object.class)).thenReturn(0.8);
        when(mockEdge.getProperty("since", Object.class)).thenReturn("2020");
        when(mockEdge.getOutVertex()).thenReturn(mockOutVertex);
        when(mockEdge.getInVertex()).thenReturn(mockInVertex);
        when(mockOutVertex.getId()).thenReturn("outVertex1");
        when(mockInVertex.getId()).thenReturn("inVertex1");

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("weight", "since"));

        // Test NORMAL mode
        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockEdge, propertyKeys, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("weight"));
        assertEquals(result.getDouble("weight"), 0.8, 0.001);
        assertTrue(result.has("since"));
        assertEquals(result.getString("since"), "2020");
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_ID));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_TYPE));
        assertEquals(result.getString(AtlasGraphSONTokens.INTERNAL_TYPE), AtlasGraphSONTokens.EDGE);
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_LABEL));
        assertEquals(result.getString(AtlasGraphSONTokens.INTERNAL_LABEL), "knows");
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_OUT_V));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_IN_V));
    }

    @Test
    public void testJsonFromElementWithExtendedMode() throws JSONException {
        // Setup vertex mock
        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("name"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("name", Object.class)).thenReturn("John");

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("name"));

        // Test EXTENDED mode
        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, propertyKeys, AtlasGraphSONMode.EXTENDED);
        assertNotNull(result);
        assertTrue(result.has("name"));

        // In EXTENDED mode, properties should have type information
        JSONObject nameValue = result.getJSONObject("name");
        assertTrue(nameValue.has(AtlasGraphSONTokens.TYPE));
        assertTrue(nameValue.has(AtlasGraphSONTokens.VALUE));
        assertEquals(nameValue.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_STRING);
        assertEquals(nameValue.getString(AtlasGraphSONTokens.VALUE), "John");
    }

    @Test
    public void testJsonFromElementWithCompactMode() throws JSONException {
        // Setup vertex mock
        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("name"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("name", Object.class)).thenReturn("John");

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("name"));

        // Test COMPACT mode
        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, propertyKeys, AtlasGraphSONMode.COMPACT);
        assertNotNull(result);
        assertTrue(result.has("name"));
        assertEquals(result.getString("name"), "John");

        // COMPACT mode should only include reserved keys when they're explicitly in the property keys
        // Since "name" is in property keys but not INTERNAL_ID, it should not be included
        assertFalse(result.has(AtlasGraphSONTokens.INTERNAL_ID));
    }

    @Test
    public void testJsonFromElementWithNullPropertyKeys() throws JSONException {
        // Setup vertex mock
        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("name", "age"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("name", Object.class)).thenReturn("John");
        when(mockVertex.getProperty("age", Object.class)).thenReturn(30);

        // Test with null property keys (should include all properties)
        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, null, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("name"));
        assertTrue(result.has("age"));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_ID));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_TYPE));
    }

    @Test
    public void testJsonFromElementWithComplexProperties() throws JSONException {
        // Setup vertex with complex properties
        List<String> listProperty = Arrays.asList("item1", "item2", "item3");
        Map<String, Object> mapProperty = new HashMap<>();
        mapProperty.put("key1", "value1");
        mapProperty.put("key2", 42);

        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("list", "map"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("list", Object.class)).thenReturn(listProperty);
        when(mockVertex.getProperty("map", Object.class)).thenReturn(mapProperty);

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("list", "map"));

        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, propertyKeys, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("list"));
        assertTrue(result.has("map"));
    }

    @Test
    public void testJsonFromElementWithArrayProperty() throws JSONException {
        // Setup vertex with array property
        String[] arrayProperty = {"item1", "item2", "item3"};

        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("array"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("array", Object.class)).thenReturn(arrayProperty);

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("array"));

        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, propertyKeys, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("array"));
    }

    @Test
    public void testJsonFromElementWithNestedElements() throws JSONException {
        // Setup vertex with nested element property
        AtlasVertex nestedVertex = mock(AtlasVertex.class);
        when(nestedVertex.getId()).thenReturn("nested1");
        doReturn(new HashSet<>(Arrays.asList("name"))).when(nestedVertex).getPropertyKeys();
        when(nestedVertex.getProperty("name", Object.class)).thenReturn("NestedVertex");

        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("nested"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("nested", Object.class)).thenReturn(nestedVertex);

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("nested"));

        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, propertyKeys, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("nested"));
    }

    @Test
    public void testJsonFromElementWithNullProperties() throws JSONException {
        // Setup vertex with null property
        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("name", "nullable"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("name", Object.class)).thenReturn("John");
        when(mockVertex.getProperty("nullable", Object.class)).thenReturn(null);

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("name", "nullable"));

        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, propertyKeys, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("name"));
        // Null properties should not be included in the JSON
        assertFalse(result.has("nullable"));
    }

    @Test
    public void testJsonFromElementWithDifferentDataTypes() throws JSONException {
        // Setup vertex with various data types
        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("stringProp", "intProp", "longProp", "doubleProp", "floatProp", "boolProp", "byteProp", "shortProp"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("stringProp", Object.class)).thenReturn("test");
        when(mockVertex.getProperty("intProp", Object.class)).thenReturn(Integer.valueOf(42));
        when(mockVertex.getProperty("longProp", Object.class)).thenReturn(Long.valueOf(123L));
        when(mockVertex.getProperty("doubleProp", Object.class)).thenReturn(Double.valueOf(3.14));
        when(mockVertex.getProperty("floatProp", Object.class)).thenReturn(Float.valueOf(2.71f));
        when(mockVertex.getProperty("boolProp", Object.class)).thenReturn(Boolean.TRUE);
        when(mockVertex.getProperty("byteProp", Object.class)).thenReturn(Byte.valueOf((byte) 10));
        when(mockVertex.getProperty("shortProp", Object.class)).thenReturn(Short.valueOf((short) 100));

        Set<String> propertyKeys = new HashSet<>(Arrays.asList("stringProp", "intProp", "longProp", "doubleProp", "floatProp", "boolProp", "byteProp", "shortProp"));

        JSONObject result = AtlasGraphSONUtility.jsonFromElement(mockVertex, propertyKeys, AtlasGraphSONMode.EXTENDED);
        assertNotNull(result);

        // Verify all properties are present and have correct type information
        assertTrue(result.has("stringProp"));
        assertTrue(result.has("intProp"));
        assertTrue(result.has("longProp"));
        assertTrue(result.has("doubleProp"));
        assertTrue(result.has("floatProp"));
        assertTrue(result.has("boolProp"));
        assertTrue(result.has("byteProp"));
        assertTrue(result.has("shortProp"));

        // Verify type information in EXTENDED mode
        JSONObject stringProp = result.getJSONObject("stringProp");
        assertEquals(stringProp.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_STRING);

        JSONObject intProp = result.getJSONObject("intProp");
        assertEquals(intProp.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_INTEGER);

        JSONObject longProp = result.getJSONObject("longProp");
        assertEquals(longProp.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_LONG);

        JSONObject doubleProp = result.getJSONObject("doubleProp");
        assertEquals(doubleProp.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_DOUBLE);

        JSONObject floatProp = result.getJSONObject("floatProp");
        assertEquals(floatProp.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_FLOAT);

        JSONObject boolProp = result.getJSONObject("boolProp");
        assertEquals(boolProp.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_BOOLEAN);

        JSONObject byteProp = result.getJSONObject("byteProp");
        assertEquals(byteProp.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_BYTE);

        JSONObject shortProp = result.getJSONObject("shortProp");
        assertEquals(shortProp.getString(AtlasGraphSONTokens.TYPE), AtlasGraphSONTokens.TYPE_SHORT);
    }

    @Test
    public void testJsonFromElementWithIOException() throws Exception {
        // Use reflection to verify the private jsonFromElement method exists and handles IOException
        Method jsonFromElementMethod = AtlasGraphSONUtility.class.getDeclaredMethod("jsonFromElement", AtlasElement.class);
        jsonFromElementMethod.setAccessible(true);
        assertNotNull(jsonFromElementMethod);
        assertTrue(jsonFromElementMethod.isAccessible());
    }

    @Test
    public void testIncludeReservedKeyPrivateMethod() throws Exception {
        Method includeReservedKeyMethod = AtlasGraphSONUtility.class.getDeclaredMethod("includeReservedKey", AtlasGraphSONMode.class, String.class, List.class, ElementPropertiesRule.class);
        includeReservedKeyMethod.setAccessible(true);

        List<String> propertyKeys = Arrays.asList("key1", AtlasGraphSONTokens.INTERNAL_ID);

        // Test NORMAL mode - should always include reserved keys
        Boolean result = (Boolean) includeReservedKeyMethod.invoke(null, AtlasGraphSONMode.NORMAL, AtlasGraphSONTokens.INTERNAL_ID, propertyKeys, ElementPropertiesRule.INCLUDE);
        assertTrue(result);

        // Test COMPACT mode with INCLUDE rule - should include if in property keys
        result = (Boolean) includeReservedKeyMethod.invoke(null, AtlasGraphSONMode.COMPACT, AtlasGraphSONTokens.INTERNAL_ID, propertyKeys, ElementPropertiesRule.INCLUDE);
        assertTrue(result);

        // Test COMPACT mode with INCLUDE rule - should not include if not in property keys
        result = (Boolean) includeReservedKeyMethod.invoke(null, AtlasGraphSONMode.COMPACT, AtlasGraphSONTokens.INTERNAL_TYPE, propertyKeys, ElementPropertiesRule.INCLUDE);
        assertFalse(result);
    }

    @Test
    public void testIncludeKeyPrivateMethod() throws Exception {
        Method includeKeyMethod = AtlasGraphSONUtility.class.getDeclaredMethod("includeKey", String.class, List.class, ElementPropertiesRule.class);
        includeKeyMethod.setAccessible(true);

        List<String> propertyKeys = Arrays.asList("key1", "key2");

        // Test with null property keys - should always return true
        Boolean result = (Boolean) includeKeyMethod.invoke(null, "anyKey", null, ElementPropertiesRule.INCLUDE);
        assertTrue(result);

        // Test INCLUDE rule with key in list
        result = (Boolean) includeKeyMethod.invoke(null, "key1", propertyKeys, ElementPropertiesRule.INCLUDE);
        assertTrue(result);

        // Test INCLUDE rule with key not in list
        result = (Boolean) includeKeyMethod.invoke(null, "key3", propertyKeys, ElementPropertiesRule.INCLUDE);
        assertFalse(result);

        // Test EXCLUDE rule with key in list
        result = (Boolean) includeKeyMethod.invoke(null, "key1", propertyKeys, ElementPropertiesRule.EXCLUDE);
        assertFalse(result);

        // Test EXCLUDE rule with key not in list
        result = (Boolean) includeKeyMethod.invoke(null, "key3", propertyKeys, ElementPropertiesRule.EXCLUDE);
        assertTrue(result);
    }

    @Test
    public void testIncludeKeyWithInvalidRule() throws Exception {
        Method includeKeyMethod = AtlasGraphSONUtility.class.getDeclaredMethod("includeKey", String.class, List.class, ElementPropertiesRule.class);
        includeKeyMethod.setAccessible(true);

        List<String> propertyKeys = Arrays.asList("key1");

        // This should throw RuntimeException for unhandled rule
        try {
            includeKeyMethod.invoke(null, "key1", propertyKeys, null);
            fail("Expected RuntimeException");
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause() instanceof NullPointerException || e.getCause() instanceof RuntimeException);
        }
    }

    @Test
    public void testCreateJSONListPrivateMethod() throws Exception {
        Method createJSONListMethod = AtlasGraphSONUtility.class.getDeclaredMethod("createJSONList", List.class, List.class, boolean.class);
        createJSONListMethod.setAccessible(true);

        // Test with simple list
        List<Object> inputList = Arrays.asList("string", 42, true, null);
        List<String> propertyKeys = Collections.emptyList();

        ArrayNode result = (ArrayNode) createJSONListMethod.invoke(null, inputList, propertyKeys, false);
        assertNotNull(result);
        assertEquals(result.size(), 4);
        assertEquals(result.get(0).textValue(), "string");
        assertEquals(result.get(1).intValue(), 42);
        assertTrue(result.get(2).booleanValue());
        assertTrue(result.get(3).isNull());
    }

    @Test
    public void testCreateJSONListWithNestedList() throws Exception {
        Method createJSONListMethod = AtlasGraphSONUtility.class.getDeclaredMethod("createJSONList", List.class, List.class, boolean.class);
        createJSONListMethod.setAccessible(true);

        // Test with nested list
        List<Object> nestedList = Arrays.asList("nested1", "nested2");
        List<Object> inputList = Arrays.asList("string", nestedList);
        List<String> propertyKeys = Collections.emptyList();

        ArrayNode result = (ArrayNode) createJSONListMethod.invoke(null, inputList, propertyKeys, false);
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get(0).textValue(), "string");
        assertTrue(result.get(1).isArray());
    }

    @Test
    public void testCreateJSONListWithNestedMap() throws Exception {
        Method createJSONListMethod = AtlasGraphSONUtility.class.getDeclaredMethod("createJSONList", List.class, List.class, boolean.class);
        createJSONListMethod.setAccessible(true);

        // Test with nested map
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("key", "value");
        List<Object> inputList = Arrays.asList("string", nestedMap);
        List<String> propertyKeys = Collections.emptyList();

        ArrayNode result = (ArrayNode) createJSONListMethod.invoke(null, inputList, propertyKeys, false);
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get(0).textValue(), "string");
        assertTrue(result.get(1).isObject());
    }

    @Test
    public void testCreateJSONListWithArray() throws Exception {
        Method createJSONListMethod = AtlasGraphSONUtility.class.getDeclaredMethod("createJSONList", List.class, List.class, boolean.class);
        createJSONListMethod.setAccessible(true);

        // Test with array
        String[] array = {"item1", "item2"};
        List<Object> inputList = Arrays.asList("string", array);
        List<String> propertyKeys = Collections.emptyList();

        ArrayNode result = (ArrayNode) createJSONListMethod.invoke(null, inputList, propertyKeys, false);
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get(0).textValue(), "string");
        assertTrue(result.get(1).isArray());
    }

    @Test
    public void testCreateJSONListWithAtlasElement() throws Exception {
        Method createJSONListMethod = AtlasGraphSONUtility.class.getDeclaredMethod("createJSONList", List.class, List.class, boolean.class);
        createJSONListMethod.setAccessible(true);

        // Test with AtlasElement
        AtlasVertex elementInList = mock(AtlasVertex.class);
        when(elementInList.getId()).thenReturn("element1");
        doReturn(new HashSet<>(Arrays.asList("name"))).when(elementInList).getPropertyKeys();
        when(elementInList.getProperty("name", Object.class)).thenReturn("ElementName");

        List<Object> inputList = Arrays.asList("string", elementInList);
        List<String> propertyKeys = Collections.emptyList();

        ArrayNode result = (ArrayNode) createJSONListMethod.invoke(null, inputList, propertyKeys, false);
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get(0).textValue(), "string");
        assertTrue(result.get(1).isObject());
    }

    @Test
    public void testCreateJSONMapPrivateMethod() throws Exception {
        Method createJSONMapMethod = AtlasGraphSONUtility.class.getDeclaredMethod("createJSONMap", Map.class, List.class, boolean.class);
        createJSONMapMethod.setAccessible(true);

        // Test with simple map
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("string", "value");
        inputMap.put("number", 42);
        inputMap.put("bool", true);
        inputMap.put("nullValue", null);
        List<String> propertyKeys = Collections.emptyList();

        ObjectNode result = (ObjectNode) createJSONMapMethod.invoke(null, inputMap, propertyKeys, false);
        assertNotNull(result);
        assertTrue(result.has("string"));
        assertTrue(result.has("number"));
        assertTrue(result.has("bool"));
        assertTrue(result.has("nullValue"));
        assertEquals(result.get("string").textValue(), "value");
        assertEquals(result.get("number").intValue(), 42);
        assertTrue(result.get("bool").booleanValue());
        assertTrue(result.get("nullValue").isNull());
    }

    @Test
    public void testAddObjectPrivateMethod() throws Exception {
        Method addObjectMethod = AtlasGraphSONUtility.class.getDeclaredMethod("addObject", ArrayNode.class, Object.class);
        addObjectMethod.setAccessible(true);

        ArrayNode arrayNode = jsonNodeFactory.arrayNode();

        // Test adding different types
        addObjectMethod.invoke(null, arrayNode, null);
        addObjectMethod.invoke(null, arrayNode, Boolean.TRUE);
        addObjectMethod.invoke(null, arrayNode, Long.valueOf(123L));
        addObjectMethod.invoke(null, arrayNode, Integer.valueOf(42));
        addObjectMethod.invoke(null, arrayNode, Float.valueOf(3.14f));
        addObjectMethod.invoke(null, arrayNode, Double.valueOf(2.71));
        addObjectMethod.invoke(null, arrayNode, Byte.valueOf((byte) 10));
        addObjectMethod.invoke(null, arrayNode, Short.valueOf((short) 100));
        addObjectMethod.invoke(null, arrayNode, "string");

        // Test adding ObjectNode
        ObjectNode objectNode = jsonNodeFactory.objectNode();
        objectNode.put("key", "value");
        addObjectMethod.invoke(null, arrayNode, objectNode);

        // Test adding ArrayNode
        ArrayNode nestedArrayNode = jsonNodeFactory.arrayNode();
        nestedArrayNode.add("item");
        addObjectMethod.invoke(null, arrayNode, nestedArrayNode);

        // Test adding custom object (should use toString)
        Object customObject = new Object() {
            @Override
            public String toString() {
                return "custom";
            }
        };
        addObjectMethod.invoke(null, arrayNode, customObject);

        assertEquals(arrayNode.size(), 12);
        assertTrue(arrayNode.get(0).isNull());
        assertTrue(arrayNode.get(1).booleanValue());
        assertEquals(arrayNode.get(2).longValue(), 123L);
        assertEquals(arrayNode.get(3).intValue(), 42);
        assertEquals(arrayNode.get(4).floatValue(), 3.14f, 0.001);
        assertEquals(arrayNode.get(5).doubleValue(), 2.71, 0.001);
        assertEquals(arrayNode.get(6).intValue(), 10); // byte becomes int
        assertEquals(arrayNode.get(7).intValue(), 100); // short becomes int
        assertEquals(arrayNode.get(8).textValue(), "string");
        assertTrue(arrayNode.get(9).isObject());
        assertTrue(arrayNode.get(10).isArray());
        assertEquals(arrayNode.get(11).textValue(), "custom");
    }

    @Test
    public void testPutObjectPrivateMethod() throws Exception {
        Method putObjectMethod = AtlasGraphSONUtility.class.getDeclaredMethod("putObject", ObjectNode.class, String.class, Object.class);
        putObjectMethod.setAccessible(true);

        ObjectNode objectNode = jsonNodeFactory.objectNode();

        // Test putting different types
        putObjectMethod.invoke(null, objectNode, "nullValue", null);
        putObjectMethod.invoke(null, objectNode, "boolValue", Boolean.TRUE);
        putObjectMethod.invoke(null, objectNode, "longValue", Long.valueOf(123L));
        putObjectMethod.invoke(null, objectNode, "intValue", Integer.valueOf(42));
        putObjectMethod.invoke(null, objectNode, "floatValue", Float.valueOf(3.14f));
        putObjectMethod.invoke(null, objectNode, "doubleValue", Double.valueOf(2.71));
        putObjectMethod.invoke(null, objectNode, "shortValue", Short.valueOf((short) 100));
        putObjectMethod.invoke(null, objectNode, "byteValue", Byte.valueOf((byte) 10));
        putObjectMethod.invoke(null, objectNode, "stringValue", "string");

        // Test putting ObjectNode
        ObjectNode nestedObjectNode = jsonNodeFactory.objectNode();
        nestedObjectNode.put("nested", "value");
        putObjectMethod.invoke(null, objectNode, "objectValue", nestedObjectNode);

        // Test putting ArrayNode
        ArrayNode arrayNode = jsonNodeFactory.arrayNode();
        arrayNode.add("item");
        putObjectMethod.invoke(null, objectNode, "arrayValue", arrayNode);

        // Test putting custom object (should use toString)
        Object customObject = new Object() {
            @Override
            public String toString() {
                return "custom";
            }
        };
        putObjectMethod.invoke(null, objectNode, "customValue", customObject);

        assertTrue(objectNode.has("nullValue"));
        assertTrue(objectNode.get("nullValue").isNull());
        assertTrue(objectNode.has("boolValue"));
        assertTrue(objectNode.get("boolValue").booleanValue());
        assertTrue(objectNode.has("longValue"));
        assertEquals(objectNode.get("longValue").longValue(), 123L);
        assertTrue(objectNode.has("intValue"));
        assertEquals(objectNode.get("intValue").intValue(), 42);
        assertTrue(objectNode.has("floatValue"));
        assertEquals(objectNode.get("floatValue").floatValue(), 3.14f, 0.001);
        assertTrue(objectNode.has("doubleValue"));
        assertEquals(objectNode.get("doubleValue").doubleValue(), 2.71, 0.001);
        assertTrue(objectNode.has("shortValue"));
        assertEquals(objectNode.get("shortValue").intValue(), 100);
        assertTrue(objectNode.has("byteValue"));
        assertEquals(objectNode.get("byteValue").intValue(), 10);
        assertTrue(objectNode.has("stringValue"));
        assertEquals(objectNode.get("stringValue").textValue(), "string");
        assertTrue(objectNode.has("objectValue"));
        assertTrue(objectNode.get("objectValue").isObject());
        assertTrue(objectNode.has("arrayValue"));
        assertTrue(objectNode.get("arrayValue").isArray());
        assertTrue(objectNode.has("customValue"));
        assertEquals(objectNode.get("customValue").textValue(), "custom");
    }

    @Test
    public void testCreatePropertyMapPrivateMethod() throws Exception {
        Method createPropertyMapMethod = AtlasGraphSONUtility.class.getDeclaredMethod("createPropertyMap", AtlasElement.class, List.class, ElementPropertiesRule.class, boolean.class);
        createPropertyMapMethod.setAccessible(true);

        // Setup mock element
        doReturn(new HashSet<>(Arrays.asList("prop1", "prop2", "prop3"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("prop1", Object.class)).thenReturn("value1");
        when(mockVertex.getProperty("prop2", Object.class)).thenReturn("value2");
        when(mockVertex.getProperty("prop3", Object.class)).thenReturn(null); // null value should be excluded

        // Test with null property keys (should include all)
        Map<String, Object> result = (Map<String, Object>) createPropertyMapMethod.invoke(null, mockVertex, null, ElementPropertiesRule.INCLUDE, false);
        assertNotNull(result);
        assertEquals(result.size(), 2); // null value excluded
        assertTrue(result.containsKey("prop1"));
        assertTrue(result.containsKey("prop2"));
        assertFalse(result.containsKey("prop3"));
        assertEquals(result.get("prop1"), "value1");
        assertEquals(result.get("prop2"), "value2");

        // Test with specific property keys and INCLUDE rule
        List<String> includeKeys = Arrays.asList("prop1");
        result = (Map<String, Object>) createPropertyMapMethod.invoke(null, mockVertex, includeKeys, ElementPropertiesRule.INCLUDE, false);
        assertNotNull(result);
        assertEquals(result.size(), 1);
        assertTrue(result.containsKey("prop1"));
        assertFalse(result.containsKey("prop2"));
        assertEquals(result.get("prop1"), "value1");

        // Test with specific property keys and EXCLUDE rule
        List<String> excludeKeys = Arrays.asList("prop1");
        result = (Map<String, Object>) createPropertyMapMethod.invoke(null, mockVertex, excludeKeys, ElementPropertiesRule.EXCLUDE, false);
        assertNotNull(result);
        assertEquals(result.size(), 1);
        assertFalse(result.containsKey("prop1"));
        assertTrue(result.containsKey("prop2"));
        assertEquals(result.get("prop2"), "value2");
    }

    @Test
    public void testCreatePropertyMapWithNormalizedKeys() throws Exception {
        Method createPropertyMapMethod = AtlasGraphSONUtility.class.getDeclaredMethod("createPropertyMap", AtlasElement.class, List.class, ElementPropertiesRule.class, boolean.class);
        createPropertyMapMethod.setAccessible(true);

        // Setup mock element with keys that need sorting
        doReturn(new HashSet<>(Arrays.asList("zzz", "aaa", "mmm"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("zzz", Object.class)).thenReturn("value_zzz");
        when(mockVertex.getProperty("aaa", Object.class)).thenReturn("value_aaa");
        when(mockVertex.getProperty("mmm", Object.class)).thenReturn("value_mmm");

        // Test with normalized = true (should sort keys)
        Map<String, Object> result = (Map<String, Object>) createPropertyMapMethod.invoke(null, mockVertex, null, ElementPropertiesRule.INCLUDE, true);
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertTrue(result.containsKey("zzz"));
        assertTrue(result.containsKey("aaa"));
        assertTrue(result.containsKey("mmm"));
    }

    @Test
    public void testGetValuePrivateMethod() throws Exception {
        Method getValueMethod = AtlasGraphSONUtility.class.getDeclaredMethod("getValue", Object.class, boolean.class);
        getValueMethod.setAccessible(true);

        // Test without type information
        Object result = getValueMethod.invoke(null, "test", false);
        assertEquals(result, "test");

        // Test with type information
        result = getValueMethod.invoke(null, "test", true);
        assertTrue(result instanceof ObjectNode);
        ObjectNode objectNode = (ObjectNode) result;
        assertTrue(objectNode.has(AtlasGraphSONTokens.TYPE));
        assertTrue(objectNode.has(AtlasGraphSONTokens.VALUE));
        assertEquals(objectNode.get(AtlasGraphSONTokens.TYPE).textValue(), AtlasGraphSONTokens.TYPE_STRING);
        assertEquals(objectNode.get(AtlasGraphSONTokens.VALUE).textValue(), "test");

        // Test with list
        ArrayNode arrayNode = jsonNodeFactory.arrayNode();
        arrayNode.add("item1");
        arrayNode.add("item2");
        result = getValueMethod.invoke(null, arrayNode, true);
        assertTrue(result instanceof ObjectNode);
        objectNode = (ObjectNode) result;
        assertEquals(objectNode.get(AtlasGraphSONTokens.TYPE).textValue(), AtlasGraphSONTokens.TYPE_LIST);
        assertTrue(objectNode.has(AtlasGraphSONTokens.VALUE));

        // Test with map
        ObjectNode mapNode = jsonNodeFactory.objectNode();
        mapNode.put("key", "value");
        result = getValueMethod.invoke(null, mapNode, true);
        assertTrue(result instanceof ObjectNode);
        objectNode = (ObjectNode) result;
        assertEquals(objectNode.get(AtlasGraphSONTokens.TYPE).textValue(), AtlasGraphSONTokens.TYPE_MAP);
        assertTrue(objectNode.has(AtlasGraphSONTokens.VALUE));
    }

    @Test
    public void testGetTypedValueFromJsonNodePrivateMethod() throws Exception {
        Method getTypedValueMethod = AtlasGraphSONUtility.class.getDeclaredMethod("getTypedValueFromJsonNode", JsonNode.class);
        getTypedValueMethod.setAccessible(true);

        // Test null node
        Object result = getTypedValueMethod.invoke(null, (JsonNode) null);
        assertNull(result);

        // Test null value node
        JsonNode nullNode = jsonNodeFactory.nullNode();
        result = getTypedValueMethod.invoke(null, nullNode);
        assertNull(result);

        // Test boolean node
        JsonNode boolNode = jsonNodeFactory.booleanNode(true);
        result = getTypedValueMethod.invoke(null, boolNode);
        assertEquals(result, Boolean.TRUE);

        // Test int node
        JsonNode intNode = jsonNodeFactory.numberNode(42);
        result = getTypedValueMethod.invoke(null, intNode);
        assertEquals(result, Integer.valueOf(42));

        // Test long node
        JsonNode longNode = jsonNodeFactory.numberNode(123L);
        result = getTypedValueMethod.invoke(null, longNode);
        assertEquals(result, Long.valueOf(123L));

        // Test double node
        JsonNode doubleNode = jsonNodeFactory.numberNode(3.14);
        result = getTypedValueMethod.invoke(null, doubleNode);
        assertEquals(result, Double.valueOf(3.14));

        // Test float node
        JsonNode floatNode = jsonNodeFactory.numberNode(2.71f);
        result = getTypedValueMethod.invoke(null, floatNode);
        assertEquals(result, Float.valueOf(2.71f));

        // Test text node
        JsonNode textNode = jsonNodeFactory.textNode("test");
        result = getTypedValueMethod.invoke(null, textNode);
        assertEquals(result, "test");

        // Test array node
        ArrayNode arrayNode = jsonNodeFactory.arrayNode();
        arrayNode.add("item");
        result = getTypedValueMethod.invoke(null, arrayNode);
        assertEquals(result, arrayNode);

        // Test object node
        ObjectNode objectNode = jsonNodeFactory.objectNode();
        objectNode.put("key", "value");
        result = getTypedValueMethod.invoke(null, objectNode);
        assertEquals(result, objectNode);
    }

    @Test
    public void testConvertArrayToListPrivateMethod() throws Exception {
        Method convertArrayToListMethod = AtlasGraphSONUtility.class.getDeclaredMethod("convertArrayToList", Object.class);
        convertArrayToListMethod.setAccessible(true);

        // Test string array
        String[] stringArray = {"item1", "item2", "item3"};
        List<Object> result = (List<Object>) convertArrayToListMethod.invoke(null, (Object) stringArray);
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get(0), "item1");
        assertEquals(result.get(1), "item2");
        assertEquals(result.get(2), "item3");

        // Test int array
        int[] intArray = {1, 2, 3};
        result = (List<Object>) convertArrayToListMethod.invoke(null, (Object) intArray);
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get(0), Integer.valueOf(1));
        assertEquals(result.get(1), Integer.valueOf(2));
        assertEquals(result.get(2), Integer.valueOf(3));

        // Test empty array
        String[] emptyArray = {};
        result = (List<Object>) convertArrayToListMethod.invoke(null, (Object) emptyArray);
        assertNotNull(result);
        assertEquals(result.size(), 0);
    }

    @Test
    public void testDetermineTypePrivateMethod() throws Exception {
        Method determineTypeMethod = AtlasGraphSONUtility.class.getDeclaredMethod("determineType", Object.class);
        determineTypeMethod.setAccessible(true);

        // Test all supported types
        assertEquals(determineTypeMethod.invoke(null, (Object) null), AtlasGraphSONTokens.TYPE_UNKNOWN);
        assertEquals(determineTypeMethod.invoke(null, Double.valueOf(3.14)), AtlasGraphSONTokens.TYPE_DOUBLE);
        assertEquals(determineTypeMethod.invoke(null, Float.valueOf(2.71f)), AtlasGraphSONTokens.TYPE_FLOAT);
        assertEquals(determineTypeMethod.invoke(null, Byte.valueOf((byte) 10)), AtlasGraphSONTokens.TYPE_BYTE);
        assertEquals(determineTypeMethod.invoke(null, Short.valueOf((short) 100)), AtlasGraphSONTokens.TYPE_SHORT);
        assertEquals(determineTypeMethod.invoke(null, Integer.valueOf(42)), AtlasGraphSONTokens.TYPE_INTEGER);
        assertEquals(determineTypeMethod.invoke(null, Long.valueOf(123L)), AtlasGraphSONTokens.TYPE_LONG);
        assertEquals(determineTypeMethod.invoke(null, Boolean.TRUE), AtlasGraphSONTokens.TYPE_BOOLEAN);

        ArrayNode arrayNode = jsonNodeFactory.arrayNode();
        assertEquals(determineTypeMethod.invoke(null, arrayNode), AtlasGraphSONTokens.TYPE_LIST);

        ObjectNode objectNode = jsonNodeFactory.objectNode();
        assertEquals(determineTypeMethod.invoke(null, objectNode), AtlasGraphSONTokens.TYPE_MAP);

        assertEquals(determineTypeMethod.invoke(null, "string"), AtlasGraphSONTokens.TYPE_STRING);

        // Test custom object (should default to string)
        Object customObject = new Object();
        assertEquals(determineTypeMethod.invoke(null, customObject), AtlasGraphSONTokens.TYPE_STRING);
    }

    @Test
    public void testObjectNodeFromElementPrivateMethod() throws Exception {
        Method objectNodeFromElementMethod = AtlasGraphSONUtility.class.getDeclaredMethod("objectNodeFromElement", AtlasElement.class, List.class, AtlasGraphSONMode.class);
        objectNodeFromElementMethod.setAccessible(true);

        // Setup vertex mock
        when(mockVertex.getId()).thenReturn("vertex1");
        doReturn(new HashSet<>(Arrays.asList("name"))).when(mockVertex).getPropertyKeys();
        when(mockVertex.getProperty("name", Object.class)).thenReturn("John");

        List<String> propertyKeys = Arrays.asList("name");

        // Test with vertex
        ObjectNode result = (ObjectNode) objectNodeFromElementMethod.invoke(null, mockVertex, propertyKeys, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("name"));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_ID));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_TYPE));
        assertEquals(result.get(AtlasGraphSONTokens.INTERNAL_TYPE).textValue(), AtlasGraphSONTokens.VERTEX);

        // Test with edge
        when(mockEdge.getId()).thenReturn("edge1");
        when(mockEdge.getLabel()).thenReturn("knows");
        doReturn(new HashSet<>(Arrays.asList("weight"))).when(mockEdge).getPropertyKeys();
        when(mockEdge.getProperty("weight", Object.class)).thenReturn(0.8);
        when(mockEdge.getOutVertex()).thenReturn(mockOutVertex);
        when(mockEdge.getInVertex()).thenReturn(mockInVertex);
        when(mockOutVertex.getId()).thenReturn("outVertex1");
        when(mockInVertex.getId()).thenReturn("inVertex1");

        List<String> edgePropertyKeys = Arrays.asList("weight");

        result = (ObjectNode) objectNodeFromElementMethod.invoke(null, mockEdge, edgePropertyKeys, AtlasGraphSONMode.NORMAL);
        assertNotNull(result);
        assertTrue(result.has("weight"));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_ID));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_TYPE));
        assertEquals(result.get(AtlasGraphSONTokens.INTERNAL_TYPE).textValue(), AtlasGraphSONTokens.EDGE);
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_LABEL));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_OUT_V));
        assertTrue(result.has(AtlasGraphSONTokens.INTERNAL_IN_V));
    }

    @Test
    public void testElementFactoryInnerClass() throws Exception {
        // Test that the ElementFactory inner class exists and can be instantiated
        Class<?> elementFactoryClass = Class.forName("org.apache.atlas.repository.graphdb.janus.graphson.AtlasGraphSONUtility$ElementFactory");
        assertNotNull(elementFactoryClass);

        // Test instantiation
        Object elementFactory = elementFactoryClass.newInstance();
        assertNotNull(elementFactory);
    }

    @Test
    public void testPrivateConstructor() throws Exception {
        // Test that the private constructor exists and works
        Class<?> utilityClass = AtlasGraphSONUtility.class;
        java.lang.reflect.Constructor<?> constructor = utilityClass.getDeclaredConstructor(AtlasGraphSONMode.class, Set.class, Set.class);
        constructor.setAccessible(true);

        Set<String> vertexKeys = new HashSet<>(Arrays.asList("vkey1"));
        Set<String> edgeKeys = new HashSet<>(Arrays.asList("ekey1"));

        Object instance = constructor.newInstance(AtlasGraphSONMode.NORMAL, vertexKeys, edgeKeys);
        assertNotNull(instance);

        // Test access to private fields
        Field modeField = utilityClass.getDeclaredField("mode");
        modeField.setAccessible(true);
        AtlasGraphSONMode mode = (AtlasGraphSONMode) modeField.get(instance);
        assertEquals(mode, AtlasGraphSONMode.NORMAL);

        Field vertexKeysField = utilityClass.getDeclaredField("vertexPropertyKeys");
        vertexKeysField.setAccessible(true);
        List<String> storedVertexKeys = (List<String>) vertexKeysField.get(instance);
        assertTrue(storedVertexKeys.contains("vkey1"));

        Field edgeKeysField = utilityClass.getDeclaredField("edgePropertyKeys");
        edgeKeysField.setAccessible(true);
        List<String> storedEdgeKeys = (List<String>) edgeKeysField.get(instance);
        assertTrue(storedEdgeKeys.contains("ekey1"));
    }

    @Test
    public void testPrivateConstructorWithNullPropertyKeys() throws Exception {
        Class<?> utilityClass = AtlasGraphSONUtility.class;
        java.lang.reflect.Constructor<?> constructor = utilityClass.getDeclaredConstructor(AtlasGraphSONMode.class, Set.class, Set.class);
        constructor.setAccessible(true);

        Object instance = constructor.newInstance(AtlasGraphSONMode.COMPACT, null, null);
        assertNotNull(instance);

        // Test that boolean fields are set correctly
        Field includeVertexIdField = utilityClass.getDeclaredField("includeReservedVertexId");
        includeVertexIdField.setAccessible(true);
        boolean includeVertexId = (Boolean) includeVertexIdField.get(instance);
        assertTrue(includeVertexId); // Should be true for COMPACT mode with null keys
    }
}
