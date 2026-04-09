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
package org.apache.atlas.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.atlas.v1.model.instance.Struct;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasJsonTest {
    @Test
    public void testToJsonWithSimpleObject() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        testMap.put("key2", "value2");

        String json = AtlasJson.toJson(testMap);
        assertNotNull(json);
        assertTrue(json.contains("key1"));
        assertTrue(json.contains("value1"));
    }

    @Test
    public void testToJsonWithTextualJsonNode() {
        TextNode textNode = new TextNode("Simple text value");
        String result = AtlasJson.toJson(textNode);
        assertEquals(result, "Simple text value");
    }

    @Test
    public void testToJsonWithNullObject() {
        String result = AtlasJson.toJson(null);
        assertEquals(result, "null");
    }

    @Test
    public void testFromLinkedHashMapWithValidData() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("typeName", "TestType");
        map.put("values", new HashMap<>());

        Struct result = AtlasJson.fromLinkedHashMap(map, Struct.class);
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestType");
    }

    @Test
    public void testFromLinkedHashMapWithNullInput() {
        Struct result = AtlasJson.fromLinkedHashMap(null, Struct.class);
        assertNull(result);
    }

    @Test
    public void testFromJsonStringWithValidJson() {
        String json = "{\"name\":\"test\",\"value\":123}";
        Map result = AtlasJson.fromJson(json, Map.class);
        assertNotNull(result);
        assertEquals(result.get("name"), "test");
        assertEquals(result.get("value"), 123);
    }

    @Test
    public void testFromJsonStringWithInvalidJson() {
        String invalidJson = "{invalid json}";
        Map result = AtlasJson.fromJson(invalidJson, Map.class);
        assertNull(result);
    }

    @Test
    public void testFromJsonStringWithNullInput() {
        Map result = AtlasJson.fromJson((String) null, Map.class);
        assertNull(result);
    }

    @Test
    public void testFromJsonWithTypeReference() {
        String json = "[\"item1\",\"item2\",\"item3\"]";
        TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
        List<String> result = AtlasJson.fromJson(json, typeRef);
        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get(0), "item1");
    }

    @Test
    public void testFromJsonWithTypeReferenceInvalidJson() {
        String invalidJson = "[invalid json]";
        TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
        List<String> result = AtlasJson.fromJson(invalidJson, typeRef);
        assertNull(result);
    }

    @Test
    public void testFromJsonWithTypeReferenceNullInput() {
        TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
        List<String> result = AtlasJson.fromJson((String) null, typeRef);
        assertNull(result);
    }

    @Test
    public void testFromJsonWithInputStream() throws IOException {
        String json = "{\"name\":\"test\",\"value\":123}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        Map result = AtlasJson.fromJson(inputStream, Map.class);
        assertNotNull(result);
        assertEquals(result.get("name"), "test");
        assertEquals(result.get("value"), 123);
    }

    @Test
    public void testFromJsonWithNullInputStream() throws IOException {
        Map result = AtlasJson.fromJson((InputStream) null, Map.class);
        assertNull(result);
    }

    @Test
    public void testToV1Json() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        String result = AtlasJson.toV1Json(testMap);
        assertNotNull(result);
        assertTrue(result.contains("key1"));
    }

    @Test
    public void testFromV1JsonString() {
        String json = "{\"name\":\"test\",\"value\":123}";
        Map result = AtlasJson.fromV1Json(json, Map.class);
        assertNotNull(result);
        assertEquals(result.get("name"), "test");
    }

    @Test
    public void testFromV1JsonWithTypeReference() {
        String json = "[\"item1\",\"item2\"]";
        TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
        List<String> result = AtlasJson.fromV1Json(json, typeRef);
        assertNotNull(result);
        assertEquals(result.size(), 2);
    }

    @Test
    public void testToV1SearchJson() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        String result = AtlasJson.toV1SearchJson(testMap);
        assertNotNull(result);
        assertTrue(result.contains("key1"));
    }

    @Test
    public void testCreateV1ObjectNode() {
        ObjectNode node = AtlasJson.createV1ObjectNode();
        assertNotNull(node);
        assertTrue(node.isObject());
    }

    @Test
    public void testCreateV1ObjectNodeWithKeyValue() {
        ObjectNode node = AtlasJson.createV1ObjectNode("testKey", "testValue");
        assertNotNull(node);
        assertTrue(node.has("testKey"));
        assertEquals(node.get("testKey").asText(), "testValue");
    }

    @Test
    public void testCreateV1ArrayNode() {
        ArrayNode node = AtlasJson.createV1ArrayNode();
        assertNotNull(node);
        assertTrue(node.isArray());
        assertEquals(node.size(), 0);
    }

    @Test
    public void testCreateV1ArrayNodeWithCollection() {
        List<String> items = Arrays.asList("item1", "item2", "item3");
        ArrayNode node = AtlasJson.createV1ArrayNode(items);
        assertNotNull(node);
        assertEquals(node.size(), 3);
    }

    @Test
    public void testParseToV1JsonNode() throws IOException {
        String json = "{\"name\":\"test\",\"value\":123}";
        JsonNode node = AtlasJson.parseToV1JsonNode(json);
        assertNotNull(node);
        assertTrue(node.isObject());
        assertEquals(node.get("name").asText(), "test");
    }

    @Test(expectedExceptions = IOException.class)
    public void testParseToV1JsonNodeWithInvalidJson() throws IOException {
        String invalidJson = "{invalid json}";
        AtlasJson.parseToV1JsonNode(invalidJson);
    }

    @Test
    public void testParseToV1ArrayNode() throws IOException {
        String json = "[\"item1\",\"item2\",\"item3\"]";
        ArrayNode node = AtlasJson.parseToV1ArrayNode(json);
        assertNotNull(node);
        assertEquals(node.size(), 3);
        assertEquals(node.get(0).asText(), "item1");
    }

    @Test(expectedExceptions = IOException.class)
    public void testParseToV1ArrayNodeWithNonArrayJson() throws IOException {
        String json = "{\"name\":\"test\"}";
        AtlasJson.parseToV1ArrayNode(json);
    }

    @Test
    public void testParseToV1ArrayNodeWithCollectionOfStrings() throws IOException {
        List<String> jsonStrings = Arrays.asList("{\"name\":\"test1\"}", "{\"name\":\"test2\"}");
        ArrayNode node = AtlasJson.parseToV1ArrayNode(jsonStrings);
        assertNotNull(node);
        assertEquals(node.size(), 2);
    }

    @Test
    public void testGetMapper() {
        assertNotNull(AtlasJson.getMapper());
    }

    @Test
    public void testPrivateConstructor() throws Exception {
        Constructor<AtlasJson> constructor = AtlasJson.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        try {
            constructor.newInstance();
        } catch (InvocationTargetException e) {
            // This is expected since the constructor is private and may block instantiation
        }
    }

    @Test
    public void testFromJsonWithStructNormalization() {
        String json = "{\"typeName\":\"TestStruct\",\"values\":{\"attr1\":\"value1\"}}";
        Struct result = AtlasJson.fromJson(json, Struct.class);
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestStruct");
    }

    @Test
    public void testFromLinkedHashMapWithStructNormalization() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("typeName", "TestStruct");
        HashMap<String, Object> values = new HashMap<>();
        values.put("attr1", "value1");
        map.put("values", values);

        Struct result = AtlasJson.fromLinkedHashMap(map, Struct.class);
        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestStruct");
    }

    @Test
    public void testToJsonWithComplexObject() {
        Map<String, Object> complexMap = new HashMap<>();
        complexMap.put("stringValue", "test");
        complexMap.put("intValue", 42);
        complexMap.put("boolValue", true);
        List<String> stringList = new ArrayList<>();
        stringList.add("item1");
        stringList.add("item2");
        complexMap.put("listValue", stringList);

        String json = AtlasJson.toJson(complexMap);
        assertNotNull(json);
        assertTrue(json.contains("stringValue"));
        assertTrue(json.contains("intValue"));
        assertTrue(json.contains("boolValue"));
        assertTrue(json.contains("listValue"));
    }

    @Test
    public void testCreateV1ArrayNodeWithEmptyCollection() {
        List<String> emptyList = new ArrayList<>();
        ArrayNode node = AtlasJson.createV1ArrayNode(emptyList);
        assertNotNull(node);
        assertEquals(node.size(), 0);
    }

    @Test
    public void testParseToV1ArrayNodeWithEmptyCollection() throws IOException {
        List<String> emptyList = new ArrayList<>();
        ArrayNode node = AtlasJson.parseToV1ArrayNode(emptyList);
        assertNotNull(node);
        assertEquals(node.size(), 0);
    }
}
