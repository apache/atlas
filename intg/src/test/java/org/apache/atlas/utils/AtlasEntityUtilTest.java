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

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasEntityUtilTest {
    @Test
    public void testFormatSoftRefValueWithTypeNameAndGuid() {
        String result = AtlasEntityUtil.formatSoftRefValue("TestType", "test-guid-123");
        assertEquals(result, "TestType:test-guid-123");
    }

    @Test
    public void testFormatSoftRefValueWithAtlasObjectId() {
        AtlasObjectId objectId = new AtlasObjectId("test-guid-123", "TestType");
        String result = AtlasEntityUtil.formatSoftRefValue(objectId);
        assertEquals(result, "TestType:test-guid-123");
    }

    @Test
    public void testFormatSoftRefValueWithListOfAtlasObjectIds() {
        List<AtlasObjectId> objIds = new ArrayList<>();
        objIds.add(new AtlasObjectId("guid1", "Type1"));
        objIds.add(new AtlasObjectId("guid2", "Type2"));

        List<String> result = AtlasEntityUtil.formatSoftRefValue(objIds);
        assertEquals(result.size(), 2);
        assertTrue(result.contains("Type1:guid1"));
        assertTrue(result.contains("Type2:guid2"));
    }

    @Test
    public void testFormatSoftRefValueWithMapOfAtlasObjectIds() {
        Map<String, AtlasObjectId> objIdMap = new HashMap<>();
        objIdMap.put("key1", new AtlasObjectId("guid1", "Type1"));
        objIdMap.put("key2", new AtlasObjectId("guid2", "Type2"));

        Map<String, String> result = AtlasEntityUtil.formatSoftRefValue(objIdMap);
        assertEquals(result.size(), 2);
        assertEquals(result.get("key1"), "Type1:guid1");
        assertEquals(result.get("key2"), "Type2:guid2");
    }

    @Test
    public void testParseSoftRefValueValidFormat() {
        String softRefValue = "TestType:test-guid-123";
        AtlasObjectId result = AtlasEntityUtil.parseSoftRefValue(softRefValue);

        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestType");
        assertEquals(result.getGuid(), "test-guid-123");
    }

    @Test
    public void testParseSoftRefValueInvalidFormat() {
        String softRefValue = "InvalidFormat";
        AtlasObjectId result = AtlasEntityUtil.parseSoftRefValue(softRefValue);
        assertNull(result);
    }

    @Test
    public void testParseSoftRefValueEmptyString() {
        AtlasObjectId result = AtlasEntityUtil.parseSoftRefValue("");
        assertNull(result);
    }

    @Test
    public void testParseSoftRefValueNullString() {
        AtlasObjectId result = AtlasEntityUtil.parseSoftRefValue((String) null);
        assertNull(result);
    }

    @Test
    public void testParseSoftRefValueWithListOfStrings() {
        List<String> softRefValues = Arrays.asList("Type1:guid1", "Type2:guid2");
        List<AtlasObjectId> result = AtlasEntityUtil.parseSoftRefValue(softRefValues);

        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get(0).getTypeName(), "Type1");
        assertEquals(result.get(0).getGuid(), "guid1");
        assertEquals(result.get(1).getTypeName(), "Type2");
        assertEquals(result.get(1).getGuid(), "guid2");
    }

    @Test
    public void testParseSoftRefValueWithEmptyList() {
        List<String> softRefValues = new ArrayList<>();
        List<AtlasObjectId> result = AtlasEntityUtil.parseSoftRefValue(softRefValues);
        assertNull(result);
    }

    @Test
    public void testParseSoftRefValueWithNullList() {
        List<String> softRefValues = null;
        List<AtlasObjectId> result = AtlasEntityUtil.parseSoftRefValue(softRefValues);
        assertNull(result);
    }

    @Test
    public void testParseSoftRefValueWithMapOfStrings() {
        Map<String, String> softRefMap = new HashMap<>();
        softRefMap.put("key1", "Type1:guid1");
        softRefMap.put("key2", "Type2:guid2");

        Map<String, AtlasObjectId> result = AtlasEntityUtil.parseSoftRefValue(softRefMap);
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get("key1").getTypeName(), "Type1");
        assertEquals(result.get("key2").getTypeName(), "Type2");
    }

    @Test
    public void testParseSoftRefValueWithEmptyMap() {
        Map<String, String> softRefMap = new HashMap<>();
        Map<String, AtlasObjectId> result = AtlasEntityUtil.parseSoftRefValue(softRefMap);
        assertNull(result);
    }

    @Test
    public void testParseSoftRefValueWithNullMap() {
        Map<String, String> softRefMap = null;
        Map<String, AtlasObjectId> result = AtlasEntityUtil.parseSoftRefValue(softRefMap);
        assertNull(result);
    }

    @Test
    public void testGetRelationshipTypeWithAtlasRelatedObjectId() {
        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId();
        relatedObjectId.setRelationshipType("TestRelationshipType");

        String result = AtlasEntityUtil.getRelationshipType(relatedObjectId);
        assertEquals(result, "TestRelationshipType");
    }

    @Test
    public void testGetRelationshipTypeWithCollection() {
        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId();
        relatedObjectId.setRelationshipType("TestRelationshipType");
        Collection<AtlasRelatedObjectId> collection = Arrays.asList(relatedObjectId);

        String result = AtlasEntityUtil.getRelationshipType(collection);
        assertEquals(result, "TestRelationshipType");
    }

    @Test
    public void testGetRelationshipTypeWithEmptyCollection() {
        Collection<Object> collection = new ArrayList<>();
        String result = AtlasEntityUtil.getRelationshipType(collection);
        assertNull(result);
    }

    @Test
    public void testGetRelationshipTypeWithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(AtlasRelatedObjectId.KEY_RELATIONSHIP_TYPE, "TestRelationshipType");

        String result = AtlasEntityUtil.getRelationshipType(map);
        assertEquals(result, "TestRelationshipType");
    }

    @Test
    public void testGetRelationshipTypeWithMapContainingRelatedObjectId() {
        AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId();
        relatedObjectId.setRelationshipType("TestRelationshipType");
        Map<String, Object> map = new HashMap<>();
        map.put("key1", relatedObjectId);

        String result = AtlasEntityUtil.getRelationshipType(map);
        assertEquals(result, "TestRelationshipType");
    }

    @Test
    public void testGetRelationshipTypeWithOtherObject() {
        String result = AtlasEntityUtil.getRelationshipType("SomeString");
        assertNull(result);
    }

    @Test
    public void testGetRelationshipTypeWithNull() {
        String result = AtlasEntityUtil.getRelationshipType(null);
        assertNull(result);
    }

    @Test
    public void testParseSoftRefValueWithInvalidListEntry() {
        List<String> softRefValues = Arrays.asList("ValidType:guid1", "InvalidFormat");
        List<AtlasObjectId> result = AtlasEntityUtil.parseSoftRefValue(softRefValues);

        assertNotNull(result);
        assertEquals(result.size(), 1); // Only valid entry should be included
        assertEquals(result.get(0).getTypeName(), "ValidType");
    }

    @Test
    public void testParseSoftRefValueWithInvalidMapEntry() {
        Map<String, String> softRefMap = new HashMap<>();
        softRefMap.put("key1", "ValidType:guid1");
        softRefMap.put("key2", "InvalidFormat");

        Map<String, AtlasObjectId> result = AtlasEntityUtil.parseSoftRefValue(softRefMap);
        assertNotNull(result);
        assertEquals(result.size(), 1); // Only valid entry should be included
        assertTrue(result.containsKey("key1"));
    }

    @Test
    public void testPrivateConstructor() throws Exception {
        Constructor<AtlasEntityUtil> constructor = AtlasEntityUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        try {
            constructor.newInstance();
        } catch (InvocationTargetException ignored) {
        }
    }

    @Test
    public void testParseSoftRefValueWithMoreThanTwoParts() {
        String softRefValue = "TestType:test:guid:123";
        AtlasObjectId result = AtlasEntityUtil.parseSoftRefValue(softRefValue);

        assertNotNull(result);
        assertEquals(result.getTypeName(), "TestType");
    }

    @Test
    public void testMetricsToTypeDataWithNullTypeData() {
        AtlasEntityUtil.metricsToTypeData(null, "TestType", null);
    }
}
