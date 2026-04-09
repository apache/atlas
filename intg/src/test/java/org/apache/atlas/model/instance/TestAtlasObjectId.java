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
package org.apache.atlas.model.instance;

import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasObjectId {
    @Test
    public void testConstructors() {
        // Test default constructor
        AtlasObjectId objectId = new AtlasObjectId();
        assertNotNull(objectId);
        assertNull(objectId.getGuid());
        assertNull(objectId.getTypeName());
        assertNull(objectId.getUniqueAttributes());

        // Test constructor with guid only
        AtlasObjectId objectIdWithGuid = new AtlasObjectId("testGuid");
        assertEquals("testGuid", objectIdWithGuid.getGuid());
        assertNull(objectIdWithGuid.getTypeName());
        assertNull(objectIdWithGuid.getUniqueAttributes());

        // Test constructor with guid and typeName
        AtlasObjectId objectIdWithGuidType = new AtlasObjectId("testGuid", "testType");
        assertEquals("testGuid", objectIdWithGuidType.getGuid());
        assertEquals("testType", objectIdWithGuidType.getTypeName());
        assertNull(objectIdWithGuidType.getUniqueAttributes());

        // Test constructor with typeName and uniqueAttributes
        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("name", "testName");
        AtlasObjectId objectIdWithTypeAttrs = new AtlasObjectId("testType", uniqueAttrs);
        assertNull(objectIdWithTypeAttrs.getGuid());
        assertEquals("testType", objectIdWithTypeAttrs.getTypeName());
        assertEquals(uniqueAttrs, objectIdWithTypeAttrs.getUniqueAttributes());

        // Test constructor with typeName, attrName, and attrValue
        AtlasObjectId objectIdWithSingleAttr = new AtlasObjectId("testType", "name", "testName");
        assertNull(objectIdWithSingleAttr.getGuid());
        assertEquals("testType", objectIdWithSingleAttr.getTypeName());
        assertEquals("testName", objectIdWithSingleAttr.getUniqueAttributes().get("name"));

        // Test constructor with all parameters
        AtlasObjectId objectIdFull = new AtlasObjectId("testGuid", "testType", uniqueAttrs);
        assertEquals("testGuid", objectIdFull.getGuid());
        assertEquals("testType", objectIdFull.getTypeName());
        assertEquals(uniqueAttrs, objectIdFull.getUniqueAttributes());
    }

    @Test
    public void testCopyConstructor() {
        AtlasObjectId original = new AtlasObjectId("testGuid", "testType");
        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("name", "testName");
        original.setUniqueAttributes(uniqueAttrs);

        AtlasObjectId copy = new AtlasObjectId(original);
        assertEquals(original.getGuid(), copy.getGuid());
        assertEquals(original.getTypeName(), copy.getTypeName());
        assertEquals(original.getUniqueAttributes(), copy.getUniqueAttributes());

        // Test copy constructor with null
        AtlasObjectId nullCopy = new AtlasObjectId((AtlasObjectId) null);
        assertNotNull(nullCopy);
        assertNull(nullCopy.getGuid());
        assertNull(nullCopy.getTypeName());
        assertNull(nullCopy.getUniqueAttributes());
    }

    @Test
    public void testMapConstructor() {
        Map<String, Object> map = new HashMap<>();
        map.put(AtlasObjectId.KEY_GUID, "testGuid");
        map.put(AtlasObjectId.KEY_TYPENAME, "testType");

        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("name", "testName");
        map.put(AtlasObjectId.KEY_UNIQUE_ATTRIBUTES, uniqueAttrs);

        AtlasObjectId objectId = new AtlasObjectId(map);
        assertEquals("testGuid", objectId.getGuid());
        assertEquals("testType", objectId.getTypeName());
        assertEquals(uniqueAttrs, objectId.getUniqueAttributes());

        // Test with null map
        AtlasObjectId nullMapObjectId = new AtlasObjectId((Map) null);
        assertNotNull(nullMapObjectId);
        assertNull(nullMapObjectId.getGuid());

        // Test with map containing non-Map unique attributes
        Map<String, Object> invalidMap = new HashMap<>();
        invalidMap.put(AtlasObjectId.KEY_GUID, "testGuid");
        invalidMap.put(AtlasObjectId.KEY_UNIQUE_ATTRIBUTES, "invalidValue");

        AtlasObjectId invalidObjectId = new AtlasObjectId(invalidMap);
        assertEquals("testGuid", invalidObjectId.getGuid());
        assertNull(invalidObjectId.getUniqueAttributes());
    }

    @Test
    public void testGettersAndSetters() {
        AtlasObjectId objectId = new AtlasObjectId();

        // Test guid
        objectId.setGuid("testGuid");
        assertEquals("testGuid", objectId.getGuid());

        // Test typeName
        objectId.setTypeName("testType");
        assertEquals("testType", objectId.getTypeName());

        // Test uniqueAttributes
        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("name", "testName");
        objectId.setUniqueAttributes(uniqueAttrs);
        assertEquals(uniqueAttrs, objectId.getUniqueAttributes());
    }

    @Test
    public void testToString() {
        AtlasObjectId objectId = new AtlasObjectId("testGuid", "testType");
        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("name", "testName");
        objectId.setUniqueAttributes(uniqueAttrs);

        String toString = objectId.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasObjectId"));
        assertTrue(toString.contains("testGuid"));
        assertTrue(toString.contains("testType"));
        assertTrue(toString.contains("name"));

        // Test toString with StringBuilder parameter
        StringBuilder sb = objectId.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("AtlasObjectId"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        objectId.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
    }

    @Test
    public void testEquals() {
        // Test equality with same guid
        AtlasObjectId objectId1 = new AtlasObjectId("testGuid", "testType");
        AtlasObjectId objectId2 = new AtlasObjectId("testGuid", "differentType");
        assertEquals(objectId1, objectId2); // Should be equal because guid is same

        // Test self equality
        assertEquals(objectId1, objectId1);

        // Test null equality
        assertNotEquals(objectId1, null);

        // Test different class equality
        assertNotEquals(objectId1, "string");

        // Test different guid
        AtlasObjectId objectId3 = new AtlasObjectId("differentGuid", "testType");
        assertNotEquals(objectId1, objectId3);

        // Test equality with empty/null guids based on typeName and uniqueAttributes
        AtlasObjectId objectIdNoGuid1 = new AtlasObjectId("testType", "name", "testName");
        AtlasObjectId objectIdNoGuid2 = new AtlasObjectId("testType", "name", "testName");
        assertEquals(objectIdNoGuid1, objectIdNoGuid2);

        // Test inequality with empty/null guids but different typeName
        AtlasObjectId objectIdNoGuid3 = new AtlasObjectId("differentType", "name", "testName");
        assertNotEquals(objectIdNoGuid1, objectIdNoGuid3);

        // Test inequality with empty/null guids but different uniqueAttributes
        AtlasObjectId objectIdNoGuid4 = new AtlasObjectId("testType", "name", "differentName");
        assertNotEquals(objectIdNoGuid1, objectIdNoGuid4);

        // Test with null guid vs empty guid
        AtlasObjectId objectIdNullGuid = new AtlasObjectId(null, "testType");
        AtlasObjectId objectIdEmptyGuid = new AtlasObjectId("", "testType");
        assertEquals(objectIdNullGuid, objectIdEmptyGuid);
    }

    @Test
    public void testHashCode() {
        // Test hashCode with guid
        AtlasObjectId objectId1 = new AtlasObjectId("testGuid", "testType");
        AtlasObjectId objectId2 = new AtlasObjectId("testGuid", "differentType");
        assertEquals(objectId1.hashCode(), objectId2.hashCode()); // Same because of same guid

        // Test hashCode without guid
        AtlasObjectId objectIdNoGuid1 = new AtlasObjectId("testType", "name", "testName");
        AtlasObjectId objectIdNoGuid2 = new AtlasObjectId("testType", "name", "testName");
        assertEquals(objectIdNoGuid1.hashCode(), objectIdNoGuid2.hashCode());

        // Test different hashCode with different guid
        AtlasObjectId objectId3 = new AtlasObjectId("differentGuid", "testType");
        assertNotEquals(objectId1.hashCode(), objectId3.hashCode());
    }

    @Test
    public void testPrivateCreateMapMethod() throws Exception {
        // Test private static method createMap using reflection
        Method createMapMethod = AtlasObjectId.class.getDeclaredMethod("createMap", String.class, Object.class);
        createMapMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) createMapMethod.invoke(null, "testKey", "testValue");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("testValue", result.get("testKey"));
    }

    @Test
    public void testAtlasObjectIdsNestedClass() {
        // Test default constructor
        AtlasObjectId.AtlasObjectIds objectIds = new AtlasObjectId.AtlasObjectIds();
        assertNotNull(objectIds);

        // Test constructor with list
        List<AtlasObjectId> objectIdList = new ArrayList<>();
        objectIdList.add(new AtlasObjectId("guid1", "type1"));
        objectIdList.add(new AtlasObjectId("guid2", "type2"));

        AtlasObjectId.AtlasObjectIds objectIdsWithList = new AtlasObjectId.AtlasObjectIds(objectIdList);
        assertNotNull(objectIdsWithList);
        assertEquals(objectIdList, objectIdsWithList.getList());

        // Test constructor with pagination parameters
        AtlasObjectId.AtlasObjectIds objectIdsWithPagination =
                new AtlasObjectId.AtlasObjectIds(objectIdList, 0, 10, 2, null, "name");
        assertNotNull(objectIdsWithPagination);
        assertEquals(objectIdList, objectIdsWithPagination.getList());
        assertEquals(0, objectIdsWithPagination.getStartIndex());
        assertEquals(10, objectIdsWithPagination.getPageSize());
        assertEquals(2, objectIdsWithPagination.getTotalCount());
    }

    @Test
    public void testConstants() {
        assertEquals("guid", AtlasObjectId.KEY_GUID);
        assertEquals("typeName", AtlasObjectId.KEY_TYPENAME);
        assertEquals("uniqueAttributes", AtlasObjectId.KEY_UNIQUE_ATTRIBUTES);
    }

    @Test
    public void testNullHandling() {
        AtlasObjectId objectId = new AtlasObjectId();

        // Test setting null values
        objectId.setGuid(null);
        assertNull(objectId.getGuid());

        objectId.setTypeName(null);
        assertNull(objectId.getTypeName());

        objectId.setUniqueAttributes(null);
        assertNull(objectId.getUniqueAttributes());

        // Test equality with null values
        AtlasObjectId objectId2 = new AtlasObjectId();
        assertEquals(objectId, objectId2);
    }

    @Test
    public void testEqualityEdgeCases() {
        // Test with one having guid and other not having guid
        AtlasObjectId withGuid = new AtlasObjectId("testGuid", "testType");
        AtlasObjectId withoutGuid = new AtlasObjectId("testType", "name", "testName");
        assertNotEquals(withGuid, withoutGuid);

        // Test with both having empty guids but same type/attributes
        AtlasObjectId emptyGuid1 = new AtlasObjectId("", "testType");
        Map<String, Object> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("name", "testName");
        emptyGuid1.setUniqueAttributes(uniqueAttrs);
        AtlasObjectId emptyGuid2 = new AtlasObjectId("", "testType");
        emptyGuid2.setUniqueAttributes(uniqueAttrs);
        assertEquals(emptyGuid1, emptyGuid2);

        // Test with null unique attributes
        AtlasObjectId nullAttrs1 = new AtlasObjectId("", "testType");
        AtlasObjectId nullAttrs2 = new AtlasObjectId("", "testType");
        assertEquals(nullAttrs1, nullAttrs2);
    }
}
