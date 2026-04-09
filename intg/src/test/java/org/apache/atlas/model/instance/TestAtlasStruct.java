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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNull;

public class TestAtlasStruct {
    @Test
    public void testConstructors() {
        // Test default constructor
        AtlasStruct struct = new AtlasStruct();
        assertNotNull(struct);
        assertNull(struct.getTypeName());
        assertNull(struct.getAttributes());

        // Test constructor with typeName
        AtlasStruct structWithType = new AtlasStruct("testType");
        assertEquals("testType", structWithType.getTypeName());
        assertNull(structWithType.getAttributes());

        // Test constructor with typeName and attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        AtlasStruct structWithAttrs = new AtlasStruct("testType", attributes);
        assertEquals("testType", structWithAttrs.getTypeName());
        assertEquals(attributes, structWithAttrs.getAttributes());

        // Test constructor with typeName, attrName, and attrValue
        AtlasStruct structWithSingleAttr = new AtlasStruct("testType", "attr1", "value1");
        assertEquals("testType", structWithSingleAttr.getTypeName());
        assertEquals("value1", structWithSingleAttr.getAttribute("attr1"));
    }

    @Test
    public void testMapConstructor() {
        // Test constructor with Map containing typeName and attributes
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        map.put(AtlasStruct.KEY_TYPENAME, "testType");
        map.put(AtlasStruct.KEY_ATTRIBUTES, attributes);

        AtlasStruct struct = new AtlasStruct(map);
        assertEquals("testType", struct.getTypeName());
        assertEquals(attributes, struct.getAttributes());

        // Test constructor with Map containing attributes directly
        Map<String, Object> directMap = new HashMap<>();
        directMap.put("attr1", "value1");
        directMap.put("attr2", "value2");

        AtlasStruct structDirect = new AtlasStruct(directMap);
        assertNull(structDirect.getTypeName());
        assertEquals(directMap, structDirect.getAttributes());

        // Test constructor with Map containing typeName and direct attributes
        Map<String, Object> mixedMap = new HashMap<>();
        mixedMap.put(AtlasStruct.KEY_TYPENAME, "testType");
        mixedMap.put("attr1", "value1");
        mixedMap.put("attr2", "value2");

        AtlasStruct structMixed = new AtlasStruct(mixedMap);
        assertEquals("testType", structMixed.getTypeName());
        assertEquals("value1", structMixed.getAttribute("attr1"));
        assertEquals("value2", structMixed.getAttribute("attr2"));
    }

    @Test
    public void testCopyConstructor() {
        AtlasStruct original = new AtlasStruct("testType");
        original.setAttribute("attr1", "value1");
        original.setAttribute("attr2", "value2");

        AtlasStruct copy = new AtlasStruct(original);
        assertEquals(original.getTypeName(), copy.getTypeName());
        assertEquals(original.getAttributes(), copy.getAttributes());

        // Verify it's a deep copy of attributes
        copy.setAttribute("attr3", "value3");
        assertNull(original.getAttribute("attr3"));

        // Test copy constructor with null
        AtlasStruct nullCopy = new AtlasStruct((AtlasStruct) null);
        assertNotNull(nullCopy);
        assertNull(nullCopy.getTypeName());
        assertNull(nullCopy.getAttributes());
    }

    @Test
    public void testGettersAndSetters() {
        AtlasStruct struct = new AtlasStruct();

        // Test typeName
        struct.setTypeName("testType");
        assertEquals("testType", struct.getTypeName());

        // Test attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        struct.setAttributes(attributes);
        assertEquals(attributes, struct.getAttributes());
    }

    @Test
    public void testAttributeOperations() {
        AtlasStruct struct = new AtlasStruct();

        // Test hasAttribute on null attributes
        assertFalse(struct.hasAttribute("attr1"));

        // Test getAttribute on null attributes
        assertNull(struct.getAttribute("attr1"));

        // Test setAttribute on null attributes
        struct.setAttribute("attr1", "value1");
        assertTrue(struct.hasAttribute("attr1"));
        assertEquals("value1", struct.getAttribute("attr1"));

        // Test setAttribute on existing attributes
        struct.setAttribute("attr2", "value2");
        assertEquals("value2", struct.getAttribute("attr2"));

        // Test overwriting attribute
        struct.setAttribute("attr1", "newValue1");
        assertEquals("newValue1", struct.getAttribute("attr1"));

        // Test removeAttribute
        Object removed = struct.removeAttribute("attr1");
        assertEquals("newValue1", removed);
        assertFalse(struct.hasAttribute("attr1"));

        // Test removeAttribute on non-existent attribute
        Object removedNull = struct.removeAttribute("nonExistent");
        assertNull(removedNull);

        // Test removeAttribute on null attributes map
        AtlasStruct emptyStruct = new AtlasStruct();
        Object removedFromEmpty = emptyStruct.removeAttribute("attr1");
        assertNull(removedFromEmpty);
    }

    @Test
    public void testStaticUtilityMethods() {
        // Test dumpModelObjects with AtlasStruct collection
        Collection<AtlasStruct> structs = new ArrayList<>();
        AtlasStruct struct1 = new AtlasStruct("type1");
        struct1.setAttribute("attr1", "value1");
        AtlasStruct struct2 = new AtlasStruct("type2");
        struct2.setAttribute("attr2", "value2");
        structs.add(struct1);
        structs.add(struct2);

        StringBuilder sb = AtlasStruct.dumpModelObjects(structs, null);
        assertNotNull(sb);
        String result = sb.toString();
        assertTrue(result.contains("type1"));
        assertTrue(result.contains("type2"));

        // Test with existing StringBuilder
        StringBuilder existingSb = new StringBuilder("Prefix: ");
        AtlasStruct.dumpModelObjects(structs, existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));

        // Test with empty collection
        StringBuilder emptySb = AtlasStruct.dumpModelObjects(new ArrayList<>(), null);
        assertNotNull(emptySb);
        assertEquals("", emptySb.toString());

        // Test with null collection
        StringBuilder nullSb = AtlasStruct.dumpModelObjects(null, null);
        assertNotNull(nullSb);
        assertEquals("", nullSb.toString());
    }

    @Test
    public void testDumpObjectsCollection() {
        Collection<String> objects = Arrays.asList("obj1", "obj2", "obj3");

        StringBuilder sb = AtlasStruct.dumpObjects(objects, null);
        assertNotNull(sb);
        String result = sb.toString();
        assertTrue(result.contains("obj1"));
        assertTrue(result.contains("obj2"));
        assertTrue(result.contains("obj3"));
        assertTrue(result.contains(", "));

        // Test with existing StringBuilder
        StringBuilder existingSb = new StringBuilder("Prefix: ");
        AtlasStruct.dumpObjects(objects, existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));

        // Test with empty collection
        StringBuilder emptySb = AtlasStruct.dumpObjects(new ArrayList<>(), null);
        assertNotNull(emptySb);
        assertEquals("", emptySb.toString());

        // Test with null collection
        StringBuilder nullSb = AtlasStruct.dumpObjects((Collection<?>) null, null);
        assertNotNull(nullSb);
        assertEquals("", nullSb.toString());
    }

    @Test
    public void testDumpObjectsMap() {
        Map<String, Object> objects = new HashMap<>();
        objects.put("key1", "value1");
        objects.put("key2", "value2");

        StringBuilder sb = AtlasStruct.dumpObjects(objects, null);
        assertNotNull(sb);
        String result = sb.toString();
        assertTrue(result.contains("key1:value1") || result.contains("key2:value2"));

        // Test with existing StringBuilder
        StringBuilder existingSb = new StringBuilder("Prefix: ");
        AtlasStruct.dumpObjects(objects, existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));

        // Test with empty map
        StringBuilder emptySb = AtlasStruct.dumpObjects(new HashMap<>(), null);
        assertNotNull(emptySb);
        assertEquals("", emptySb.toString());

        // Test with null map
        StringBuilder nullSb = AtlasStruct.dumpObjects((Map<?, ?>) null, null);
        assertNotNull(nullSb);
        assertEquals("", nullSb.toString());
    }

    @Test
    public void testDumpDateField() {
        StringBuilder sb = new StringBuilder();

        // Test with non-null date
        Date testDate = new Date(1000000000000L); // Fixed timestamp for consistent testing
        AtlasStruct.dumpDateField("Date: ", testDate, sb);
        String result = sb.toString();
        assertTrue(result.startsWith("Date: "));
        assertTrue(result.length() > "Date: ".length());

        // Test with null date
        StringBuilder nullSb = new StringBuilder();
        AtlasStruct.dumpDateField("Date: ", null, nullSb);
        assertEquals("Date: null", nullSb.toString());
    }

    @Test
    public void testToString() {
        AtlasStruct struct = new AtlasStruct("testType");
        struct.setAttribute("attr1", "value1");
        struct.setAttribute("attr2", "value2");

        String toString = struct.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasStruct"));
        assertTrue(toString.contains("testType"));
        assertTrue(toString.contains("attr1"));
        assertTrue(toString.contains("value1"));

        // Test toString with StringBuilder parameter
        StringBuilder sb = struct.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("AtlasStruct"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        struct.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
    }

    @Test
    public void testEquals() {
        AtlasStruct struct1 = new AtlasStruct("testType");
        struct1.setAttribute("attr1", "value1");

        AtlasStruct struct2 = new AtlasStruct("testType");
        struct2.setAttribute("attr1", "value1");

        // Test equals
        assertEquals(struct1, struct2);
        assertEquals(struct1.hashCode(), struct2.hashCode());

        // Test self equality
        assertEquals(struct1, struct1);

        // Test null equality
        assertNotEquals(struct1, null);

        // Test different class equality
        assertNotEquals(struct1, "string");

        // Test different typeName
        struct2.setTypeName("differentType");
        assertNotEquals(struct1, struct2);

        // Reset and test different attributes
        struct2.setTypeName("testType");
        struct2.setAttribute("attr1", "differentValue");
        assertNotEquals(struct1, struct2);

        // Test with null typeName
        AtlasStruct structNullType1 = new AtlasStruct();
        AtlasStruct structNullType2 = new AtlasStruct();
        assertEquals(structNullType1, structNullType2);

        // Test with null attributes
        structNullType1.setTypeName("testType");
        structNullType2.setTypeName("testType");
        assertEquals(structNullType1, structNullType2);
    }

    @Test
    public void testHashCode() {
        AtlasStruct struct1 = new AtlasStruct("testType");
        struct1.setAttribute("attr1", "value1");

        AtlasStruct struct2 = new AtlasStruct("testType");
        struct2.setAttribute("attr1", "value1");

        assertEquals(struct1.hashCode(), struct2.hashCode());

        // Change an attribute and verify hashCode changes
        struct2.setAttribute("attr1", "differentValue");
        assertNotEquals(struct1.hashCode(), struct2.hashCode());
    }

    @Test
    public void testAtlasStructsNestedClass() {
        // Test default constructor
        AtlasStruct.AtlasStructs structs = new AtlasStruct.AtlasStructs();
        assertNotNull(structs);

        // Test constructor with list
        List<AtlasStruct> structList = new ArrayList<>();
        structList.add(new AtlasStruct("type1"));
        structList.add(new AtlasStruct("type2"));

        AtlasStruct.AtlasStructs structsWithList = new AtlasStruct.AtlasStructs(structList);
        assertNotNull(structsWithList);
        assertEquals(structList, structsWithList.getList());

        // Test constructor with pagination parameters
        AtlasStruct.AtlasStructs structsWithPagination =
                new AtlasStruct.AtlasStructs(structList, 0, 10, 2, null, "name");
        assertNotNull(structsWithPagination);
        assertEquals(structList, structsWithPagination.getList());
        assertEquals(0, structsWithPagination.getStartIndex());
        assertEquals(10, structsWithPagination.getPageSize());
        assertEquals(2, structsWithPagination.getTotalCount());
    }

    @Test
    public void testConstants() {
        assertEquals("typeName", AtlasStruct.KEY_TYPENAME);
        assertEquals("attributes", AtlasStruct.KEY_ATTRIBUTES);
        assertEquals("yyyyMMdd-HH:mm:ss.SSS-Z", AtlasStruct.SERIALIZED_DATE_FORMAT_STR);

        // Test deprecated DATE_FORMATTER
        @SuppressWarnings("deprecation")
        DateFormat formatter = AtlasStruct.DATE_FORMATTER;
        assertNotNull(formatter);
    }

    @Test
    public void testNullHandling() {
        AtlasStruct struct = new AtlasStruct();

        // Test operations with null values
        struct.setTypeName(null);
        assertNull(struct.getTypeName());

        struct.setAttributes(null);
        assertNull(struct.getAttributes());

        // Test attribute operations with null attributes map
        assertFalse(struct.hasAttribute("any"));
        assertNull(struct.getAttribute("any"));
        assertNull(struct.removeAttribute("any"));

        // Test setAttribute creates new map when null
        struct.setAttribute("attr1", "value1");
        assertNotNull(struct.getAttributes());
        assertEquals("value1", struct.getAttribute("attr1"));
    }
}
