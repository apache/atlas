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

import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasRelationshipHeader {
    @Test
    public void testConstructors() {
        // Test default constructor
        AtlasRelationshipHeader header = new AtlasRelationshipHeader();
        assertNotNull(header);
        assertNull(header.getGuid());
        assertNull(header.getTypeName());
        assertEquals(AtlasEntity.Status.ACTIVE, header.getStatus());
        assertEquals(AtlasRelationshipDef.PropagateTags.NONE, header.getPropagateTags());

        // Test constructor with typeName and guid
        AtlasRelationshipHeader headerWithGuid = new AtlasRelationshipHeader("relationType", "relGuid123");
        assertEquals("relationType", headerWithGuid.getTypeName());
        assertEquals("relGuid123", headerWithGuid.getGuid());

        // Test constructor with typeName, guid, ends, and propagateTags
        AtlasObjectId end1 = new AtlasObjectId("guid1", "type1");
        AtlasObjectId end2 = new AtlasObjectId("guid2", "type2");
        AtlasRelationshipHeader headerWithEnds = new AtlasRelationshipHeader(
                "relationType", "relGuid123", end1, end2, AtlasRelationshipDef.PropagateTags.BOTH);

        assertEquals("relationType", headerWithEnds.getTypeName());
        assertEquals("relGuid123", headerWithEnds.getGuid());
        assertEquals(end1, headerWithEnds.getEnd1());
        assertEquals(end2, headerWithEnds.getEnd2());
        assertEquals(AtlasRelationshipDef.PropagateTags.BOTH, headerWithEnds.getPropagateTags());
    }

    @Test
    public void testConstructorFromAtlasRelationship() {
        // Create an AtlasRelationship
        AtlasRelationship relationship = new AtlasRelationship("relationType");
        relationship.setGuid("relGuid123");
        relationship.setLabel("testLabel");
        relationship.setStatus(AtlasRelationship.Status.ACTIVE);
        relationship.setPropagateTags(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);

        AtlasObjectId end1 = new AtlasObjectId("guid1", "type1");
        AtlasObjectId end2 = new AtlasObjectId("guid2", "type2");
        relationship.setEnd1(end1);
        relationship.setEnd2(end2);

        // Test constructor from AtlasRelationship
        AtlasRelationshipHeader header = new AtlasRelationshipHeader(relationship);
        assertEquals("relationType", header.getTypeName());
        assertEquals("relGuid123", header.getGuid());
        assertEquals("testLabel", header.getLabel());
        assertEquals(AtlasEntity.Status.ACTIVE, header.getStatus());
        assertEquals(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, header.getPropagateTags());
        assertEquals(end1, header.getEnd1());
        assertEquals(end2, header.getEnd2());

        // Test with DELETED status
        relationship.setStatus(AtlasRelationship.Status.DELETED);
        AtlasRelationshipHeader headerDeleted = new AtlasRelationshipHeader(relationship);
        assertEquals(AtlasEntity.Status.DELETED, headerDeleted.getStatus());

        // Test constructor with attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        relationship.setAttributes(attributes);
        relationship.setStatus(AtlasRelationship.Status.ACTIVE);

        AtlasRelationshipHeader headerWithAttrs = new AtlasRelationshipHeader(relationship, true);
        assertEquals(attributes, headerWithAttrs.getAttributes());

        AtlasRelationshipHeader headerWithoutAttrs = new AtlasRelationshipHeader(relationship, false);
        assertNull(headerWithoutAttrs.getAttributes());
    }

    @Test
    public void testGettersAndSetters() {
        AtlasRelationshipHeader header = new AtlasRelationshipHeader();

        // Test guid
        header.setGuid("testGuid");
        assertEquals("testGuid", header.getGuid());

        // Test status
        header.setStatus(AtlasEntity.Status.DELETED);
        assertEquals(AtlasEntity.Status.DELETED, header.getStatus());

        // Test propagateTags
        header.setPropagateTags(AtlasRelationshipDef.PropagateTags.TWO_TO_ONE);
        assertEquals(AtlasRelationshipDef.PropagateTags.TWO_TO_ONE, header.getPropagateTags());

        // Test label
        header.setLabel("testLabel");
        assertEquals("testLabel", header.getLabel());

        // Test end1 and end2
        AtlasObjectId end1 = new AtlasObjectId("guid1", "type1");
        AtlasObjectId end2 = new AtlasObjectId("guid2", "type2");
        header.setEnd1(end1);
        header.setEnd2(end2);
        assertEquals(end1, header.getEnd1());
        assertEquals(end2, header.getEnd2());
    }

    @Test
    public void testToString() {
        AtlasRelationshipHeader header = new AtlasRelationshipHeader("relationType", "relGuid123");
        header.setStatus(AtlasEntity.Status.ACTIVE);
        header.setLabel("testLabel");
        header.setPropagateTags(AtlasRelationshipDef.PropagateTags.BOTH);

        String toString = header.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("AtlasRelationshipHeader"));
        assertTrue(toString.contains("relGuid123"));
        assertTrue(toString.contains("ACTIVE"));
        assertTrue(toString.contains("testLabel"));
        assertTrue(toString.contains("BOTH"));

        // Test toString with StringBuilder parameter
        StringBuilder sb = header.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("AtlasRelationshipHeader"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        header.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
    }

    @Test
    public void testEquals() {
        AtlasRelationshipHeader header1 = new AtlasRelationshipHeader("relationType", "relGuid123");
        header1.setStatus(AtlasEntity.Status.ACTIVE);
        header1.setLabel("testLabel");

        AtlasRelationshipHeader header2 = new AtlasRelationshipHeader("relationType", "relGuid123");
        header2.setStatus(AtlasEntity.Status.ACTIVE);
        header2.setLabel("testLabel");

        // Test equals
        assertEquals(header1, header2);
        assertEquals(header1.hashCode(), header2.hashCode());

        // Test self equality
        assertEquals(header1, header1);

        // Test null equality
        assertNotEquals(header1, null);

        // Test different class equality
        assertNotEquals(header1, "string");

        // Test different guid
        header2.setGuid("differentGuid");
        assertNotEquals(header1, header2);

        // Reset and test different status
        header2.setGuid("relGuid123");
        header2.setStatus(AtlasEntity.Status.DELETED);
        assertNotEquals(header1, header2);

        // Reset and test different propagateTags
        header2.setStatus(AtlasEntity.Status.ACTIVE);
        header2.setPropagateTags(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);
        assertNotEquals(header1, header2);

        // Reset and test different label
        header2.setPropagateTags(AtlasRelationshipDef.PropagateTags.NONE);
        header2.setLabel("differentLabel");
        assertNotEquals(header1, header2);

        // Test with different ends
        header2.setLabel("testLabel");
        AtlasObjectId end1 = new AtlasObjectId("guid1", "type1");
        AtlasObjectId end2 = new AtlasObjectId("guid2", "type2");
        header1.setEnd1(end1);
        header1.setEnd2(end2);
        assertNotEquals(header1, header2);

        header2.setEnd1(end1);
        header2.setEnd2(end2);
        assertEquals(header1, header2);
    }

    @Test
    public void testHashCode() {
        AtlasRelationshipHeader header1 = new AtlasRelationshipHeader("relationType", "relGuid123");
        header1.setStatus(AtlasEntity.Status.ACTIVE);

        AtlasRelationshipHeader header2 = new AtlasRelationshipHeader("relationType", "relGuid123");
        header2.setStatus(AtlasEntity.Status.ACTIVE);

        assertEquals(header1.hashCode(), header2.hashCode());

        // Change a property and verify hashCode changes
        header2.setStatus(AtlasEntity.Status.DELETED);
        assertNotEquals(header1.hashCode(), header2.hashCode());
    }

    @Test
    public void testAtlasRelationshipHeadersNestedClass() {
        // Test default constructor
        AtlasRelationshipHeader.AtlasRelationshipHeaders headers = new AtlasRelationshipHeader.AtlasRelationshipHeaders();
        assertNotNull(headers);

        // Test constructor with list
        List<AtlasRelationshipHeader> headerList = new ArrayList<>();
        headerList.add(new AtlasRelationshipHeader("type1", "guid1"));
        headerList.add(new AtlasRelationshipHeader("type2", "guid2"));

        AtlasRelationshipHeader.AtlasRelationshipHeaders headersWithList = new AtlasRelationshipHeader.AtlasRelationshipHeaders(headerList);
        assertNotNull(headersWithList);
        assertEquals(headerList, headersWithList.getList());

        // Test constructor with pagination parameters
        AtlasRelationshipHeader.AtlasRelationshipHeaders headersWithPagination =
                new AtlasRelationshipHeader.AtlasRelationshipHeaders(headerList, 0, 10, 2, null, "name");
        assertNotNull(headersWithPagination);
        assertEquals(headerList, headersWithPagination.getList());
        assertEquals(0, headersWithPagination.getStartIndex());
        assertEquals(10, headersWithPagination.getPageSize());
        assertEquals(2, headersWithPagination.getTotalCount());
    }

    @Test
    public void testNullHandling() {
        AtlasRelationshipHeader header = new AtlasRelationshipHeader();

        header.setGuid(null);
        assertNull(header.getGuid());

        header.setLabel(null);
        assertNull(header.getLabel());

        header.setEnd1(null);
        assertNull(header.getEnd1());

        header.setEnd2(null);
        assertNull(header.getEnd2());

        assertNotNull(header.getStatus());
        assertNotNull(header.getPropagateTags());
    }

    @Test
    public void testInheritanceFromAtlasStruct() {
        AtlasRelationshipHeader header = new AtlasRelationshipHeader();

        // Test inherited methods
        header.setTypeName("testType");
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        header.setAttributes(attributes);

        assertEquals("testType", header.getTypeName());
        assertEquals(attributes, header.getAttributes());

        // Test that it can be treated as AtlasStruct
        AtlasStruct struct = header;
        assertEquals("testType", struct.getTypeName());
        assertEquals(attributes, struct.getAttributes());
    }

    @Test
    public void testStatusMapping() {
        AtlasRelationship relationship = new AtlasRelationship("relationType");

        // Test ACTIVE status mapping
        relationship.setStatus(AtlasRelationship.Status.ACTIVE);
        AtlasRelationshipHeader headerActive = new AtlasRelationshipHeader(relationship);
        assertEquals(AtlasEntity.Status.ACTIVE, headerActive.getStatus());

        // Test DELETED status mapping
        relationship.setStatus(AtlasRelationship.Status.DELETED);
        AtlasRelationshipHeader headerDeleted = new AtlasRelationshipHeader(relationship);
        assertEquals(AtlasEntity.Status.DELETED, headerDeleted.getStatus());
    }

    @Test
    public void testPropagateTagsValues() {
        AtlasRelationshipHeader header = new AtlasRelationshipHeader();

        // Test all PropagateTags enum values
        header.setPropagateTags(AtlasRelationshipDef.PropagateTags.NONE);
        assertEquals(AtlasRelationshipDef.PropagateTags.NONE, header.getPropagateTags());

        header.setPropagateTags(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO);
        assertEquals(AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, header.getPropagateTags());

        header.setPropagateTags(AtlasRelationshipDef.PropagateTags.TWO_TO_ONE);
        assertEquals(AtlasRelationshipDef.PropagateTags.TWO_TO_ONE, header.getPropagateTags());

        header.setPropagateTags(AtlasRelationshipDef.PropagateTags.BOTH);
        assertEquals(AtlasRelationshipDef.PropagateTags.BOTH, header.getPropagateTags());
    }
}
