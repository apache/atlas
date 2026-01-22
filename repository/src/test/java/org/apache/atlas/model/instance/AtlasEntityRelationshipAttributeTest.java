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
package org.apache.atlas.model.instance;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * Tests for AtlasEntity relationship attributes deduplication.
 * This addresses the unlink-relink scenario where the same entity should not appear
 * in both addedRelationshipAttributes and removedRelationshipAttributes.
 */
public class AtlasEntityRelationshipAttributeTest {

    private static final String README_ATTR = "readme";
    private static final String README_GUID = "test-readme-guid-123";
    private static final String README_TYPE = "Readme";

    /**
     * Test that when setting addedRelationshipAttribute with a Map value,
     * the same entity is removed from removedRelationshipAttributes if present.
     */
    @Test
    public void testSetAddedRelationshipAttributeRemovesDuplicateFromRemoved_MapValue() {
        AtlasEntity entity = new AtlasEntity("TestEntity");

        // First, set the readme as removed (simulating unlink)
        Map<String, String> removedReadme = new HashMap<>();
        removedReadme.put("guid", README_GUID);
        removedReadme.put("typeName", README_TYPE);
        entity.setRemovedRelationshipAttribute(README_ATTR, removedReadme);

        // Verify removed is set
        assertNotNull(entity.getRemovedRelationshipAttributes());
        assertEquals(entity.getRemovedRelationshipAttributes().get(README_ATTR), removedReadme);

        // Now, set the same readme as added (simulating re-link)
        Map<String, String> addedReadme = new HashMap<>();
        addedReadme.put("guid", README_GUID);
        addedReadme.put("typeName", README_TYPE);
        entity.setAddedRelationshipAttribute(README_ATTR, addedReadme);

        // Verify added is set
        assertNotNull(entity.getAddedRelationshipAttributes());
        assertEquals(entity.getAddedRelationshipAttributes().get(README_ATTR), addedReadme);

        // Verify removed is now empty (deduplication occurred)
        assertNull(entity.getRemovedRelationshipAttributes().get(README_ATTR),
                "Same entity should not appear in both added and removed relationship attributes");
    }

    /**
     * Test that when setting addedRelationshipAttribute with AtlasObjectId value,
     * the same entity is removed from removedRelationshipAttributes if present.
     */
    @Test
    public void testSetAddedRelationshipAttributeRemovesDuplicateFromRemoved_AtlasObjectIdValue() {
        AtlasEntity entity = new AtlasEntity("TestEntity");

        // First, set the readme as removed (simulating unlink)
        AtlasObjectId removedReadme = new AtlasObjectId(README_GUID, README_TYPE);
        entity.setRemovedRelationshipAttribute(README_ATTR, removedReadme);

        // Verify removed is set
        assertNotNull(entity.getRemovedRelationshipAttributes());
        assertEquals(entity.getRemovedRelationshipAttributes().get(README_ATTR), removedReadme);

        // Now, set the same readme as added (simulating re-link)
        AtlasObjectId addedReadme = new AtlasObjectId(README_GUID, README_TYPE);
        entity.setAddedRelationshipAttribute(README_ATTR, addedReadme);

        // Verify added is set
        assertNotNull(entity.getAddedRelationshipAttributes());
        assertEquals(entity.getAddedRelationshipAttributes().get(README_ATTR), addedReadme);

        // Verify removed is now empty (deduplication occurred)
        assertNull(entity.getRemovedRelationshipAttributes().get(README_ATTR),
                "Same entity should not appear in both added and removed relationship attributes");
    }

    /**
     * Test that addOrAppendAddedRelationshipAttribute removes matching entries from
     * removedRelationshipAttributes list.
     */
    @Test
    public void testAddOrAppendAddedRelationshipAttributeRemovesDuplicateFromRemovedList() {
        AtlasEntity entity = new AtlasEntity("TestEntity");

        // First, append the readme to removed list (simulating unlink)
        AtlasObjectId removedReadme = new AtlasObjectId(README_GUID, README_TYPE);
        entity.addOrAppendRemovedRelationshipAttribute(README_ATTR, removedReadme);

        // Verify removed list has the item
        assertNotNull(entity.getRemovedRelationshipAttributes());
        Object removedValue = entity.getRemovedRelationshipAttributes().get(README_ATTR);
        assertNotNull(removedValue);
        assertTrue(removedValue instanceof List);
        assertEquals(((List<?>) removedValue).size(), 1);

        // Now, append the same readme to added list (simulating re-link)
        AtlasObjectId addedReadme = new AtlasObjectId(README_GUID, README_TYPE);
        entity.addOrAppendAddedRelationshipAttribute(README_ATTR, addedReadme);

        // Verify added list has the item
        assertNotNull(entity.getAddedRelationshipAttributes());
        Object addedValue = entity.getAddedRelationshipAttributes().get(README_ATTR);
        assertNotNull(addedValue);
        assertTrue(addedValue instanceof List);
        assertEquals(((List<?>) addedValue).size(), 1);

        // Verify removed list is now empty or the attribute is removed (deduplication occurred)
        Object removedAfterAdd = entity.getRemovedRelationshipAttributes().get(README_ATTR);
        assertTrue(removedAfterAdd == null || (removedAfterAdd instanceof List && ((List<?>) removedAfterAdd).isEmpty()),
                "Same entity should be removed from removedRelationshipAttributes when added");
    }

    /**
     * Test that different guids are NOT deduplicated.
     */
    @Test
    public void testDifferentGuidsAreNotDeduplicated() {
        AtlasEntity entity = new AtlasEntity("TestEntity");
        String differentGuid = "different-readme-guid-456";

        // First, set a readme as removed
        Map<String, String> removedReadme = new HashMap<>();
        removedReadme.put("guid", README_GUID);
        removedReadme.put("typeName", README_TYPE);
        entity.setRemovedRelationshipAttribute(README_ATTR, removedReadme);

        // Now, set a DIFFERENT readme as added
        Map<String, String> addedReadme = new HashMap<>();
        addedReadme.put("guid", differentGuid);
        addedReadme.put("typeName", README_TYPE);
        entity.setAddedRelationshipAttribute(README_ATTR, addedReadme);

        // Verify both added and removed have their respective values
        assertNotNull(entity.getAddedRelationshipAttributes().get(README_ATTR));
        assertNotNull(entity.getRemovedRelationshipAttributes().get(README_ATTR),
                "Different guids should NOT be deduplicated");
        assertEquals(((Map<?, ?>) entity.getRemovedRelationshipAttributes().get(README_ATTR)).get("guid"), README_GUID);
    }

    /**
     * Test that deduplication works when removed has Map and added has AtlasObjectId.
     */
    @Test
    public void testDeduplicationWithMixedTypes() {
        AtlasEntity entity = new AtlasEntity("TestEntity");

        // First, set the readme as removed using Map
        Map<String, String> removedReadme = new HashMap<>();
        removedReadme.put("guid", README_GUID);
        removedReadme.put("typeName", README_TYPE);
        entity.setRemovedRelationshipAttribute(README_ATTR, removedReadme);

        // Now, set the same readme as added using AtlasObjectId
        AtlasObjectId addedReadme = new AtlasObjectId(README_GUID, README_TYPE);
        entity.setAddedRelationshipAttribute(README_ATTR, addedReadme);

        // Verify deduplication occurred
        assertNotNull(entity.getAddedRelationshipAttributes().get(README_ATTR));
        assertNull(entity.getRemovedRelationshipAttributes().get(README_ATTR),
                "Same guid should be deduplicated regardless of value type");
    }

    /**
     * Test that deduplication doesn't fail when removedRelationshipAttributes is null.
     */
    @Test
    public void testSetAddedWhenRemovedIsNull() {
        AtlasEntity entity = new AtlasEntity("TestEntity");

        // removedRelationshipAttributes is null by default
        assertNull(entity.getRemovedRelationshipAttributes());

        // Setting added should not throw exception
        Map<String, String> addedReadme = new HashMap<>();
        addedReadme.put("guid", README_GUID);
        addedReadme.put("typeName", README_TYPE);
        entity.setAddedRelationshipAttribute(README_ATTR, addedReadme);

        // Verify added is set correctly
        assertNotNull(entity.getAddedRelationshipAttributes());
        assertEquals(entity.getAddedRelationshipAttributes().get(README_ATTR), addedReadme);
    }

    /**
     * Test deduplication in list when there are multiple items and only one matches.
     */
    @Test
    public void testPartialDeduplicationInList() {
        AtlasEntity entity = new AtlasEntity("TestEntity");
        String otherGuid = "other-guid-789";

        // Add two items to removed list
        entity.addOrAppendRemovedRelationshipAttribute(README_ATTR, new AtlasObjectId(README_GUID, README_TYPE));
        entity.addOrAppendRemovedRelationshipAttribute(README_ATTR, new AtlasObjectId(otherGuid, README_TYPE));

        // Verify removed list has 2 items
        List<?> removedList = (List<?>) entity.getRemovedRelationshipAttributes().get(README_ATTR);
        assertEquals(removedList.size(), 2);

        // Add one of them to added (should only remove matching one)
        entity.addOrAppendAddedRelationshipAttribute(README_ATTR, new AtlasObjectId(README_GUID, README_TYPE));

        // Verify removed list now has only 1 item (the non-matching one)
        removedList = (List<?>) entity.getRemovedRelationshipAttributes().get(README_ATTR);
        assertEquals(removedList.size(), 1, "Only matching guid should be removed from list");
    }
}
