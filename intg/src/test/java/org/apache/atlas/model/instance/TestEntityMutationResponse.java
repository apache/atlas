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

import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
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

public class TestEntityMutationResponse {
    @Test
    public void testConstructors() {
        // Test default constructor
        EntityMutationResponse response = new EntityMutationResponse();
        assertNotNull(response);
        assertNull(response.getMutatedEntities());
        assertNull(response.getGuidAssignments());

        // Test constructor with mutated entities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        List<AtlasEntityHeader> createdEntities = new ArrayList<>();
        createdEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        mutatedEntities.put(EntityOperation.CREATE, createdEntities);

        EntityMutationResponse responseWithEntities = new EntityMutationResponse(mutatedEntities);
        assertNotNull(responseWithEntities);
        assertEquals(mutatedEntities, responseWithEntities.getMutatedEntities());
        assertNull(responseWithEntities.getGuidAssignments());
    }

    @Test
    public void testGettersAndSetters() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test setting and getting mutated entities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        List<AtlasEntityHeader> createdEntities = new ArrayList<>();
        createdEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        createdEntities.add(new AtlasEntityHeader("type2", "guid2", null));
        mutatedEntities.put(EntityOperation.CREATE, createdEntities);

        List<AtlasEntityHeader> updatedEntities = new ArrayList<>();
        updatedEntities.add(new AtlasEntityHeader("type3", "guid3", null));
        mutatedEntities.put(EntityOperation.UPDATE, updatedEntities);

        response.setMutatedEntities(mutatedEntities);
        assertEquals(mutatedEntities, response.getMutatedEntities());

        // Test setting and getting guid assignments
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("tempGuid1", "realGuid1");
        guidAssignments.put("tempGuid2", "realGuid2");

        response.setGuidAssignments(guidAssignments);
        assertEquals(guidAssignments, response.getGuidAssignments());
    }

    @Test
    public void testGetEntitiesByOperation() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getEntitiesByOperation(EntityOperation.CREATE));
        assertNull(response.getEntitiesByOperation(EntityOperation.UPDATE));
        assertNull(response.getEntitiesByOperation(EntityOperation.PARTIAL_UPDATE));
        assertNull(response.getEntitiesByOperation(EntityOperation.DELETE));

        // Test with populated mutatedEntities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        List<AtlasEntityHeader> createdEntities = new ArrayList<>();
        createdEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        mutatedEntities.put(EntityOperation.CREATE, createdEntities);

        List<AtlasEntityHeader> deletedEntities = new ArrayList<>();
        deletedEntities.add(new AtlasEntityHeader("type2", "guid2", null));
        mutatedEntities.put(EntityOperation.DELETE, deletedEntities);

        response.setMutatedEntities(mutatedEntities);

        assertEquals(createdEntities, response.getEntitiesByOperation(EntityOperation.CREATE));
        assertEquals(deletedEntities, response.getEntitiesByOperation(EntityOperation.DELETE));
        assertNull(response.getEntitiesByOperation(EntityOperation.UPDATE));
        assertNull(response.getEntitiesByOperation(EntityOperation.PARTIAL_UPDATE));
    }

    @Test
    public void testGetCreatedEntities() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getCreatedEntities());

        // Test with populated mutatedEntities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        List<AtlasEntityHeader> createdEntities = new ArrayList<>();
        createdEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        createdEntities.add(new AtlasEntityHeader("type2", "guid2", null));
        mutatedEntities.put(EntityOperation.CREATE, createdEntities);

        response.setMutatedEntities(mutatedEntities);
        assertEquals(createdEntities, response.getCreatedEntities());

        // Test when CREATE operation is not present
        mutatedEntities.remove(EntityOperation.CREATE);
        response.setMutatedEntities(mutatedEntities);
        assertNull(response.getCreatedEntities());
    }

    @Test
    public void testGetUpdatedEntities() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getUpdatedEntities());

        // Test with populated mutatedEntities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        List<AtlasEntityHeader> updatedEntities = new ArrayList<>();
        updatedEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        updatedEntities.add(new AtlasEntityHeader("type2", "guid2", null));
        mutatedEntities.put(EntityOperation.UPDATE, updatedEntities);

        response.setMutatedEntities(mutatedEntities);
        assertEquals(updatedEntities, response.getUpdatedEntities());

        // Test when UPDATE operation is not present
        mutatedEntities.remove(EntityOperation.UPDATE);
        response.setMutatedEntities(mutatedEntities);
        assertNull(response.getUpdatedEntities());
    }

    @Test
    public void testGetPartialUpdatedEntities() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getPartialUpdatedEntities());

        // Test with populated mutatedEntities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        List<AtlasEntityHeader> partialUpdatedEntities = new ArrayList<>();
        partialUpdatedEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        mutatedEntities.put(EntityOperation.PARTIAL_UPDATE, partialUpdatedEntities);

        response.setMutatedEntities(mutatedEntities);
        assertEquals(partialUpdatedEntities, response.getPartialUpdatedEntities());

        // Test when PARTIAL_UPDATE operation is not present
        mutatedEntities.remove(EntityOperation.PARTIAL_UPDATE);
        response.setMutatedEntities(mutatedEntities);
        assertNull(response.getPartialUpdatedEntities());
    }

    @Test
    public void testGetDeletedEntities() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getDeletedEntities());

        // Test with populated mutatedEntities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        List<AtlasEntityHeader> deletedEntities = new ArrayList<>();
        deletedEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        deletedEntities.add(new AtlasEntityHeader("type2", "guid2", null));
        mutatedEntities.put(EntityOperation.DELETE, deletedEntities);

        response.setMutatedEntities(mutatedEntities);
        assertEquals(deletedEntities, response.getDeletedEntities());

        // Test when DELETE operation is not present
        mutatedEntities.remove(EntityOperation.DELETE);
        response.setMutatedEntities(mutatedEntities);
        assertNull(response.getDeletedEntities());
    }

    @Test
    public void testGetPurgedEntities() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getPurgedEntities());

        // Test with populated mutatedEntities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        List<AtlasEntityHeader> purgedEntities = new ArrayList<>();
        purgedEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        mutatedEntities.put(EntityOperation.PURGE, purgedEntities);

        response.setMutatedEntities(mutatedEntities);
        assertEquals(purgedEntities, response.getPurgedEntities());

        // Test when PURGE operation is not present
        mutatedEntities.remove(EntityOperation.PURGE);
        response.setMutatedEntities(mutatedEntities);
        assertNull(response.getPurgedEntities());
    }

    @Test
    public void testGetPurgedEntitiesIds() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getPurgedEntitiesIds());

        // Test with empty purged entities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        mutatedEntities.put(EntityOperation.PURGE, new ArrayList<>());
        response.setMutatedEntities(mutatedEntities);
        assertNull(response.getPurgedEntitiesIds());

        // Test with populated purged entities
        List<AtlasEntityHeader> purgedEntities = new ArrayList<>();
        purgedEntities.add(new AtlasEntityHeader("type1", "guid1", null));
        purgedEntities.add(new AtlasEntityHeader("type2", "guid2", null));
        purgedEntities.add(new AtlasEntityHeader("type3", "guid3", null));
        mutatedEntities.put(EntityOperation.PURGE, purgedEntities);
        response.setMutatedEntities(mutatedEntities);

        String purgedIds = response.getPurgedEntitiesIds();
        assertNotNull(purgedIds);
        assertTrue(purgedIds.contains("guid1"));
        assertTrue(purgedIds.contains("guid2"));
        assertTrue(purgedIds.contains("guid3"));
        assertTrue(purgedIds.contains(","));
    }

    @Test
    public void testGetFirstEntityCreated() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getFirstEntityCreated());

        // Test with empty created entities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        mutatedEntities.put(EntityOperation.CREATE, new ArrayList<>());
        response.setMutatedEntities(mutatedEntities);
        assertNull(response.getFirstEntityCreated());

        // Test with populated created entities
        List<AtlasEntityHeader> createdEntities = new ArrayList<>();
        AtlasEntityHeader firstEntity = new AtlasEntityHeader("type1", "guid1", null);
        AtlasEntityHeader secondEntity = new AtlasEntityHeader("type2", "guid2", null);
        createdEntities.add(firstEntity);
        createdEntities.add(secondEntity);
        mutatedEntities.put(EntityOperation.CREATE, createdEntities);
        response.setMutatedEntities(mutatedEntities);

        assertEquals(firstEntity, response.getFirstEntityCreated());
    }

    @Test
    public void testGetFirstEntityUpdated() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getFirstEntityUpdated());

        // Test with empty updated entities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        mutatedEntities.put(EntityOperation.UPDATE, new ArrayList<>());
        response.setMutatedEntities(mutatedEntities);
        assertNull(response.getFirstEntityUpdated());

        // Test with populated updated entities
        List<AtlasEntityHeader> updatedEntities = new ArrayList<>();
        AtlasEntityHeader firstEntity = new AtlasEntityHeader("type1", "guid1", null);
        AtlasEntityHeader secondEntity = new AtlasEntityHeader("type2", "guid2", null);
        updatedEntities.add(firstEntity);
        updatedEntities.add(secondEntity);
        mutatedEntities.put(EntityOperation.UPDATE, updatedEntities);
        response.setMutatedEntities(mutatedEntities);

        assertEquals(firstEntity, response.getFirstEntityUpdated());
    }

    @Test
    public void testGetFirstEntityPartialUpdated() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test with null mutatedEntities
        assertNull(response.getFirstEntityPartialUpdated());

        // Test with populated partial updated entities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        List<AtlasEntityHeader> partialUpdatedEntities = new ArrayList<>();
        AtlasEntityHeader firstEntity = new AtlasEntityHeader("type1", "guid1", null);
        partialUpdatedEntities.add(firstEntity);
        mutatedEntities.put(EntityOperation.PARTIAL_UPDATE, partialUpdatedEntities);
        response.setMutatedEntities(mutatedEntities);

        assertEquals(firstEntity, response.getFirstEntityPartialUpdated());
    }

    @Test
    public void testGetEntityByTypeName() {
        EntityMutationResponse response = new EntityMutationResponse();
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        // Create entities of different types
        List<AtlasEntityHeader> createdEntities = new ArrayList<>();
        AtlasEntityHeader tableEntity = new AtlasEntityHeader("Table", "table-guid", null);
        AtlasEntityHeader columnEntity = new AtlasEntityHeader("Column", "column-guid", null);
        AtlasEntityHeader databaseEntity = new AtlasEntityHeader("Database", "database-guid", null);
        createdEntities.add(tableEntity);
        createdEntities.add(columnEntity);
        createdEntities.add(databaseEntity);
        mutatedEntities.put(EntityOperation.CREATE, createdEntities);

        List<AtlasEntityHeader> deletedEntities = new ArrayList<>();
        AtlasEntityHeader deletedTable = new AtlasEntityHeader("Table", "deleted-table-guid", null);
        deletedEntities.add(deletedTable);
        mutatedEntities.put(EntityOperation.DELETE, deletedEntities);

        response.setMutatedEntities(mutatedEntities);

        // Test getFirstCreatedEntityByTypeName
        assertEquals(tableEntity, response.getFirstCreatedEntityByTypeName("Table"));
        assertEquals(columnEntity, response.getFirstCreatedEntityByTypeName("Column"));
        assertEquals(databaseEntity, response.getFirstCreatedEntityByTypeName("Database"));
        assertNull(response.getFirstCreatedEntityByTypeName("NonExistentType"));

        // Test getFirstDeletedEntityByTypeName
        assertEquals(deletedTable, response.getFirstDeletedEntityByTypeName("Table"));
        assertNull(response.getFirstDeletedEntityByTypeName("Column"));

        // Test getCreatedEntitiesByTypeName
        List<AtlasEntityHeader> tableEntities = response.getCreatedEntitiesByTypeName("Table");
        assertEquals(1, tableEntities.size());
        assertEquals(tableEntity, tableEntities.get(0));

        List<AtlasEntityHeader> columnEntities = response.getCreatedEntitiesByTypeName("Column");
        assertEquals(1, columnEntities.size());
        assertEquals(columnEntity, columnEntities.get(0));

        List<AtlasEntityHeader> nonExistentEntities = response.getCreatedEntitiesByTypeName("NonExistentType");
        assertTrue(nonExistentEntities.isEmpty());
    }

    @Test
    public void testGetEntityByTypeNameAndAttribute() {
        EntityMutationResponse response = new EntityMutationResponse();
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();

        // Create entities with attributes
        List<AtlasEntityHeader> createdEntities = new ArrayList<>();

        AtlasEntityHeader tableEntity = new AtlasEntityHeader("Table", "table-guid", null);
        Map<String, Object> tableAttributes = new HashMap<>();
        tableAttributes.put("name", "customer_table");
        tableAttributes.put("qualifiedName", "customer_table@cluster");
        tableEntity.setAttributes(tableAttributes);
        createdEntities.add(tableEntity);

        AtlasEntityHeader columnEntity = new AtlasEntityHeader("Column", "column-guid", null);
        Map<String, Object> columnAttributes = new HashMap<>();
        columnAttributes.put("name", "customer_id");
        columnAttributes.put("qualifiedName", "customer_table.customer_id@cluster");
        columnEntity.setAttributes(columnAttributes);
        createdEntities.add(columnEntity);

        mutatedEntities.put(EntityOperation.CREATE, createdEntities);

        List<AtlasEntityHeader> updatedEntities = new ArrayList<>();
        AtlasEntityHeader updatedTable = new AtlasEntityHeader("Table", "updated-table-guid", null);
        Map<String, Object> updatedTableAttributes = new HashMap<>();
        updatedTableAttributes.put("name", "order_table");
        updatedTable.setAttributes(updatedTableAttributes);
        updatedEntities.add(updatedTable);
        mutatedEntities.put(EntityOperation.UPDATE, updatedEntities);

        response.setMutatedEntities(mutatedEntities);

        // Test getCreatedEntityByTypeNameAndAttribute
        assertEquals(tableEntity, response.getCreatedEntityByTypeNameAndAttribute("Table", "name", "customer_table"));
        assertEquals(columnEntity, response.getCreatedEntityByTypeNameAndAttribute("Column", "name", "customer_id"));
        assertNull(response.getCreatedEntityByTypeNameAndAttribute("Table", "name", "non_existent_table"));
        assertNull(response.getCreatedEntityByTypeNameAndAttribute("NonExistentType", "name", "customer_table"));

        // Test getUpdatedEntityByTypeNameAndAttribute
        assertEquals(updatedTable, response.getUpdatedEntityByTypeNameAndAttribute("Table", "name", "order_table"));
        assertNull(response.getUpdatedEntityByTypeNameAndAttribute("Table", "name", "non_existent_table"));
    }

    @Test
    public void testAddEntity() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test adding entities to empty response
        AtlasEntityHeader entity1 = new AtlasEntityHeader("type1", "guid1", null);
        AtlasEntityHeader entity2 = new AtlasEntityHeader("type2", "guid2", null);
        AtlasEntityHeader entity3 = new AtlasEntityHeader("type3", "guid3", null);

        response.addEntity(EntityOperation.CREATE, entity1);
        response.addEntity(EntityOperation.UPDATE, entity2);
        response.addEntity(EntityOperation.DELETE, entity3);

        assertNotNull(response.getMutatedEntities());
        assertEquals(1, response.getCreatedEntities().size());
        assertEquals(1, response.getUpdatedEntities().size());
        assertEquals(1, response.getDeletedEntities().size());

        assertEquals(entity1, response.getCreatedEntities().get(0));
        assertEquals(entity2, response.getUpdatedEntities().get(0));
        assertEquals(entity3, response.getDeletedEntities().get(0));

        // Test adding duplicate entity (should not be added)
        response.addEntity(EntityOperation.CREATE, entity1);
        assertEquals(1, response.getCreatedEntities().size());

        // Test the logic where UPDATE is converted to CREATE if entity was already created
        AtlasEntityHeader entity4 = new AtlasEntityHeader("type4", "guid4", null);
        response.addEntity(EntityOperation.CREATE, entity4);
        assertEquals(2, response.getCreatedEntities().size());

        // Now try to update the same entity - should update the CREATE entry instead
        response.addEntity(EntityOperation.UPDATE, entity4);
        assertEquals(2, response.getCreatedEntities().size());
        assertEquals(1, response.getUpdatedEntities().size());
    }

    @Test
    public void testEquals() {
        EntityMutationResponse response1 = new EntityMutationResponse();
        EntityMutationResponse response2 = new EntityMutationResponse();

        // Test equals with empty responses
        assertEquals(response1, response2);
        assertEquals(response1.hashCode(), response2.hashCode());

        // Test self equality
        assertEquals(response1, response1);

        // Test null equality
        assertNotEquals(response1, null);

        // Test different class equality
        assertNotEquals(response1, "string");

        // Test with different mutated entities
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities1 = new HashMap<>();
        List<AtlasEntityHeader> createdEntities1 = new ArrayList<>();
        createdEntities1.add(new AtlasEntityHeader("type1", "guid1", null));
        mutatedEntities1.put(EntityOperation.CREATE, createdEntities1);
        response1.setMutatedEntities(mutatedEntities1);

        assertNotEquals(response1, response2);

        // Make response2 equal to response1
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities2 = new HashMap<>();
        List<AtlasEntityHeader> createdEntities2 = new ArrayList<>();
        createdEntities2.add(new AtlasEntityHeader("type1", "guid1", null));
        mutatedEntities2.put(EntityOperation.CREATE, createdEntities2);
        response2.setMutatedEntities(mutatedEntities2);

        assertEquals(response1, response2);
        assertEquals(response1.hashCode(), response2.hashCode());

        // Test with different guid assignments
        Map<String, String> guidAssignments1 = new HashMap<>();
        guidAssignments1.put("temp1", "real1");
        response1.setGuidAssignments(guidAssignments1);

        assertNotEquals(response1, response2);

        Map<String, String> guidAssignments2 = new HashMap<>();
        guidAssignments2.put("temp1", "real1");
        response2.setGuidAssignments(guidAssignments2);

        assertEquals(response1, response2);
        assertEquals(response1.hashCode(), response2.hashCode());
    }

    @Test
    public void testToString() {
        EntityMutationResponse response = new EntityMutationResponse();

        // Test toString with empty response
        String toStringEmpty = response.toString();
        assertNotNull(toStringEmpty);

        // Test toString with populated response
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        List<AtlasEntityHeader> createdEntities = new ArrayList<>();
        createdEntities.add(new AtlasEntityHeader("Table", "table-guid", null));
        mutatedEntities.put(EntityOperation.CREATE, createdEntities);

        List<AtlasEntityHeader> deletedEntities = new ArrayList<>();
        deletedEntities.add(new AtlasEntityHeader("Column", "column-guid", null));
        mutatedEntities.put(EntityOperation.DELETE, deletedEntities);

        response.setMutatedEntities(mutatedEntities);

        String toStringWithData = response.toString();
        assertNotNull(toStringWithData);

        // Test toString with StringBuilder parameter
        StringBuilder sb = response.toString(null);
        assertNotNull(sb);

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        response.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
    }

    @Test
    public void testComplexScenario() {
        // Test a complex real-world scenario
        EntityMutationResponse response = new EntityMutationResponse();

        // Create various entities
        AtlasEntityHeader databaseEntity = new AtlasEntityHeader("Database", "db-guid", null);
        AtlasEntityHeader tableEntity = new AtlasEntityHeader("Table", "table-guid", null);
        AtlasEntityHeader columnEntity1 = new AtlasEntityHeader("Column", "col1-guid", null);
        AtlasEntityHeader columnEntity2 = new AtlasEntityHeader("Column", "col2-guid", null);

        // Set up attributes
        Map<String, Object> dbAttrs = new HashMap<>();
        dbAttrs.put("name", "production_db");
        databaseEntity.setAttributes(dbAttrs);

        Map<String, Object> tableAttrs = new HashMap<>();
        tableAttrs.put("name", "customer_table");
        tableEntity.setAttributes(tableAttrs);

        // Add entities to response
        response.addEntity(EntityOperation.CREATE, databaseEntity);
        response.addEntity(EntityOperation.CREATE, tableEntity);
        response.addEntity(EntityOperation.CREATE, columnEntity1);
        response.addEntity(EntityOperation.UPDATE, columnEntity2);

        // Test the complex response
        assertEquals(3, response.getCreatedEntities().size());
        assertEquals(1, response.getUpdatedEntities().size());
        assertNull(response.getDeletedEntities());

        // Test type-specific queries
        assertEquals(databaseEntity, response.getFirstCreatedEntityByTypeName("Database"));
        assertEquals(tableEntity, response.getFirstCreatedEntityByTypeName("Table"));

        List<AtlasEntityHeader> columns = response.getCreatedEntitiesByTypeName("Column");
        assertEquals(1, columns.size());
        assertEquals(columnEntity1, columns.get(0));

        // Test attribute-based queries
        assertEquals(databaseEntity, response.getCreatedEntityByTypeNameAndAttribute("Database", "name", "production_db"));
        assertEquals(tableEntity, response.getCreatedEntityByTypeNameAndAttribute("Table", "name", "customer_table"));

        // Add guid assignments
        Map<String, String> guidAssignments = new HashMap<>();
        guidAssignments.put("temp-db-guid", "db-guid");
        guidAssignments.put("temp-table-guid", "table-guid");
        response.setGuidAssignments(guidAssignments);

        assertEquals(guidAssignments, response.getGuidAssignments());
    }
}
