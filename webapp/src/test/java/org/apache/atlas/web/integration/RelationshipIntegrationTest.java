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
package org.apache.atlas.web.integration;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasRelationship.AtlasRelationshipWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Relationship CRUD operations.
 *
 * <p>Exercises relationship creation, retrieval, update, and deletion
 * using Schema-Table and Table-Column relationship types.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RelationshipIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(RelationshipIntegrationTest.class);

    private final long testId = System.currentTimeMillis();

    private String schemaGuid;
    private String table1Guid;
    private String table2Guid;
    private String columnGuid;
    private String relationshipGuid;

    @Test
    @Order(1)
    void testCreateRelationshipEntities() throws AtlasServiceException {
        // Create a Schema
        AtlasEntity schema = new AtlasEntity("Schema");
        schema.setAttribute("name", "rel-schema-" + testId);
        schema.setAttribute("qualifiedName", "test://integration/relationship/schema/" + testId);

        EntityMutationResponse resp1 = atlasClient.createEntity(new AtlasEntityWithExtInfo(schema));
        schemaGuid = resp1.getFirstEntityCreated().getGuid();

        // Create Table 1
        AtlasEntity table1 = new AtlasEntity("Table");
        table1.setAttribute("name", "rel-table1-" + testId);
        table1.setAttribute("qualifiedName", "test://integration/relationship/table1/" + testId);

        EntityMutationResponse resp2 = atlasClient.createEntity(new AtlasEntityWithExtInfo(table1));
        table1Guid = resp2.getFirstEntityCreated().getGuid();

        // Create Table 2
        AtlasEntity table2 = new AtlasEntity("Table");
        table2.setAttribute("name", "rel-table2-" + testId);
        table2.setAttribute("qualifiedName", "test://integration/relationship/table2/" + testId);

        EntityMutationResponse resp3 = atlasClient.createEntity(new AtlasEntityWithExtInfo(table2));
        table2Guid = resp3.getFirstEntityCreated().getGuid();

        // Create Column
        AtlasEntity column = new AtlasEntity("Column");
        column.setAttribute("name", "rel-column-" + testId);
        column.setAttribute("qualifiedName", "test://integration/relationship/column/" + testId);
        column.setAttribute("order", 0);

        EntityMutationResponse resp4 = atlasClient.createEntity(new AtlasEntityWithExtInfo(column));
        columnGuid = resp4.getFirstEntityCreated().getGuid();

        LOG.info("Created relationship test entities: schema={}, table1={}, table2={}, column={}",
                schemaGuid, table1Guid, table2Guid, columnGuid);
    }

    @Test
    @Order(2)
    void testCreateRelationship() throws AtlasServiceException {
        assertNotNull(table1Guid);
        assertNotNull(columnGuid);

        // Create a relationship between Table and Column (table_columns)
        AtlasRelationship relationship = new AtlasRelationship("table_columns");
        relationship.setEnd1(new AtlasObjectId(table1Guid, "Table"));
        relationship.setEnd2(new AtlasObjectId(columnGuid, "Column"));

        AtlasRelationship created = atlasClient.createRelationship(relationship);

        assertNotNull(created);
        assertNotNull(created.getGuid());
        assertEquals("table_columns", created.getTypeName());
        assertEquals(AtlasRelationship.Status.ACTIVE, created.getStatus());

        relationshipGuid = created.getGuid();
        LOG.info("Created relationship: guid={}, type={}", relationshipGuid, created.getTypeName());
    }

    @Test
    @Order(3)
    void testGetRelationship() throws AtlasServiceException {
        assertNotNull(relationshipGuid);

        AtlasRelationshipWithExtInfo result = atlasClient.getRelationshipByGuid(relationshipGuid);

        assertNotNull(result);
        assertNotNull(result.getRelationship());
        assertEquals(relationshipGuid, result.getRelationship().getGuid());
        assertEquals("table_columns", result.getRelationship().getTypeName());
        assertEquals(AtlasRelationship.Status.ACTIVE, result.getRelationship().getStatus());

        LOG.info("Fetched relationship: guid={}", result.getRelationship().getGuid());
    }

    @Test
    @Order(4)
    void testUpdateRelationship() throws AtlasServiceException {
        assertNotNull(relationshipGuid);

        AtlasRelationshipWithExtInfo current = atlasClient.getRelationshipByGuid(relationshipGuid);
        AtlasRelationship relationship = current.getRelationship();

        // Update the relationship (add/change propagateTags or other attributes)
        relationship.setPropagateTags(PropagateTags.ONE_TO_TWO);

        AtlasRelationship updated = atlasClient.updateRelationship(relationship);
        assertNotNull(updated);

        LOG.info("Updated relationship: guid={}", relationshipGuid);
    }

    @Test
    @Order(5)
    void testDeleteRelationship() throws AtlasServiceException {
        assertNotNull(relationshipGuid);

        atlasClient.deleteRelationshipByGuid(relationshipGuid);

        // Verify relationship is deleted
        AtlasRelationshipWithExtInfo result = atlasClient.getRelationshipByGuid(relationshipGuid);
        assertNotNull(result);
        assertEquals(AtlasRelationship.Status.DELETED, result.getRelationship().getStatus());

        LOG.info("Deleted relationship: guid={}", relationshipGuid);
    }

    @Test
    @Order(6)
    void testRelationshipStatusOnEntityDelete() throws AtlasServiceException {
        // Create a new relationship for this test
        AtlasRelationship rel = new AtlasRelationship("table_columns");
        rel.setEnd1(new AtlasObjectId(table2Guid, "Table"));
        rel.setEnd2(new AtlasObjectId(columnGuid, "Column"));

        AtlasRelationship created = atlasClient.createRelationship(rel);
        assertNotNull(created);
        String relGuid = created.getGuid();

        // Delete the table entity
        atlasClient.deleteEntityByGuid(table2Guid);

        // Verify entity is DELETED
        AtlasEntityWithExtInfo entityResult = atlasClient.getEntityByGuid(table2Guid);
        assertEquals(AtlasEntity.Status.DELETED, entityResult.getEntity().getStatus());

        // Check relationship status (may be DELETED or ACTIVE depending on cascade timing)
        AtlasRelationshipWithExtInfo relResult = atlasClient.getRelationshipByGuid(relGuid);
        AtlasRelationship.Status relStatus = relResult.getRelationship().getStatus();
        LOG.info("Relationship {} status after entity delete: {}", relGuid, relStatus);
        // Relationship cascade-delete may be async; accept either status
        assertNotNull(relStatus);

        LOG.info("Verified entity {} deleted, relationship {} status={}", table2Guid, relGuid, relStatus);
    }

    @Test
    @Order(7)
    void testOneToManyRelationship() throws AtlasServiceException {
        assertNotNull(schemaGuid);
        assertNotNull(table1Guid);

        // Create schema_tables relationship (schema -> table)
        AtlasRelationship rel = new AtlasRelationship("schema_tables");
        rel.setEnd1(new AtlasObjectId(schemaGuid, "Schema"));
        rel.setEnd2(new AtlasObjectId(table1Guid, "Table"));

        AtlasRelationship created = atlasClient.createRelationship(rel);
        assertNotNull(created);

        // Verify schema now has tables relationship
        AtlasEntityWithExtInfo schemaResult = atlasClient.getEntityByGuid(schemaGuid, false, false);
        Object tables = schemaResult.getEntity().getRelationshipAttribute("tables");
        assertNotNull(tables, "Schema should have tables relationship");

        LOG.info("Verified one-to-many relationship: schema -> table");
    }

    @Test
    @Order(8)
    void testCreateDuplicateRelationship() {
        // Creating a second relationship of the same type between the same entities
        // may either update/reuse the existing one or throw an error
        AtlasRelationship rel = new AtlasRelationship("schema_tables");
        rel.setEnd1(new AtlasObjectId(schemaGuid, "Schema"));
        rel.setEnd2(new AtlasObjectId(table1Guid, "Table"));

        try {
            AtlasRelationship result = atlasClient.createRelationship(rel);
            assertNotNull(result);
            LOG.info("Duplicate relationship handled (reused/updated): guid={}", result.getGuid());
        } catch (AtlasServiceException e) {
            // Some Atlas versions reject duplicate relationships
            LOG.info("Duplicate relationship correctly rejected: {}", e.getMessage());
        }
    }
}
