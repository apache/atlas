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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.TestUtilsV2.randomString;
import static org.apache.atlas.type.AtlasTypeUtil.createClassTypeDef;
import static org.apache.atlas.type.AtlasTypeUtil.createOptionalAttrDef;
import static org.apache.atlas.type.AtlasTypeUtil.createUniqueRequiredAttrDef;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test class to verify handling of multiple relationship types for the same attribute.
 * This covers the scenario where a hive_db can have both hive_table and delta_table
 * entities in its "tables" attribute, using different relationship types.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class EntityGraphMapperMultipleRelationshipTypesTest extends AtlasEntityTestBase {
    private static final String HIVE_DB_TYPE                = "hive_db";
    private static final String HIVE_TABLE_TYPE             = "hive_table";
    private static final String DELTA_TABLE_TYPE            = "delta_table";
    private static final String INVALID_TABLE_TYPE          = "invalid_table";
    private static final String HIVE_TABLE_DB_RELATIONSHIP  = "hive_table_db";
    private static final String DELTA_TABLE_DB_RELATIONSHIP = "delta_table_db";
    private static final String TABLES_ATTR_NAME            = "tables";

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        // Create type definitions with multiple relationship types for the same attribute
        AtlasTypesDef typesDef = createTypesWithMultipleRelationshipTypes();
        createTypesDef(new AtlasTypesDef[] {typesDef});
    }

    @BeforeTest
    public void init() throws Exception {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
    }

    /**
     * Test CREATE operation with mixed relationship types in a collection.
     * Creates a hive_db with both hive_table and delta_table entities.
     */
    @Test
    public void testCreateWithMixedRelationshipTypes() throws Exception {
        init();

        // Create a new hive_db with mixed table types
        AtlasEntity dbEntity = new AtlasEntity(HIVE_DB_TYPE);
        dbEntity.setAttribute("name", "test_db_" + randomString());
        dbEntity.setAttribute("description", "Test database with mixed tables");

        // Create hive_table entities
        AtlasEntity hiveTable1 = createHiveTableEntity("ht1_" + randomString());
        AtlasEntity hiveTable2 = createHiveTableEntity("ht2_" + randomString());

        // Create delta_table entities
        AtlasEntity deltaTable1 = createDeltaTableEntity("dt1_" + randomString());
        AtlasEntity deltaTable2 = createDeltaTableEntity("dt2_" + randomString());

        // Create all entities first
        AtlasEntitiesWithExtInfo entitiesToCreate = new AtlasEntitiesWithExtInfo();
        entitiesToCreate.addEntity(hiveTable1);
        entitiesToCreate.addEntity(hiveTable2);
        entitiesToCreate.addEntity(deltaTable1);
        entitiesToCreate.addEntity(deltaTable2);
        entitiesToCreate.addEntity(dbEntity);

        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesToCreate), false);

        assertNotNull(response);
        List<AtlasEntityHeader> createdEntities = response.getCreatedEntities();
        assertNotNull(createdEntities);
        assertEquals(createdEntities.size(), 5);

        // Find the created entities
        String createdDbGuid          = null;
        String createdHiveTable1Guid  = null;
        String createdHiveTable2Guid  = null;
        String createdDeltaTable1Guid = null;
        String createdDeltaTable2Guid = null;

        for (AtlasEntityHeader header : createdEntities) {
            if (HIVE_DB_TYPE.equals(header.getTypeName())) {
                createdDbGuid = header.getGuid();
            } else if (HIVE_TABLE_TYPE.equals(header.getTypeName())) {
                if (createdHiveTable1Guid == null) {
                    createdHiveTable1Guid = header.getGuid();
                } else {
                    createdHiveTable2Guid = header.getGuid();
                }
            } else if (DELTA_TABLE_TYPE.equals(header.getTypeName())) {
                if (createdDeltaTable1Guid == null) {
                    createdDeltaTable1Guid = header.getGuid();
                } else {
                    createdDeltaTable2Guid = header.getGuid();
                }
            }
        }

        assertNotNull(createdDbGuid);
        assertNotNull(createdHiveTable1Guid);
        assertNotNull(createdHiveTable2Guid);
        assertNotNull(createdDeltaTable1Guid);
        assertNotNull(createdDeltaTable2Guid);

        // Now update the db entity to include the tables using relationship attributes
        AtlasEntity dbEntityUpdate = new AtlasEntity(HIVE_DB_TYPE);
        dbEntityUpdate.setGuid(createdDbGuid);
        dbEntityUpdate.setAttribute("name", "db1");

        List<AtlasRelatedObjectId> tableRefs = new ArrayList<>();
        tableRefs.add(new AtlasRelatedObjectId(new AtlasObjectId(createdHiveTable1Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        tableRefs.add(new AtlasRelatedObjectId(new AtlasObjectId(createdHiveTable2Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        tableRefs.add(new AtlasRelatedObjectId(new AtlasObjectId(createdDeltaTable1Guid, DELTA_TABLE_TYPE), DELTA_TABLE_DB_RELATIONSHIP));
        tableRefs.add(new AtlasRelatedObjectId(new AtlasObjectId(createdDeltaTable2Guid, DELTA_TABLE_TYPE), DELTA_TABLE_DB_RELATIONSHIP));

        dbEntityUpdate.setRelationshipAttribute(TABLES_ATTR_NAME, tableRefs);

        AtlasEntityWithExtInfo dbEntityExtInfo = new AtlasEntityWithExtInfo(dbEntityUpdate);
        EntityMutationResponse updateResponse  = entityStore.createOrUpdate(new AtlasEntityStream(dbEntityExtInfo), false);

        assertNotNull(updateResponse);

        // Verify relationships were created correctly
        verifyRelationships(createdDbGuid, createdHiveTable1Guid, createdHiveTable2Guid,
                createdDeltaTable1Guid, createdDeltaTable2Guid);
    }

    /**
     * Test UPDATE operation with mixed relationship types.
     * Simulates incremental import scenario where a new delta_table is added.
     */
    @Test(dependsOnMethods = "testCreateWithMixedRelationshipTypes")
    public void testUpdateWithMixedRelationshipTypesIncremental() throws Exception {
        init();

        // Create initial entities (simulating existing state)
        AtlasEntity dbEntity    = createHiveDbEntity("mixdb_" + randomString());
        AtlasEntity hiveTable1  = createHiveTableEntity("ht1_" + randomString());
        AtlasEntity hiveTable2  = createHiveTableEntity("ht2_" + randomString());
        AtlasEntity deltaTable1 = createDeltaTableEntity("dt1_" + randomString());
        AtlasEntity deltaTable2 = createDeltaTableEntity("dt2_" + randomString());

        // Create all entities
        AtlasEntitiesWithExtInfo entitiesToCreate = new AtlasEntitiesWithExtInfo();
        entitiesToCreate.addEntity(hiveTable1);
        entitiesToCreate.addEntity(hiveTable2);
        entitiesToCreate.addEntity(deltaTable1);
        entitiesToCreate.addEntity(deltaTable2);
        entitiesToCreate.addEntity(dbEntity);

        EntityMutationResponse createResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesToCreate), false);

        assertNotNull(createResponse);
        List<AtlasEntityHeader> createdEntities = createResponse.getCreatedEntities();
        assertEquals(createdEntities.size(), 5);

        // Extract GUIDs
        String dbGuid  = findEntityGuid(createdEntities, HIVE_DB_TYPE);
        String ht1Guid = findEntityGuid(createdEntities, HIVE_TABLE_TYPE, 0);
        String ht2Guid = findEntityGuid(createdEntities, HIVE_TABLE_TYPE, 1);
        String dt1Guid = findEntityGuid(createdEntities, DELTA_TABLE_TYPE, 0);
        String dt2Guid = findEntityGuid(createdEntities, DELTA_TABLE_TYPE, 1);

        // Set up initial relationships
        AtlasEntity dbEntityWithTables = new AtlasEntity(HIVE_DB_TYPE);
        dbEntityWithTables.setGuid(dbGuid);
        dbEntityWithTables.setAttribute("name", "db2");

        List<AtlasRelatedObjectId> initialTables = new ArrayList<>();
        initialTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ht1Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        initialTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ht2Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        initialTables.add(new AtlasRelatedObjectId(new AtlasObjectId(dt1Guid, DELTA_TABLE_TYPE), DELTA_TABLE_DB_RELATIONSHIP));
        initialTables.add(new AtlasRelatedObjectId(new AtlasObjectId(dt2Guid, DELTA_TABLE_TYPE), DELTA_TABLE_DB_RELATIONSHIP));

        dbEntityWithTables.setRelationshipAttribute(TABLES_ATTR_NAME, initialTables);
        AtlasEntityWithExtInfo dbEntityExtInfo = new AtlasEntityWithExtInfo(dbEntityWithTables);
        entityStore.createOrUpdate(new AtlasEntityStream(dbEntityExtInfo), false);

        // Now simulate incremental import: add a new delta_table
        AtlasEntity              newDeltaTable = createDeltaTableEntity("dt3_" + randomString());
        AtlasEntitiesWithExtInfo newEntity     = new AtlasEntitiesWithExtInfo();
        newEntity.addEntity(newDeltaTable);

        EntityMutationResponse newTableResponse = entityStore.createOrUpdate(new AtlasEntityStream(newEntity), false);
        String                 dt3Guid          = findEntityGuid(newTableResponse.getCreatedEntities(), DELTA_TABLE_TYPE);

        // Update db entity to include the new delta_table (incremental update)
        AtlasEntity dbEntityUpdate = new AtlasEntity(HIVE_DB_TYPE);
        dbEntityUpdate.setGuid(dbGuid);
        dbEntityUpdate.setAttribute("name", dbEntityWithTables.getAttribute("name"));

        // Include all existing tables plus the new one
        List<AtlasRelatedObjectId> allTables = new ArrayList<>();
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ht1Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ht2Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(dt1Guid, DELTA_TABLE_TYPE), DELTA_TABLE_DB_RELATIONSHIP));
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(dt2Guid, DELTA_TABLE_TYPE), DELTA_TABLE_DB_RELATIONSHIP));
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(dt3Guid, DELTA_TABLE_TYPE), DELTA_TABLE_DB_RELATIONSHIP));

        dbEntityUpdate.setRelationshipAttribute(TABLES_ATTR_NAME, allTables);

        AtlasEntityWithExtInfo updateExtInfo  = new AtlasEntityWithExtInfo(dbEntityUpdate);
        EntityMutationResponse updateResponse = entityStore.createOrUpdate(new AtlasEntityStream(updateExtInfo), false);

        assertNotNull(updateResponse);

        // Verify all relationships are correct
        verifyRelationships(dbGuid, ht1Guid, ht2Guid, dt1Guid, dt2Guid, dt3Guid);
    }

    /**
     * Test that validates the correct relationship types are used for each entity type.
     * This ensures hive_table uses hive_table_db and delta_table uses delta_table_db.
     */
    @Test
    public void testRelationshipTypeValidation() throws Exception {
        init();

        // Create entities
        AtlasEntity dbEntity   = createHiveDbEntity("validate_db_" + randomString());
        AtlasEntity hiveTable  = createHiveTableEntity("validate_ht_" + randomString());
        AtlasEntity deltaTable = createDeltaTableEntity("validate_dt_" + randomString());

        AtlasEntitiesWithExtInfo entitiesToCreate = new AtlasEntitiesWithExtInfo();
        entitiesToCreate.addEntity(dbEntity);
        entitiesToCreate.addEntity(hiveTable);
        entitiesToCreate.addEntity(deltaTable);

        EntityMutationResponse createResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesToCreate), false);

        String dbGuid = findEntityGuid(createResponse.getCreatedEntities(), HIVE_DB_TYPE);
        String htGuid = findEntityGuid(createResponse.getCreatedEntities(), HIVE_TABLE_TYPE);
        String dtGuid = findEntityGuid(createResponse.getCreatedEntities(), DELTA_TABLE_TYPE);

        // Update db with mixed tables
        AtlasEntity dbEntityUpdate = new AtlasEntity(HIVE_DB_TYPE);
        dbEntityUpdate.setGuid(dbGuid);
        dbEntityUpdate.setAttribute("name", dbEntity.getAttributes().get("name"));

        List<AtlasRelatedObjectId> tables = new ArrayList<>();
        tables.add(new AtlasRelatedObjectId(new AtlasObjectId(htGuid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        tables.add(new AtlasRelatedObjectId(new AtlasObjectId(dtGuid, DELTA_TABLE_TYPE), DELTA_TABLE_DB_RELATIONSHIP));

        dbEntityUpdate.setRelationshipAttribute(TABLES_ATTR_NAME, tables);

        AtlasEntityWithExtInfo dbEntityExtInfo = new AtlasEntityWithExtInfo(dbEntityUpdate);

        // This should not throw an exception about invalid relationship types
        try {
            EntityMutationResponse updateResponse = entityStore.createOrUpdate(new AtlasEntityStream(dbEntityExtInfo), false);
            assertNotNull(updateResponse);

            // Verify relationships exist with correct types
            AtlasEntityType dbType            = typeRegistry.getEntityTypeByName(HIVE_DB_TYPE);
            Set<String>     relationshipTypes = dbType.getAttributeRelationshipTypes(TABLES_ATTR_NAME);
            assertTrue(relationshipTypes.contains(HIVE_TABLE_DB_RELATIONSHIP));
            assertTrue(relationshipTypes.contains(DELTA_TABLE_DB_RELATIONSHIP));
        } catch (AtlasBaseException e) {
            // Should not get the error: "invalid relationshipDef: delta_table_db: end type 1: hive_db, end type 2: hive_table"
            if (e.getMessage().contains("invalid relationshipDef") && e.getMessage().contains("delta_table_db")) {
                fail("Should not use delta_table_db relationship for hive_table entities: " + e.getMessage());
            }
            throw e;
        }
    }

    /**
     * Test that a single invalid element type for the relationship attribute is ignored
     * and does not create a relationship, instead of being silently routed to an incorrect relationship type.
     */
    @Test
    public void testSingleInvalidElementTypeIgnored() throws Exception {
        init();

        // Create hive_db and an invalid_table entity (not covered by any relationshipDef on TABLES_ATTR_NAME)
        AtlasEntity dbEntity     = createHiveDbEntity("invalid_single_db_" + randomString());
        AtlasEntity invalidTable = createInvalidTableEntity("invalid_single_tbl_" + randomString());

        AtlasEntitiesWithExtInfo entitiesToCreate = new AtlasEntitiesWithExtInfo();
        entitiesToCreate.addEntity(dbEntity);
        entitiesToCreate.addEntity(invalidTable);

        EntityMutationResponse createResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesToCreate), false);

        String dbGuid         = findEntityGuid(createResponse.getCreatedEntities(), HIVE_DB_TYPE);
        String invalidTblGuid = findEntityGuid(createResponse.getCreatedEntities(), INVALID_TABLE_TYPE);

        // Attempt to set relationship attribute with a single invalid_table element
        AtlasEntity dbUpdate = new AtlasEntity(HIVE_DB_TYPE);
        dbUpdate.setGuid(dbGuid);
        dbUpdate.setAttribute("name", dbEntity.getAttribute("name"));

        List<AtlasRelatedObjectId> tables = new ArrayList<>();
        tables.add(new AtlasRelatedObjectId(new AtlasObjectId(invalidTblGuid, INVALID_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));

        dbUpdate.setRelationshipAttribute(TABLES_ATTR_NAME, tables);

        AtlasEntityWithExtInfo dbUpdateExtInfo = new AtlasEntityWithExtInfo(dbUpdate);

        // Should not throw; invalid element should simply be ignored
        EntityMutationResponse updateResponse = entityStore.createOrUpdate(new AtlasEntityStream(dbUpdateExtInfo), false);
        assertNotNull(updateResponse);

        // Verify that no relationship was created between db and invalid table
        AtlasVertex dbVertex      = AtlasGraphUtilsV2.findByGuid(graph, dbGuid);
        AtlasVertex invalidVertex = AtlasGraphUtilsV2.findByGuid(graph, invalidTblGuid);
        assertNotNull(dbVertex, "Database vertex should exist");
        assertNotNull(invalidVertex, "Invalid table vertex should exist");

        boolean found = false;

        Iterable<AtlasEdge> outEdges = dbVertex.getEdges(AtlasEdgeDirection.OUT);
        for (AtlasEdge edge : outEdges) {
            String edgeGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getInVertex());
            if (invalidTblGuid.equals(edgeGuid)) {
                found = true;
                break;
            }
        }

        if (!found) {
            Iterable<AtlasEdge> inEdges = dbVertex.getEdges(AtlasEdgeDirection.IN);
            for (AtlasEdge edge : inEdges) {
                String edgeGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getOutVertex());
                if (invalidTblGuid.equals(edgeGuid)) {
                    found = true;
                    break;
                }
            }
        }

        assertFalse(found, "No relationship should exist between db and invalid table: " + invalidTblGuid);
    }

    /**
     * Test that when all elements in a collection are of invalid type, the mapping ignores them
     * and does not create any relationships.
     */
    @Test
    public void testAllInvalidElementTypesInCollectionIgnored() throws Exception {
        init();

        // Create hive_db and two invalid_table entities
        AtlasEntity dbEntity      = createHiveDbEntity("invalid_coll_db_" + randomString());
        AtlasEntity invalidTable1 = createInvalidTableEntity("invalid_coll_tbl1_" + randomString());
        AtlasEntity invalidTable2 = createInvalidTableEntity("invalid_coll_tbl2_" + randomString());

        AtlasEntitiesWithExtInfo entitiesToCreate = new AtlasEntitiesWithExtInfo();
        entitiesToCreate.addEntity(dbEntity);
        entitiesToCreate.addEntity(invalidTable1);
        entitiesToCreate.addEntity(invalidTable2);

        EntityMutationResponse createResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesToCreate), false);

        String dbGuid          = findEntityGuid(createResponse.getCreatedEntities(), HIVE_DB_TYPE);
        String invalidTbl1Guid = findEntityGuid(createResponse.getCreatedEntities(), INVALID_TABLE_TYPE, 0);
        String invalidTbl2Guid = findEntityGuid(createResponse.getCreatedEntities(), INVALID_TABLE_TYPE, 1);

        // Attempt to set relationship attribute with only invalid_table elements
        AtlasEntity dbUpdate = new AtlasEntity(HIVE_DB_TYPE);
        dbUpdate.setGuid(dbGuid);
        dbUpdate.setAttribute("name", dbEntity.getAttribute("name"));

        List<AtlasRelatedObjectId> tables = new ArrayList<>();
        tables.add(new AtlasRelatedObjectId(new AtlasObjectId(invalidTbl1Guid, INVALID_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        tables.add(new AtlasRelatedObjectId(new AtlasObjectId(invalidTbl2Guid, INVALID_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));

        dbUpdate.setRelationshipAttribute(TABLES_ATTR_NAME, tables);

        AtlasEntityWithExtInfo dbUpdateExtInfo = new AtlasEntityWithExtInfo(dbUpdate);

        // Should not throw; invalid elements should simply be ignored
        EntityMutationResponse updateResponse = entityStore.createOrUpdate(new AtlasEntityStream(dbUpdateExtInfo), false);
        assertNotNull(updateResponse);

        // Verify that no relationships were created between db and invalid tables
        AtlasVertex dbVertex       = AtlasGraphUtilsV2.findByGuid(graph, dbGuid);
        AtlasVertex invalidVertex1 = AtlasGraphUtilsV2.findByGuid(graph, invalidTbl1Guid);
        AtlasVertex invalidVertex2 = AtlasGraphUtilsV2.findByGuid(graph, invalidTbl2Guid);
        assertNotNull(dbVertex, "Database vertex should exist");
        assertNotNull(invalidVertex1, "Invalid table vertex 1 should exist");
        assertNotNull(invalidVertex2, "Invalid table vertex 2 should exist");

        // helper lambda-style pattern, but keeping it explicit for clarity
        for (String invalidGuid : new String[] {invalidTbl1Guid, invalidTbl2Guid}) {
            boolean found = false;

            Iterable<AtlasEdge> outEdges = dbVertex.getEdges(AtlasEdgeDirection.OUT);
            for (AtlasEdge edge : outEdges) {
                String edgeGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getInVertex());
                if (invalidGuid.equals(edgeGuid)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                Iterable<AtlasEdge> inEdges = dbVertex.getEdges(AtlasEdgeDirection.IN);
                for (AtlasEdge edge : inEdges) {
                    String edgeGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getOutVertex());
                    if (invalidGuid.equals(edgeGuid)) {
                        found = true;
                        break;
                    }
                }
            }

            assertFalse(found, "No relationship should exist between db and invalid table: " + invalidGuid);
        }
    }

    // Helper methods

    private AtlasTypesDef createTypesWithMultipleRelationshipTypes() {
        AtlasTypesDef typesDef = new AtlasTypesDef();

        // Create hive_db entity type
        AtlasEntityDef hiveDbDef = createClassTypeDef(HIVE_DB_TYPE, "Hive Database",
                Collections.emptySet(),
                createUniqueRequiredAttrDef("name", "string"),
                createOptionalAttrDef("description", "string"));

        // Create hive_table entity type
        AtlasEntityDef hiveTableDef = createClassTypeDef(HIVE_TABLE_TYPE, "Hive Table",
                Collections.emptySet(),
                createUniqueRequiredAttrDef("name", "string"),
                createOptionalAttrDef("description", "string"));

        // Create delta_table entity type
        AtlasEntityDef deltaTableDef = createClassTypeDef(DELTA_TABLE_TYPE, "Delta Table",
                Collections.emptySet(),
                createUniqueRequiredAttrDef("name", "string"),
                createOptionalAttrDef("description", "string"));

        // Create invalid_table entity type (not part of any relationshipDef for tables)
        AtlasEntityDef invalidTableDef = createClassTypeDef(INVALID_TABLE_TYPE, "Invalid Table",
                Collections.emptySet(),
                createUniqueRequiredAttrDef("name", "string"),
                createOptionalAttrDef("description", "string"));

        typesDef.getEntityDefs().add(hiveDbDef);
        typesDef.getEntityDefs().add(hiveTableDef);
        typesDef.getEntityDefs().add(deltaTableDef);
        typesDef.getEntityDefs().add(invalidTableDef);

        // Create hive_table_db relationship
        AtlasRelationshipDef hiveTableDbRel = new AtlasRelationshipDef();
        hiveTableDbRel.setName(HIVE_TABLE_DB_RELATIONSHIP);
        hiveTableDbRel.setServiceType("hive");
        hiveTableDbRel.setTypeVersion("1.0");
        hiveTableDbRel.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.AGGREGATION);
        hiveTableDbRel.setRelationshipLabel("__hive_table.db");
        hiveTableDbRel.setPropagateTags(AtlasRelationshipDef.PropagateTags.NONE);

        AtlasRelationshipEndDef hiveTableEnd = new AtlasRelationshipEndDef();
        hiveTableEnd.setType(HIVE_TABLE_TYPE);
        hiveTableEnd.setName("db");
        hiveTableEnd.setIsContainer(false);
        hiveTableEnd.setCardinality(Cardinality.SINGLE);
        hiveTableEnd.setIsLegacyAttribute(true);

        AtlasRelationshipEndDef hiveDbEnd = new AtlasRelationshipEndDef();
        hiveDbEnd.setType(HIVE_DB_TYPE);
        hiveDbEnd.setName(TABLES_ATTR_NAME);
        hiveDbEnd.setIsContainer(true);
        hiveDbEnd.setCardinality(Cardinality.SET);
        hiveDbEnd.setIsLegacyAttribute(false);

        hiveTableDbRel.setEndDef1(hiveTableEnd);
        hiveTableDbRel.setEndDef2(hiveDbEnd);

        // Create delta_table_db relationship
        AtlasRelationshipDef deltaTableDbRel = new AtlasRelationshipDef();
        deltaTableDbRel.setName(DELTA_TABLE_DB_RELATIONSHIP);
        deltaTableDbRel.setServiceType("hive");
        deltaTableDbRel.setTypeVersion("1.0");
        deltaTableDbRel.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.AGGREGATION);
        deltaTableDbRel.setRelationshipLabel("__delta_table.db");
        deltaTableDbRel.setPropagateTags(AtlasRelationshipDef.PropagateTags.NONE);

        AtlasRelationshipEndDef deltaTableEnd = new AtlasRelationshipEndDef();
        deltaTableEnd.setType(DELTA_TABLE_TYPE);
        deltaTableEnd.setName("db");
        deltaTableEnd.setIsContainer(false);
        deltaTableEnd.setCardinality(Cardinality.SINGLE);
        deltaTableEnd.setIsLegacyAttribute(true);

        AtlasRelationshipEndDef deltaDbEnd = new AtlasRelationshipEndDef();
        deltaDbEnd.setType(HIVE_DB_TYPE);
        deltaDbEnd.setName(TABLES_ATTR_NAME);
        deltaDbEnd.setIsContainer(true);
        deltaDbEnd.setCardinality(Cardinality.SET);
        deltaDbEnd.setIsLegacyAttribute(false);

        deltaTableDbRel.setEndDef1(deltaTableEnd);
        deltaTableDbRel.setEndDef2(deltaDbEnd);

        typesDef.getRelationshipDefs().add(hiveTableDbRel);
        typesDef.getRelationshipDefs().add(deltaTableDbRel);

        return typesDef;
    }

    private AtlasEntity createHiveDbEntity(String name) {
        return createNewEntity(HIVE_DB_TYPE, name, "Test database: " + name);
    }

    private AtlasEntity createHiveTableEntity(String name) {
        return createNewEntity(HIVE_TABLE_TYPE, name, "Test hive table: " + name);
    }

    private AtlasEntity createDeltaTableEntity(String name) {
        return createNewEntity(DELTA_TABLE_TYPE, name, "Test delta table: " + name);
    }

    private AtlasEntity createInvalidTableEntity(String name) {
        return createNewEntity(INVALID_TABLE_TYPE, name, "Test invalid table: " + name);
    }

    private AtlasEntity createNewEntity(String typeName, String name, String description) {
        AtlasEntity entity = new AtlasEntity(typeName);
        entity.setAttribute("name", name);
        entity.setAttribute("description", description);
        return entity;
    }

    private String findEntityGuid(List<AtlasEntityHeader> entities, String typeName) {
        return findEntityGuid(entities, typeName, 0);
    }

    private String findEntityGuid(List<AtlasEntityHeader> entities, String typeName, int index) {
        int count = 0;
        for (AtlasEntityHeader header : entities) {
            if (typeName.equals(header.getTypeName())) {
                if (count == index) {
                    return header.getGuid();
                }
                count++;
            }
        }
        return null;
    }

    private void verifyRelationships(String dbGuid, String... tableGuids) {
        AtlasVertex dbVertex = AtlasGraphUtilsV2.findByGuid(graph, dbGuid);
        assertNotNull(dbVertex, "Database vertex should exist");

        // Verify relationships exist
        for (String tableGuid : tableGuids) {
            AtlasVertex tableVertex = AtlasGraphUtilsV2.findByGuid(graph, tableGuid);
            assertNotNull(tableVertex, "Table vertex should exist for guid: " + tableGuid);

            // Check that there's an edge between db and table
            boolean             found    = false;
            Iterable<AtlasEdge> outEdges = dbVertex.getEdges(AtlasEdgeDirection.OUT);
            for (AtlasEdge edge : outEdges) {
                String edgeGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getInVertex());
                if (tableGuid.equals(edgeGuid)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                Iterable<AtlasEdge> inEdges = dbVertex.getEdges(AtlasEdgeDirection.IN);
                for (AtlasEdge edge : inEdges) {
                    String edgeGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getOutVertex());
                    if (tableGuid.equals(edgeGuid)) {
                        found = true;
                        break;
                    }
                }
            }
            assertTrue(found, "Relationship should exist between db and table: " + tableGuid);
        }
    }
}
