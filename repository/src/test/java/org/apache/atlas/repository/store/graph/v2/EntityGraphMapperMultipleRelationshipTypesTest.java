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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test class to verify handling of multiple relationship types for the same attribute.
 * This covers the scenario where a hive_db can have both hive_table and iceberg_table
 * entities in its "tables" attribute, using different relationship types.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class EntityGraphMapperMultipleRelationshipTypesTest extends AtlasEntityTestBase {
    private static final String HIVE_DB_TYPE                  = "hive_db";
    private static final String HIVE_TABLE_TYPE               = "hive_table";
    private static final String ICEBERG_TABLE_TYPE            = "iceberg_table";
    private static final String HIVE_TABLE_DB_RELATIONSHIP    = "hive_table_db";
    private static final String ICEBERG_TABLE_DB_RELATIONSHIP = "iceberg_table_db";
    private static final String TABLES_ATTR_NAME              = "tables";

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
     * Creates a hive_db with both hive_table and iceberg_table entities.
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

        // Create iceberg_table entities
        AtlasEntity icebergTable1 = createIcebergTableEntity("ice1_" + randomString());
        AtlasEntity icebergTable2 = createIcebergTableEntity("ice2_" + randomString());

        // Create all entities first
        AtlasEntitiesWithExtInfo entitiesToCreate = new AtlasEntitiesWithExtInfo();
        entitiesToCreate.addEntity(hiveTable1);
        entitiesToCreate.addEntity(hiveTable2);
        entitiesToCreate.addEntity(icebergTable1);
        entitiesToCreate.addEntity(icebergTable2);
        entitiesToCreate.addEntity(dbEntity);

        EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(entitiesToCreate), false);

        assertNotNull(response);
        List<AtlasEntityHeader> createdEntities = response.getCreatedEntities();
        assertNotNull(createdEntities);
        assertEquals(createdEntities.size(), 5);

        // Find the created entities
        String createdDbGuid            = null;
        String createdHiveTable1Guid    = null;
        String createdHiveTable2Guid    = null;
        String createdIcebergTable1Guid = null;
        String createdIcebergTable2Guid = null;

        for (AtlasEntityHeader header : createdEntities) {
            if (HIVE_DB_TYPE.equals(header.getTypeName())) {
                createdDbGuid = header.getGuid();
            } else if (HIVE_TABLE_TYPE.equals(header.getTypeName())) {
                if (createdHiveTable1Guid == null) {
                    createdHiveTable1Guid = header.getGuid();
                } else {
                    createdHiveTable2Guid = header.getGuid();
                }
            } else if (ICEBERG_TABLE_TYPE.equals(header.getTypeName())) {
                if (createdIcebergTable1Guid == null) {
                    createdIcebergTable1Guid = header.getGuid();
                } else {
                    createdIcebergTable2Guid = header.getGuid();
                }
            }
        }

        assertNotNull(createdDbGuid);
        assertNotNull(createdHiveTable1Guid);
        assertNotNull(createdHiveTable2Guid);
        assertNotNull(createdIcebergTable1Guid);
        assertNotNull(createdIcebergTable2Guid);

        // Now update the db entity to include the tables using relationship attributes
        AtlasEntity dbEntityUpdate = new AtlasEntity(HIVE_DB_TYPE);
        dbEntityUpdate.setGuid(createdDbGuid);
        dbEntityUpdate.setAttribute("name", "db1");

        List<AtlasRelatedObjectId> tableRefs = new ArrayList<>();
        tableRefs.add(new AtlasRelatedObjectId(new AtlasObjectId(createdHiveTable1Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        tableRefs.add(new AtlasRelatedObjectId(new AtlasObjectId(createdHiveTable2Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        tableRefs.add(new AtlasRelatedObjectId(new AtlasObjectId(createdIcebergTable1Guid, ICEBERG_TABLE_TYPE), ICEBERG_TABLE_DB_RELATIONSHIP));
        tableRefs.add(new AtlasRelatedObjectId(new AtlasObjectId(createdIcebergTable2Guid, ICEBERG_TABLE_TYPE), ICEBERG_TABLE_DB_RELATIONSHIP));

        dbEntityUpdate.setRelationshipAttribute(TABLES_ATTR_NAME, tableRefs);

        AtlasEntityWithExtInfo dbEntityExtInfo = new AtlasEntityWithExtInfo(dbEntityUpdate);
        EntityMutationResponse updateResponse  = entityStore.createOrUpdate(new AtlasEntityStream(dbEntityExtInfo), false);

        assertNotNull(updateResponse);

        // Verify relationships were created correctly
        verifyRelationships(createdDbGuid, createdHiveTable1Guid, createdHiveTable2Guid,
                createdIcebergTable1Guid, createdIcebergTable2Guid);
    }

    /**
     * Test UPDATE operation with mixed relationship types.
     * Simulates incremental import scenario where a new iceberg_table is added.
     */
    @Test(dependsOnMethods = "testCreateWithMixedRelationshipTypes")
    public void testUpdateWithMixedRelationshipTypesIncremental() throws Exception {
        init();

        // Create initial entities (simulating existing state)
        AtlasEntity dbEntity      = createHiveDbEntity("mixdb_" + randomString());
        AtlasEntity hiveTable1    = createHiveTableEntity("ht1_" + randomString());
        AtlasEntity hiveTable2    = createHiveTableEntity("ht2_" + randomString());
        AtlasEntity icebergTable1 = createIcebergTableEntity("ice1_" + randomString());
        AtlasEntity icebergTable2 = createIcebergTableEntity("ice2_" + randomString());

        // Create all entities
        AtlasEntitiesWithExtInfo entitiesToCreate = new AtlasEntitiesWithExtInfo();
        entitiesToCreate.addEntity(hiveTable1);
        entitiesToCreate.addEntity(hiveTable2);
        entitiesToCreate.addEntity(icebergTable1);
        entitiesToCreate.addEntity(icebergTable2);
        entitiesToCreate.addEntity(dbEntity);

        EntityMutationResponse createResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesToCreate), false);

        assertNotNull(createResponse);
        List<AtlasEntityHeader> createdEntities = createResponse.getCreatedEntities();
        assertEquals(createdEntities.size(), 5);

        // Extract GUIDs
        String dbGuid   = findEntityGuid(createdEntities, HIVE_DB_TYPE);
        String ht1Guid  = findEntityGuid(createdEntities, HIVE_TABLE_TYPE, 0);
        String ht2Guid  = findEntityGuid(createdEntities, HIVE_TABLE_TYPE, 1);
        String ice1Guid = findEntityGuid(createdEntities, ICEBERG_TABLE_TYPE, 0);
        String ice2Guid = findEntityGuid(createdEntities, ICEBERG_TABLE_TYPE, 1);

        // Set up initial relationships
        AtlasEntity dbEntityWithTables = new AtlasEntity(HIVE_DB_TYPE);
        dbEntityWithTables.setGuid(dbGuid);
        dbEntityWithTables.setAttribute("name", "db2");

        List<AtlasRelatedObjectId> initialTables = new ArrayList<>();
        initialTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ht1Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        initialTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ht2Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        initialTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ice1Guid, ICEBERG_TABLE_TYPE), ICEBERG_TABLE_DB_RELATIONSHIP));
        initialTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ice2Guid, ICEBERG_TABLE_TYPE), ICEBERG_TABLE_DB_RELATIONSHIP));

        dbEntityWithTables.setRelationshipAttribute(TABLES_ATTR_NAME, initialTables);
        AtlasEntityWithExtInfo dbEntityExtInfo = new AtlasEntityWithExtInfo(dbEntityWithTables);
        entityStore.createOrUpdate(new AtlasEntityStream(dbEntityExtInfo), false);

        // Now simulate incremental import: add a new iceberg_table
        AtlasEntity              newIcebergTable = createIcebergTableEntity("ice3_" + randomString());
        AtlasEntitiesWithExtInfo newEntity       = new AtlasEntitiesWithExtInfo();
        newEntity.addEntity(newIcebergTable);

        EntityMutationResponse newTableResponse = entityStore.createOrUpdate(new AtlasEntityStream(newEntity), false);
        String                 ice3Guid         = findEntityGuid(newTableResponse.getCreatedEntities(), ICEBERG_TABLE_TYPE);

        // Update db entity to include the new iceberg_table (incremental update)
        AtlasEntity dbEntityUpdate = new AtlasEntity(HIVE_DB_TYPE);
        dbEntityUpdate.setGuid(dbGuid);
        dbEntityUpdate.setAttribute("name", dbEntityWithTables.getAttribute("name"));

        // Include all existing tables plus the new one
        List<AtlasRelatedObjectId> allTables = new ArrayList<>();
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ht1Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ht2Guid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ice1Guid, ICEBERG_TABLE_TYPE), ICEBERG_TABLE_DB_RELATIONSHIP));
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ice2Guid, ICEBERG_TABLE_TYPE), ICEBERG_TABLE_DB_RELATIONSHIP));
        allTables.add(new AtlasRelatedObjectId(new AtlasObjectId(ice3Guid, ICEBERG_TABLE_TYPE), ICEBERG_TABLE_DB_RELATIONSHIP));

        dbEntityUpdate.setRelationshipAttribute(TABLES_ATTR_NAME, allTables);

        AtlasEntityWithExtInfo updateExtInfo  = new AtlasEntityWithExtInfo(dbEntityUpdate);
        EntityMutationResponse updateResponse = entityStore.createOrUpdate(new AtlasEntityStream(updateExtInfo), false);

        assertNotNull(updateResponse);

        // Verify all relationships are correct
        verifyRelationships(dbGuid, ht1Guid, ht2Guid, ice1Guid, ice2Guid, ice3Guid);
    }

    /**
     * Test that validates the correct relationship types are used for each entity type.
     * This ensures hive_table uses hive_table_db and iceberg_table uses iceberg_table_db.
     */
    @Test
    public void testRelationshipTypeValidation() throws Exception {
        init();

        // Create entities
        AtlasEntity dbEntity     = createHiveDbEntity("validate_db_" + randomString());
        AtlasEntity hiveTable    = createHiveTableEntity("validate_ht_" + randomString());
        AtlasEntity icebergTable = createIcebergTableEntity("validate_ice_" + randomString());

        AtlasEntitiesWithExtInfo entitiesToCreate = new AtlasEntitiesWithExtInfo();
        entitiesToCreate.addEntity(dbEntity);
        entitiesToCreate.addEntity(hiveTable);
        entitiesToCreate.addEntity(icebergTable);

        EntityMutationResponse createResponse = entityStore.createOrUpdate(new AtlasEntityStream(entitiesToCreate), false);

        String dbGuid  = findEntityGuid(createResponse.getCreatedEntities(), HIVE_DB_TYPE);
        String htGuid  = findEntityGuid(createResponse.getCreatedEntities(), HIVE_TABLE_TYPE);
        String iceGuid = findEntityGuid(createResponse.getCreatedEntities(), ICEBERG_TABLE_TYPE);

        // Update db with mixed tables
        AtlasEntity dbEntityUpdate = new AtlasEntity(HIVE_DB_TYPE);
        dbEntityUpdate.setGuid(dbGuid);
        dbEntityUpdate.setAttribute("name", dbEntity.getAttributes().get("name"));

        List<AtlasRelatedObjectId> tables = new ArrayList<>();
        tables.add(new AtlasRelatedObjectId(new AtlasObjectId(htGuid, HIVE_TABLE_TYPE), HIVE_TABLE_DB_RELATIONSHIP));
        tables.add(new AtlasRelatedObjectId(new AtlasObjectId(iceGuid, ICEBERG_TABLE_TYPE), ICEBERG_TABLE_DB_RELATIONSHIP));

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
            assertTrue(relationshipTypes.contains(ICEBERG_TABLE_DB_RELATIONSHIP));
        } catch (AtlasBaseException e) {
            // Should not get the error: "invalid relationshipDef: iceberg_table_db: end type 1: hive_db, end type 2: hive_table"
            if (e.getMessage().contains("invalid relationshipDef") && e.getMessage().contains("iceberg_table_db")) {
                fail("Should not use iceberg_table_db relationship for hive_table entities: " + e.getMessage());
            }
            throw e;
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

        // Create iceberg_table entity type
        AtlasEntityDef icebergTableDef = createClassTypeDef(ICEBERG_TABLE_TYPE, "Iceberg Table",
                Collections.emptySet(),
                createUniqueRequiredAttrDef("name", "string"),
                createOptionalAttrDef("description", "string"));

        typesDef.getEntityDefs().add(hiveDbDef);
        typesDef.getEntityDefs().add(hiveTableDef);
        typesDef.getEntityDefs().add(icebergTableDef);

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

        // Create iceberg_table_db relationship
        AtlasRelationshipDef icebergTableDbRel = new AtlasRelationshipDef();
        icebergTableDbRel.setName(ICEBERG_TABLE_DB_RELATIONSHIP);
        icebergTableDbRel.setServiceType("hive");
        icebergTableDbRel.setTypeVersion("1.0");
        icebergTableDbRel.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.AGGREGATION);
        icebergTableDbRel.setRelationshipLabel("__iceberg_table.db");
        icebergTableDbRel.setPropagateTags(AtlasRelationshipDef.PropagateTags.NONE);

        AtlasRelationshipEndDef icebergTableEnd = new AtlasRelationshipEndDef();
        icebergTableEnd.setType(ICEBERG_TABLE_TYPE);
        icebergTableEnd.setName("db");
        icebergTableEnd.setIsContainer(false);
        icebergTableEnd.setCardinality(Cardinality.SINGLE);
        icebergTableEnd.setIsLegacyAttribute(true);

        AtlasRelationshipEndDef icebergDbEnd = new AtlasRelationshipEndDef();
        icebergDbEnd.setType(HIVE_DB_TYPE);
        icebergDbEnd.setName(TABLES_ATTR_NAME);
        icebergDbEnd.setIsContainer(true);
        icebergDbEnd.setCardinality(Cardinality.SET);
        icebergDbEnd.setIsLegacyAttribute(false);

        icebergTableDbRel.setEndDef1(icebergTableEnd);
        icebergTableDbRel.setEndDef2(icebergDbEnd);

        typesDef.getRelationshipDefs().add(hiveTableDbRel);
        typesDef.getRelationshipDefs().add(icebergTableDbRel);

        return typesDef;
    }

    private AtlasEntity createHiveDbEntity(String name) {
        return createNewEntity(HIVE_DB_TYPE, name, "Test database: " + name);
    }

    private AtlasEntity createHiveTableEntity(String name) {
        return createNewEntity(HIVE_TABLE_TYPE, name, "Test hive table: " + name);
    }

    private AtlasEntity createIcebergTableEntity(String name) {
        return createNewEntity(ICEBERG_TABLE_TYPE, name, "Test iceberg table: " + name);
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
