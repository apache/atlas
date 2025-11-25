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
 * This covers the scenario where a hive_db can have both hive_table and delta_table
 * entities in its "tables" attribute, using different relationship types.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class EntityGraphMapperMultipleRelationshipTypesTest extends AtlasEntityTestBase {
    private static final String HIVE_DB_TYPE                          = "hive_db";
    private static final String HIVE_TABLE_TYPE                       = "hive_table";
    private static final String DELTA_TABLE_TYPE                      = "delta_table";
    private static final String INVALID_TABLE_TYPE                    = "invalid_table";
    private static final String HIVE_COLUMN_TYPE                      = "hive_column";

    private static final String HIVE_TABLE_DB_RELATIONSHIP            = "hive_table_db";
    private static final String DELTA_TABLE_DB_RELATIONSHIP           = "delta_table_db";
    private static final String HIVE_TABLE_COLUMNS_RELATIONSHIP       = "hive_table_columns";
    private static final String HIVE_TABLE_PARTITIONKEYS_RELATIONSHIP = "hive_table_partitionkeys";

    private static final String TABLES_ATTR_NAME                      = "tables";

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

        // Create hive_column entity type
        AtlasEntityDef hiveColumnDef = createClassTypeDef(HIVE_COLUMN_TYPE, "Hive Column",
                Collections.emptySet(),
                createUniqueRequiredAttrDef("name", "string"),
                createOptionalAttrDef("isPartition", "boolean"));

        // Create invalid_table entity type (not part of any relationshipDef for tables)
        AtlasEntityDef invalidTableDef = createClassTypeDef(INVALID_TABLE_TYPE, "Invalid Table",
                Collections.emptySet(),
                createUniqueRequiredAttrDef("name", "string"),
                createOptionalAttrDef("description", "string"));

        typesDef.getEntityDefs().add(hiveDbDef);
        typesDef.getEntityDefs().add(hiveTableDef);
        typesDef.getEntityDefs().add(deltaTableDef);
        typesDef.getEntityDefs().add(hiveColumnDef);
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

        // Create hive_table_columns relationship
        AtlasRelationshipDef hiveTableColumnsRel = new AtlasRelationshipDef();
        hiveTableColumnsRel.setName(HIVE_TABLE_COLUMNS_RELATIONSHIP);
        hiveTableColumnsRel.setServiceType("hive");
        hiveTableColumnsRel.setTypeVersion("1.2");
        hiveTableColumnsRel.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.COMPOSITION);
        hiveTableColumnsRel.setRelationshipLabel("__hive_table.columns");
        hiveTableColumnsRel.setPropagateTags(AtlasRelationshipDef.PropagateTags.NONE);

        AtlasRelationshipEndDef tableColumnsEnd = new AtlasRelationshipEndDef();
        tableColumnsEnd.setType(HIVE_TABLE_TYPE);
        tableColumnsEnd.setName("columns");
        tableColumnsEnd.setIsContainer(true);
        tableColumnsEnd.setCardinality(Cardinality.SET);
        tableColumnsEnd.setIsLegacyAttribute(true);

        AtlasRelationshipEndDef columnTableEnd = new AtlasRelationshipEndDef();
        columnTableEnd.setType(HIVE_COLUMN_TYPE);
        columnTableEnd.setName("table");
        columnTableEnd.setIsContainer(false);
        columnTableEnd.setCardinality(Cardinality.SINGLE);
        columnTableEnd.setIsLegacyAttribute(true);

        hiveTableColumnsRel.setEndDef1(tableColumnsEnd);
        hiveTableColumnsRel.setEndDef2(columnTableEnd);

        // Create hive_table_partitionkeys relationship
        AtlasRelationshipDef hiveTablePartitionKeysRel = new AtlasRelationshipDef();
        hiveTablePartitionKeysRel.setName(HIVE_TABLE_PARTITIONKEYS_RELATIONSHIP);
        hiveTablePartitionKeysRel.setServiceType("hive");
        hiveTablePartitionKeysRel.setTypeVersion("1.2");
        hiveTablePartitionKeysRel.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.COMPOSITION);
        hiveTablePartitionKeysRel.setRelationshipLabel("__hive_table.partitionKeys");
        hiveTablePartitionKeysRel.setPropagateTags(AtlasRelationshipDef.PropagateTags.NONE);

        AtlasRelationshipEndDef tablePkEnd = new AtlasRelationshipEndDef();
        tablePkEnd.setType(HIVE_TABLE_TYPE);
        tablePkEnd.setName("partitionKeys");
        tablePkEnd.setIsContainer(true);
        tablePkEnd.setCardinality(Cardinality.SET);
        tablePkEnd.setIsLegacyAttribute(true);

        AtlasRelationshipEndDef columnPkEnd = new AtlasRelationshipEndDef();
        columnPkEnd.setType(HIVE_COLUMN_TYPE);
        columnPkEnd.setName("table");
        columnPkEnd.setIsContainer(false);
        columnPkEnd.setCardinality(Cardinality.SINGLE);
        columnPkEnd.setIsLegacyAttribute(true);

        hiveTablePartitionKeysRel.setEndDef1(tablePkEnd);
        hiveTablePartitionKeysRel.setEndDef2(columnPkEnd);

        typesDef.getRelationshipDefs().add(hiveTableDbRel);
        typesDef.getRelationshipDefs().add(deltaTableDbRel);
        typesDef.getRelationshipDefs().add(hiveTableColumnsRel);
        typesDef.getRelationshipDefs().add(hiveTablePartitionKeysRel);

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

    /**
     * Test that hive_table_columns and hive_table_partitionkeys relationships
     * (which share the same end types hive_table <-> hive_column) are both
     * mapped correctly by EntityGraphMapper.
     *
     * Normal columns: viewTime, userid, page_url, referrer_url, ip
     * Partition columns: dt, country
     */
    @Test
    public void testHiveTableColumnsAndPartitionKeysRelationships() throws Exception {
        init();

        // STEP 1: Create the table first so we have a real GUID to satisfy hive_column.table
        AtlasEntity tableEntity = createHiveTableEntity("cols_pk_table_" + randomString());

        AtlasEntitiesWithExtInfo tableOnly = new AtlasEntitiesWithExtInfo();
        tableOnly.addEntity(tableEntity);

        EntityMutationResponse createResponse = entityStore.createOrUpdate(new AtlasEntityStream(tableOnly), false);
        assertNotNull(createResponse);

        List<AtlasEntityHeader> createdEntities = createResponse.getCreatedEntities();
        assertNotNull(createdEntities);

        String tableGuid = findEntityGuid(createdEntities, HIVE_TABLE_TYPE);
        assertNotNull(tableGuid);

        // STEP 2: Create columns with mandatory hive_column.table relationship set
        // Normal (non-partition) columns
        AtlasRelatedObjectId tableRefColumns =
                new AtlasRelatedObjectId(new AtlasObjectId(tableGuid, HIVE_TABLE_TYPE), HIVE_TABLE_COLUMNS_RELATIONSHIP);

        AtlasEntity colViewTime    = createNewEntity(HIVE_COLUMN_TYPE, "viewTime", "non-partition column");
        colViewTime.setAttribute("isPartition", false);
        colViewTime.setRelationshipAttribute("table", tableRefColumns);

        AtlasEntity colUserId      = createNewEntity(HIVE_COLUMN_TYPE, "userid", "non-partition column");
        colUserId.setAttribute("isPartition", false);
        colUserId.setRelationshipAttribute("table", tableRefColumns);

        AtlasEntity colPageUrl     = createNewEntity(HIVE_COLUMN_TYPE, "page_url", "non-partition column");
        colPageUrl.setAttribute("isPartition", false);
        colPageUrl.setRelationshipAttribute("table", tableRefColumns);

        AtlasEntity colReferrerUrl = createNewEntity(HIVE_COLUMN_TYPE, "referrer_url", "non-partition column");
        colReferrerUrl.setAttribute("isPartition", false);
        colReferrerUrl.setRelationshipAttribute("table", tableRefColumns);

        AtlasEntity colIp          = createNewEntity(HIVE_COLUMN_TYPE, "ip", "non-partition column");
        colIp.setAttribute("isPartition", false);
        colIp.setRelationshipAttribute("table", tableRefColumns);

        // Partition columns use the partitionKeys relationship type
        AtlasRelatedObjectId tableRefPartitionKeys =
                new AtlasRelatedObjectId(new AtlasObjectId(tableGuid, HIVE_TABLE_TYPE), HIVE_TABLE_PARTITIONKEYS_RELATIONSHIP);

        AtlasEntity colDt      = createNewEntity(HIVE_COLUMN_TYPE, "dt", "partition column");
        colDt.setAttribute("isPartition", true);
        colDt.setRelationshipAttribute("table", tableRefPartitionKeys);

        AtlasEntity colCountry = createNewEntity(HIVE_COLUMN_TYPE, "country", "partition column");
        colCountry.setAttribute("isPartition", true);
        colCountry.setRelationshipAttribute("table", tableRefPartitionKeys);

        AtlasEntitiesWithExtInfo colsOnly = new AtlasEntitiesWithExtInfo();
        colsOnly.addEntity(colViewTime);
        colsOnly.addEntity(colUserId);
        colsOnly.addEntity(colPageUrl);
        colsOnly.addEntity(colReferrerUrl);
        colsOnly.addEntity(colIp);
        colsOnly.addEntity(colDt);
        colsOnly.addEntity(colCountry);

        EntityMutationResponse colsCreateResponse = entityStore.createOrUpdate(new AtlasEntityStream(colsOnly), false);
        assertNotNull(colsCreateResponse);

        List<AtlasEntityHeader> colCreated = colsCreateResponse.getCreatedEntities();
        assertNotNull(colCreated);

        String colViewTimeGuid    = findEntityGuid(colCreated, HIVE_COLUMN_TYPE, 0);
        String colUserIdGuid      = findEntityGuid(colCreated, HIVE_COLUMN_TYPE, 1);
        String colPageUrlGuid     = findEntityGuid(colCreated, HIVE_COLUMN_TYPE, 2);
        String colReferrerUrlGuid = findEntityGuid(colCreated, HIVE_COLUMN_TYPE, 3);
        String colIpGuid          = findEntityGuid(colCreated, HIVE_COLUMN_TYPE, 4);
        String colDtGuid          = findEntityGuid(colCreated, HIVE_COLUMN_TYPE, 5);
        String colCountryGuid     = findEntityGuid(colCreated, HIVE_COLUMN_TYPE, 6);

        assertNotNull(colViewTimeGuid);
        assertNotNull(colUserIdGuid);
        assertNotNull(colPageUrlGuid);
        assertNotNull(colReferrerUrlGuid);
        assertNotNull(colIpGuid);
        assertNotNull(colDtGuid);
        assertNotNull(colCountryGuid);

        // Verify graph edges: __hive_table.columns should point only to normal columns,
        // and __hive_table.partitionKeys only to partition columns.
        AtlasVertex tableVertex = AtlasGraphUtilsV2.findByGuid(graph, tableGuid);
        assertNotNull(tableVertex, "Table vertex should exist");

        Set<String> columnTargets        = new java.util.HashSet<>();
        Set<String> partitionKeyTargets  = new java.util.HashSet<>();

        for (Object o : tableVertex.getEdges(AtlasEdgeDirection.OUT)) {
            AtlasEdge edge = (AtlasEdge) o;

            if ("__hive_table.columns".equals(edge.getLabel())) {
                columnTargets.add(AtlasGraphUtilsV2.getIdFromVertex(edge.getInVertex()));
            } else if ("__hive_table.partitionKeys".equals(edge.getLabel())) {
                partitionKeyTargets.add(AtlasGraphUtilsV2.getIdFromVertex(edge.getInVertex()));
            }
        }

        // Normal columns must be in columns set and not in partitionKeys set
        assertTrue(columnTargets.contains(colViewTimeGuid));
        assertTrue(columnTargets.contains(colUserIdGuid));
        assertTrue(columnTargets.contains(colPageUrlGuid));
        assertTrue(columnTargets.contains(colReferrerUrlGuid));
        assertTrue(columnTargets.contains(colIpGuid));

        assertTrue(!partitionKeyTargets.contains(colViewTimeGuid));
        assertTrue(!partitionKeyTargets.contains(colUserIdGuid));
        assertTrue(!partitionKeyTargets.contains(colPageUrlGuid));
        assertTrue(!partitionKeyTargets.contains(colReferrerUrlGuid));
        assertTrue(!partitionKeyTargets.contains(colIpGuid));

        // Partition columns must be in partitionKeys set and not in columns set
        assertTrue(partitionKeyTargets.contains(colDtGuid));
        assertTrue(partitionKeyTargets.contains(colCountryGuid));

        assertTrue(!columnTargets.contains(colDtGuid));
        assertTrue(!columnTargets.contains(colCountryGuid));
    }
}
