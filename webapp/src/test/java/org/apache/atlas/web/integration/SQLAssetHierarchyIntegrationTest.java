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
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for SQL asset hierarchy: Connection → Database → Schema → Table/View → Column.
 *
 * <p>Exercises the full hierarchy creation, relationship traversal, updates, and cascading deletes
 * using the in-process Atlas server with testcontainers infrastructure.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SQLAssetHierarchyIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(SQLAssetHierarchyIntegrationTest.class);

    private static final String CONNECTOR_NAME = "snowflake";
    private final long testId = System.currentTimeMillis();

    private String connectionGuid;
    private String connectionQN;
    private String databaseGuid;
    private String databaseQN;
    private String schemaGuid;
    private String schemaQN;
    private String tableGuid;
    private String tableQN;
    private String viewGuid;
    private String viewQN;
    private final List<String> columnGuids = new ArrayList<>();
    private String columnQN1;

    @Test
    @Order(1)
    void testCreateConnection() throws AtlasServiceException {
        connectionQN = "default/" + CONNECTOR_NAME + "/" + testId;
        AtlasEntity entity = new AtlasEntity("Connection");
        entity.setAttribute("name", "test-connection-" + testId);
        entity.setAttribute("qualifiedName", connectionQN);
        entity.setAttribute("connectorName", CONNECTOR_NAME);
        entity.setAttribute("category", "warehouse");

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        assertEquals("Connection", created.getTypeName());
        connectionGuid = created.getGuid();
        LOG.info("Created Connection: guid={}, qn={}", connectionGuid, connectionQN);
    }

    @Test
    @Order(2)
    void testCreateDatabase() throws AtlasServiceException {
        assertNotNull(connectionGuid);

        databaseQN = connectionQN + "/db-" + testId;
        AtlasEntity entity = new AtlasEntity("Database");
        entity.setAttribute("name", "test-database-" + testId);
        entity.setAttribute("qualifiedName", databaseQN);
        entity.setAttribute("connectorName", CONNECTOR_NAME);
        entity.setAttribute("connectionQualifiedName", connectionQN);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        assertEquals("Database", created.getTypeName());
        databaseGuid = created.getGuid();
        LOG.info("Created Database: guid={}, qn={}", databaseGuid, databaseQN);
    }

    @Test
    @Order(3)
    void testCreateSchema() throws AtlasServiceException {
        assertNotNull(databaseGuid);

        schemaQN = databaseQN + "/schema-" + testId;
        AtlasEntity entity = new AtlasEntity("Schema");
        entity.setAttribute("name", "test-schema-" + testId);
        entity.setAttribute("qualifiedName", schemaQN);
        entity.setAttribute("connectorName", CONNECTOR_NAME);
        entity.setAttribute("connectionQualifiedName", connectionQN);
        entity.setAttribute("databaseQualifiedName", databaseQN);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        assertEquals("Schema", created.getTypeName());
        schemaGuid = created.getGuid();
        LOG.info("Created Schema: guid={}, qn={}", schemaGuid, schemaQN);
    }

    @Test
    @Order(4)
    void testCreateTable() throws AtlasServiceException {
        assertNotNull(schemaGuid);

        tableQN = schemaQN + "/table-" + testId;
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", "test-table-" + testId);
        entity.setAttribute("qualifiedName", tableQN);
        entity.setAttribute("connectorName", CONNECTOR_NAME);
        entity.setAttribute("connectionQualifiedName", connectionQN);
        entity.setAttribute("databaseQualifiedName", databaseQN);
        entity.setAttribute("schemaQualifiedName", schemaQN);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        assertEquals("Table", created.getTypeName());
        tableGuid = created.getGuid();
        LOG.info("Created Table: guid={}, qn={}", tableGuid, tableQN);
    }

    @Test
    @Order(5)
    void testCreateView() throws AtlasServiceException {
        assertNotNull(schemaGuid);

        viewQN = schemaQN + "/view-" + testId;
        AtlasEntity entity = new AtlasEntity("View");
        entity.setAttribute("name", "test-view-" + testId);
        entity.setAttribute("qualifiedName", viewQN);
        entity.setAttribute("connectorName", CONNECTOR_NAME);
        entity.setAttribute("connectionQualifiedName", connectionQN);
        entity.setAttribute("databaseQualifiedName", databaseQN);
        entity.setAttribute("schemaQualifiedName", schemaQN);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = response.getFirstEntityCreated();

        assertNotNull(created);
        assertEquals("View", created.getTypeName());
        viewGuid = created.getGuid();
        LOG.info("Created View: guid={}, qn={}", viewGuid, viewQN);
    }

    @Test
    @Order(6)
    void testCreateColumns() throws AtlasServiceException {
        assertNotNull(tableGuid);

        for (int i = 0; i < 3; i++) {
            String colQN = tableQN + "/col-" + i + "-" + testId;
            AtlasEntity entity = new AtlasEntity("Column");
            entity.setAttribute("name", "test-col-" + i);
            entity.setAttribute("qualifiedName", colQN);
            entity.setAttribute("connectorName", CONNECTOR_NAME);
            entity.setAttribute("connectionQualifiedName", connectionQN);
            entity.setAttribute("order", i);
            entity.setRelationshipAttribute("table",
                    new AtlasObjectId(tableGuid, "Table"));

            if (i == 0) {
                columnQN1 = colQN;
            }

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
            AtlasEntityHeader created = response.getFirstEntityCreated();
            assertNotNull(created);
            assertEquals("Column", created.getTypeName());
            columnGuids.add(created.getGuid());
        }

        assertEquals(3, columnGuids.size());
        LOG.info("Created {} columns for table {}", columnGuids.size(), tableGuid);
    }

    @Test
    @Order(7)
    void testGetTableWithRelationships() throws AtlasServiceException {
        assertNotNull(tableGuid);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(tableGuid, false, false);
        AtlasEntity table = result.getEntity();

        assertNotNull(table);
        assertEquals("Table", table.getTypeName());
        assertEquals(tableQN, table.getAttribute("qualifiedName"));

        Object columns = table.getRelationshipAttribute("columns");
        assertNotNull(columns, "Table should have columns relationship attribute");
        assertTrue(columns instanceof List, "Columns should be a list");
        @SuppressWarnings("unchecked")
        List<Object> columnList = (List<Object>) columns;
        assertEquals(3, columnList.size(), "Table should have 3 columns");

        LOG.info("Table has {} columns", columnList.size());
    }

    @Test
    @Order(8)
    void testGetSchemaWithRelationships() throws AtlasServiceException {
        assertNotNull(schemaGuid);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(schemaGuid, false, false);
        AtlasEntity schema = result.getEntity();

        assertNotNull(schema);
        assertEquals("Schema", schema.getTypeName());

        Object tables = schema.getRelationshipAttribute("tables");
        assertNotNull(tables, "Schema should have tables relationship");
        assertTrue(tables instanceof List);

        LOG.info("Schema retrieved with relationships");
    }

    @Test
    @Order(9)
    void testUpdateTableAttributes() throws AtlasServiceException {
        assertNotNull(tableGuid);

        AtlasEntityWithExtInfo current = atlasClient.getEntityByGuid(tableGuid);
        AtlasEntity entity = current.getEntity();
        entity.setAttribute("description", "Updated table description");
        entity.setAttribute("displayName", "Integration Test Table");

        EntityMutationResponse response = atlasClient.updateEntity(new AtlasEntityWithExtInfo(entity));
        assertNotNull(response);

        AtlasEntityWithExtInfo updated = atlasClient.getEntityByGuid(tableGuid);
        assertEquals("Updated table description", updated.getEntity().getAttribute("description"));
        assertEquals("Integration Test Table", updated.getEntity().getAttribute("displayName"));

        LOG.info("Updated table attributes: guid={}", tableGuid);
    }

    @Test
    @Order(10)
    void testSearchTableByQualifiedName() throws AtlasServiceException {
        assertNotNull(tableQN);

        Map<String, String> uniqAttrs = Collections.singletonMap("qualifiedName", tableQN);
        AtlasEntityWithExtInfo result = atlasClient.getEntityByAttribute("Table", uniqAttrs);

        assertNotNull(result);
        assertEquals(tableGuid, result.getEntity().getGuid());
        assertEquals("Table", result.getEntity().getTypeName());

        LOG.info("Found table by qualifiedName lookup");
    }

    @Test
    @Order(11)
    void testDeleteColumn() throws AtlasServiceException {
        assertFalse(columnGuids.isEmpty());

        String colGuid = columnGuids.get(0);
        EntityMutationResponse response = atlasClient.deleteEntityByGuid(colGuid);
        assertNotNull(response);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(colGuid);
        assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());

        LOG.info("Soft-deleted column: guid={}", colGuid);
    }

    @Test
    @Order(12)
    void testDeleteTable() throws AtlasServiceException {
        assertNotNull(tableGuid);

        EntityMutationResponse response = atlasClient.deleteEntityByGuid(tableGuid);
        assertNotNull(response);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(tableGuid);
        assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());

        LOG.info("Soft-deleted table: guid={}", tableGuid);
    }

    @Test
    @Order(13)
    void testDeleteConnection() throws AtlasServiceException {
        assertNotNull(connectionGuid);

        EntityMutationResponse response = atlasClient.deleteEntityByGuid(connectionGuid);
        assertNotNull(response);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(connectionGuid);
        assertEquals(AtlasEntity.Status.DELETED, result.getEntity().getStatus());

        LOG.info("Soft-deleted connection: guid={}", connectionGuid);
    }

    @Test
    @Order(14)
    void testBulkCreateHierarchy() throws AtlasServiceException {
        String bulkConnQN = "default/" + CONNECTOR_NAME + "/bulk-" + testId;
        String bulkDbQN = bulkConnQN + "/bulk-db";
        String bulkSchemaQN = bulkDbQN + "/bulk-schema";
        String bulkTableQN = bulkSchemaQN + "/bulk-table";
        String bulkColQN = bulkTableQN + "/bulk-col";

        AtlasEntity conn = new AtlasEntity("Connection");
        conn.setAttribute("name", "bulk-connection-" + testId);
        conn.setAttribute("qualifiedName", bulkConnQN);
        conn.setAttribute("connectorName", CONNECTOR_NAME);
        conn.setAttribute("category", "warehouse");

        AtlasEntity db = new AtlasEntity("Database");
        db.setAttribute("name", "bulk-database");
        db.setAttribute("qualifiedName", bulkDbQN);
        db.setAttribute("connectorName", CONNECTOR_NAME);
        db.setAttribute("connectionQualifiedName", bulkConnQN);

        AtlasEntity schema = new AtlasEntity("Schema");
        schema.setAttribute("name", "bulk-schema");
        schema.setAttribute("qualifiedName", bulkSchemaQN);
        schema.setAttribute("connectorName", CONNECTOR_NAME);
        schema.setAttribute("connectionQualifiedName", bulkConnQN);
        schema.setAttribute("databaseQualifiedName", bulkDbQN);

        AtlasEntity table = new AtlasEntity("Table");
        table.setAttribute("name", "bulk-table");
        table.setAttribute("qualifiedName", bulkTableQN);
        table.setAttribute("connectorName", CONNECTOR_NAME);
        table.setAttribute("connectionQualifiedName", bulkConnQN);
        table.setAttribute("schemaQualifiedName", bulkSchemaQN);

        AtlasEntity col = new AtlasEntity("Column");
        col.setAttribute("name", "bulk-col");
        col.setAttribute("qualifiedName", bulkColQN);
        col.setAttribute("connectorName", CONNECTOR_NAME);
        col.setAttribute("connectionQualifiedName", bulkConnQN);
        col.setAttribute("order", 0);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        entities.addEntity(conn);
        entities.addEntity(db);
        entities.addEntity(schema);
        entities.addEntity(table);
        entities.addEntity(col);

        EntityMutationResponse response = atlasClient.createEntities(entities);
        assertNotNull(response);

        List<AtlasEntityHeader> created = response.getCreatedEntities();
        assertNotNull(created);
        assertTrue(created.size() >= 5, "Should have created at least 5 entities, got " + created.size());

        LOG.info("Bulk created {} entities in hierarchy", created.size());
    }

    @Test
    @Order(15)
    void testCreateTableWithMinimalAttributes() throws AtlasServiceException {
        // Table can be created with just name and qualifiedName (connectionQN is optional)
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", "minimal-table-" + testId);
        entity.setAttribute("qualifiedName", "test://integration/minimal/" + testId);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created);
        assertEquals("Table", created.getTypeName());

        LOG.info("Created table with minimal attributes: guid={}", created.getGuid());
    }
}
