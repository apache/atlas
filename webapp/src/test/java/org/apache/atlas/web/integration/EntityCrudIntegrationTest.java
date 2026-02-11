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
 * Integration test for general-purpose Entity CRUD operations via {@code AtlasClientV2}.
 *
 * <p>Exercises create, read, update, and delete of {@code Table} entities using
 * the in-process Atlas server with testcontainers infrastructure.</p>
 *
 * <p>Run with:
 * <pre>
 * mvn install -pl webapp -am -DskipTests -Drat.skip=true
 * mvn test -pl webapp -Dtest=EntityCrudIntegrationTest -Drat.skip=true
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class EntityCrudIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(EntityCrudIntegrationTest.class);

    private String singleEntityGuid;
    private String singleEntityQualifiedName;

    private final List<String> bulkEntityGuids = new ArrayList<>();

    @Test
    @Order(1)
    void testCreateSingleEntity() throws AtlasServiceException {
        AtlasEntity entity = createTableEntity("integration-test-table", "A test table entity");

        singleEntityQualifiedName = (String) entity.getAttribute("qualifiedName");

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));

        assertNotNull(response, "Mutation response should not be null");

        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created, "Should have a created entity header");
        assertNotNull(created.getGuid(), "Created entity GUID should not be null");
        assertEquals("Table", created.getTypeName());

        singleEntityGuid = created.getGuid();
        LOG.info("Created entity: guid={}, typeName={}", singleEntityGuid, created.getTypeName());
    }

    @Test
    @Order(2)
    void testGetEntityByGuid() throws AtlasServiceException {
        assertNotNull(singleEntityGuid, "Entity must be created first");

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(singleEntityGuid);

        assertNotNull(result, "Result should not be null");

        AtlasEntity entity = result.getEntity();
        assertNotNull(entity, "Entity should not be null");
        assertEquals(singleEntityGuid, entity.getGuid());
        assertEquals("Table", entity.getTypeName());
        assertEquals(AtlasEntity.Status.ACTIVE, entity.getStatus());
        assertEquals("integration-test-table", entity.getAttribute("name"));
        assertEquals("A test table entity", entity.getAttribute("description"));
        assertEquals(singleEntityQualifiedName, entity.getAttribute("qualifiedName"));

        LOG.info("Fetched entity by GUID: {}", entity.getGuid());
    }

    @Test
    @Order(3)
    void testGetEntityByUniqueAttribute() throws AtlasServiceException {
        assertNotNull(singleEntityQualifiedName, "Entity must be created first");

        Map<String, String> uniqAttrs = Collections.singletonMap("qualifiedName", singleEntityQualifiedName);
        AtlasEntityWithExtInfo result = atlasClient.getEntityByAttribute("Table", uniqAttrs);

        assertNotNull(result, "Result should not be null");

        AtlasEntity entity = result.getEntity();
        assertNotNull(entity);
        assertEquals(singleEntityGuid, entity.getGuid(), "GUID should match the previously created entity");
        assertEquals("Table", entity.getTypeName());

        LOG.info("Fetched entity by qualifiedName: guid={}", entity.getGuid());
    }

    @Test
    @Order(4)
    void testGetEntityHeaderByGuid() throws AtlasServiceException {
        assertNotNull(singleEntityGuid, "Entity must be created first");

        AtlasEntityHeader header = atlasClient.getEntityHeaderByGuid(singleEntityGuid);

        assertNotNull(header, "Header should not be null");
        assertEquals(singleEntityGuid, header.getGuid());
        assertEquals("Table", header.getTypeName());

        LOG.info("Fetched entity header: guid={}, typeName={}", header.getGuid(), header.getTypeName());
    }

    @Test
    @Order(5)
    void testUpdateEntity() throws AtlasServiceException {
        assertNotNull(singleEntityGuid, "Entity must be created first");

        // Fetch current entity
        AtlasEntityWithExtInfo current = atlasClient.getEntityByGuid(singleEntityGuid);
        AtlasEntity entity = current.getEntity();

        // Update: change description, add displayName
        entity.setAttribute("description", "Updated description");
        entity.setAttribute("displayName", "Integration Test Table");

        EntityMutationResponse response = atlasClient.updateEntity(new AtlasEntityWithExtInfo(entity));
        assertNotNull(response);

        // Verify updates persisted
        AtlasEntityWithExtInfo updated = atlasClient.getEntityByGuid(singleEntityGuid);
        AtlasEntity updatedEntity = updated.getEntity();

        assertEquals("Updated description", updatedEntity.getAttribute("description"));
        assertEquals("Integration Test Table", updatedEntity.getAttribute("displayName"));
        // Original attributes should still be there
        assertEquals("integration-test-table", updatedEntity.getAttribute("name"));
        assertEquals(singleEntityQualifiedName, updatedEntity.getAttribute("qualifiedName"));

        LOG.info("Updated entity: guid={}", singleEntityGuid);
    }

    @Test
    @Order(6)
    void testPartialUpdateByGuid() throws AtlasServiceException {
        assertNotNull(singleEntityGuid, "Entity must be created first");

        // Partial update: change only the name
        // The value is serialized as the raw JSON request body, so it must be a valid JSON string literal
        EntityMutationResponse response = atlasClient.partialUpdateEntityByGuid(
                singleEntityGuid, "\"renamed-table\"", "name");
        assertNotNull(response);

        // Verify only name changed, other attrs unchanged
        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(singleEntityGuid);
        AtlasEntity entity = result.getEntity();

        assertEquals("renamed-table", entity.getAttribute("name"));
        assertEquals("Updated description", entity.getAttribute("description"));
        assertEquals("Integration Test Table", entity.getAttribute("displayName"));

        LOG.info("Partially updated entity name: guid={}", singleEntityGuid);
    }

    @Test
    @Order(7)
    void testCreateMultipleEntities() throws AtlasServiceException {
        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        for (int i = 0; i < 3; i++) {
            entities.addEntity(createTableEntity("bulk-table-" + i, "Bulk table " + i));
        }

        EntityMutationResponse response = atlasClient.createEntities(entities);

        assertNotNull(response, "Mutation response should not be null");

        List<AtlasEntityHeader> created = response.getCreatedEntities();
        assertNotNull(created, "Created entities list should not be null");
        assertEquals(3, created.size(), "Should have created 3 entities");

        for (AtlasEntityHeader header : created) {
            assertNotNull(header.getGuid());
            bulkEntityGuids.add(header.getGuid());
        }

        LOG.info("Bulk created {} entities: {}", bulkEntityGuids.size(), bulkEntityGuids);
    }

    @Test
    @Order(8)
    void testGetEntitiesByGuids() throws AtlasServiceException {
        assertFalse(bulkEntityGuids.isEmpty(), "Bulk entities must be created first");

        AtlasEntitiesWithExtInfo result = atlasClient.getEntitiesByGuids(bulkEntityGuids);

        assertNotNull(result, "Result should not be null");
        assertNotNull(result.getEntities(), "Entities list should not be null");
        assertEquals(3, result.getEntities().size(), "Should return 3 entities");

        for (AtlasEntity entity : result.getEntities()) {
            assertTrue(bulkEntityGuids.contains(entity.getGuid()),
                    "Returned entity GUID should be in the requested list");
            assertEquals("Table", entity.getTypeName());
        }

        LOG.info("Bulk fetched {} entities", result.getEntities().size());
    }

    @Test
    @Order(9)
    void testDeleteSingleEntity() throws AtlasServiceException {
        assertNotNull(singleEntityGuid, "Entity must be created first");

        EntityMutationResponse response = atlasClient.deleteEntityByGuid(singleEntityGuid);
        assertNotNull(response);

        List<AtlasEntityHeader> deleted = response.getDeletedEntities();
        assertNotNull(deleted, "Deleted entities list should not be null");
        assertFalse(deleted.isEmpty(), "Should have at least one deleted entity");

        // Verify soft-delete: entity should still be fetchable with status=DELETED
        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(singleEntityGuid);
        AtlasEntity entity = result.getEntity();

        assertEquals(AtlasEntity.Status.DELETED, entity.getStatus(),
                "Soft-deleted entity should have DELETED status");

        LOG.info("Soft-deleted entity: guid={}, status={}", singleEntityGuid, entity.getStatus());
    }

    @Test
    @Order(10)
    void testDeleteMultipleEntities() throws AtlasServiceException {
        assertFalse(bulkEntityGuids.isEmpty(), "Bulk entities must be created first");

        EntityMutationResponse response = atlasClient.deleteEntitiesByGuids(bulkEntityGuids);
        assertNotNull(response);

        List<AtlasEntityHeader> deleted = response.getDeletedEntities();
        assertNotNull(deleted, "Deleted entities list should not be null");

        // Verify all are soft-deleted
        AtlasEntitiesWithExtInfo result = atlasClient.getEntitiesByGuids(bulkEntityGuids);
        for (AtlasEntity entity : result.getEntities()) {
            assertEquals(AtlasEntity.Status.DELETED, entity.getStatus(),
                    "Bulk soft-deleted entity " + entity.getGuid() + " should have DELETED status");
        }

        LOG.info("Bulk soft-deleted {} entities", bulkEntityGuids.size());
    }

    @Test
    @Order(11)
    void testCreateEntityMissingRequired() {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", "missing-qn-table");
        // Intentionally NOT setting qualifiedName

        assertThrows(AtlasServiceException.class,
                () -> atlasClient.createEntity(new AtlasEntityWithExtInfo(entity)),
                "Creating entity without qualifiedName should throw");

        LOG.info("Correctly rejected entity creation without qualifiedName");
    }

    @Test
    @Order(12)
    void testGetNonExistentEntity() {
        String bogusGuid = "00000000-0000-0000-0000-000000000000";

        assertThrows(AtlasServiceException.class,
                () -> atlasClient.getEntityByGuid(bogusGuid),
                "Getting non-existent entity should throw");

        LOG.info("Correctly rejected get for non-existent GUID");
    }

    private AtlasEntity createTableEntity(String name, String description) {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", name);
        entity.setAttribute("qualifiedName",
                "test://integration/table/" + name + "/" + System.currentTimeMillis());
        if (description != null) {
            entity.setAttribute("description", description);
        }
        return entity;
    }
}
