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
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Label CRUD operations.
 *
 * <p>Exercises adding, setting, and removing labels on entities
 * using the AtlasClientV2 labels API.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LabelIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(LabelIntegrationTest.class);

    private final long testId = System.currentTimeMillis();
    private String entityGuid;

    @Test
    @Order(1)
    void testSetupEntity() throws AtlasServiceException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", "label-test-table-" + testId);
        entity.setAttribute("qualifiedName", "test://integration/labels/table/" + testId);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        entityGuid = response.getFirstEntityCreated().getGuid();
        assertNotNull(entityGuid);

        LOG.info("Created entity for label tests: guid={}", entityGuid);
    }

    @Test
    @Order(2)
    void testAddLabels() throws AtlasServiceException {
        assertNotNull(entityGuid);

        Set<String> labels = new HashSet<>();
        labels.add("label1");
        labels.add("label2");

        atlasClient.addLabels(entityGuid, labels);

        LOG.info("Added labels to entity: guid={}", entityGuid);
    }

    @Test
    @Order(3)
    void testGetEntityWithLabels() throws AtlasServiceException {
        assertNotNull(entityGuid);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(entityGuid);
        AtlasEntity entity = result.getEntity();

        assertNotNull(entity);
        assertNotNull(entity.getLabels(), "Entity should have labels");
        assertTrue(entity.getLabels().contains("label1"), "Should have label1");
        assertTrue(entity.getLabels().contains("label2"), "Should have label2");

        LOG.info("Entity has labels: {}", entity.getLabels());
    }

    @Test
    @Order(4)
    void testSetLabels() throws AtlasServiceException {
        assertNotNull(entityGuid);

        Set<String> newLabels = new HashSet<>();
        newLabels.add("label3");

        atlasClient.setLabels(entityGuid, newLabels);

        // Verify setLabels replaced all labels
        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(entityGuid);
        Set<String> labels = result.getEntity().getLabels();

        assertNotNull(labels);
        assertTrue(labels.contains("label3"), "Should have label3");
        assertFalse(labels.contains("label1"), "label1 should have been replaced");
        assertFalse(labels.contains("label2"), "label2 should have been replaced");

        LOG.info("Set labels: {}", labels);
    }

    @Test
    @Order(5)
    void testRemoveLabels() throws AtlasServiceException {
        assertNotNull(entityGuid);

        Set<String> toRemove = new HashSet<>();
        toRemove.add("label3");

        atlasClient.removeLabels(entityGuid, toRemove);

        LOG.info("Removed labels from entity: guid={}", entityGuid);
    }

    @Test
    @Order(6)
    void testVerifyLabelsRemoved() throws AtlasServiceException {
        assertNotNull(entityGuid);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(entityGuid);
        Set<String> labels = result.getEntity().getLabels();

        assertTrue(labels == null || labels.isEmpty(),
                "Labels should be empty after removal, but found: " + labels);

        LOG.info("Verified labels are empty after removal");
    }

    @Test
    @Order(7)
    void testAddLabelToNonExistentEntity() {
        String bogusGuid = "00000000-0000-0000-0000-000000000000";

        Set<String> labels = new HashSet<>();
        labels.add("test-label");

        assertThrows(AtlasServiceException.class,
                () -> atlasClient.addLabels(bogusGuid, labels),
                "Adding labels to non-existent entity should throw");

        LOG.info("Correctly rejected label addition to non-existent entity");
    }
}
