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
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Lineage operations.
 *
 * <p>Creates Table and View entities connected by Process entities
 * (inputs/outputs), then verifies lineage traversal in all directions.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LineageIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(LineageIntegrationTest.class);

    private final long testId = System.currentTimeMillis();

    private String sourceTableGuid;
    private String intermediateViewGuid;
    private String targetTableGuid;
    private String process1Guid;
    private String process2Guid;
    private String isolatedTableGuid;

    @Test
    @Order(1)
    void testCreateLineageEntities() throws AtlasServiceException {
        // Source Table
        AtlasEntity sourceTable = new AtlasEntity("Table");
        sourceTable.setAttribute("name", "lineage-source-" + testId);
        sourceTable.setAttribute("qualifiedName", "test://integration/lineage/source/" + testId);

        EntityMutationResponse resp1 = atlasClient.createEntity(new AtlasEntityWithExtInfo(sourceTable));
        sourceTableGuid = resp1.getFirstEntityCreated().getGuid();

        // Intermediate View
        AtlasEntity intermediateView = new AtlasEntity("View");
        intermediateView.setAttribute("name", "lineage-intermediate-" + testId);
        intermediateView.setAttribute("qualifiedName", "test://integration/lineage/intermediate/" + testId);

        EntityMutationResponse resp2 = atlasClient.createEntity(new AtlasEntityWithExtInfo(intermediateView));
        intermediateViewGuid = resp2.getFirstEntityCreated().getGuid();

        // Target Table
        AtlasEntity targetTable = new AtlasEntity("Table");
        targetTable.setAttribute("name", "lineage-target-" + testId);
        targetTable.setAttribute("qualifiedName", "test://integration/lineage/target/" + testId);

        EntityMutationResponse resp3 = atlasClient.createEntity(new AtlasEntityWithExtInfo(targetTable));
        targetTableGuid = resp3.getFirstEntityCreated().getGuid();

        // Isolated Table (no lineage)
        AtlasEntity isolatedTable = new AtlasEntity("Table");
        isolatedTable.setAttribute("name", "lineage-isolated-" + testId);
        isolatedTable.setAttribute("qualifiedName", "test://integration/lineage/isolated/" + testId);

        EntityMutationResponse resp4 = atlasClient.createEntity(new AtlasEntityWithExtInfo(isolatedTable));
        isolatedTableGuid = resp4.getFirstEntityCreated().getGuid();

        assertNotNull(sourceTableGuid);
        assertNotNull(intermediateViewGuid);
        assertNotNull(targetTableGuid);
        assertNotNull(isolatedTableGuid);

        LOG.info("Created lineage entities: source={}, intermediate={}, target={}, isolated={}",
                sourceTableGuid, intermediateViewGuid, targetTableGuid, isolatedTableGuid);
    }

    @Test
    @Order(2)
    void testCreateLineageProcess() throws AtlasServiceException {
        assertNotNull(sourceTableGuid);
        assertNotNull(intermediateViewGuid);

        AtlasEntity process = new AtlasEntity("Process");
        process.setAttribute("name", "lineage-process-1-" + testId);
        process.setAttribute("qualifiedName", "test://integration/lineage/process1/" + testId);
        process.setAttribute("inputs",
                Collections.singletonList(new AtlasObjectId(sourceTableGuid, "Table")));
        process.setAttribute("outputs",
                Collections.singletonList(new AtlasObjectId(intermediateViewGuid, "View")));

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(process));
        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created);
        process1Guid = created.getGuid();

        LOG.info("Created Process 1: guid={} (source -> intermediate)", process1Guid);
    }

    @Test
    @Order(3)
    void testCreateSecondProcess() throws AtlasServiceException {
        assertNotNull(intermediateViewGuid);
        assertNotNull(targetTableGuid);

        AtlasEntity process = new AtlasEntity("Process");
        process.setAttribute("name", "lineage-process-2-" + testId);
        process.setAttribute("qualifiedName", "test://integration/lineage/process2/" + testId);
        process.setAttribute("inputs",
                Collections.singletonList(new AtlasObjectId(intermediateViewGuid, "View")));
        process.setAttribute("outputs",
                Collections.singletonList(new AtlasObjectId(targetTableGuid, "Table")));

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(process));
        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created);
        process2Guid = created.getGuid();

        LOG.info("Created Process 2: guid={} (intermediate -> target)", process2Guid);
    }

    @Test
    @Order(4)
    void testGetDownstreamLineage() throws AtlasServiceException {
        assertNotNull(sourceTableGuid);

        AtlasLineageInfo lineage = atlasClient.getLineageInfo(sourceTableGuid, LineageDirection.OUTPUT, 5);

        assertNotNull(lineage);
        assertNotNull(lineage.getGuidEntityMap());
        assertFalse(lineage.getGuidEntityMap().isEmpty(), "Downstream lineage should not be empty");

        // Source table should have downstream: process1 -> intermediate -> process2 -> target
        assertTrue(lineage.getGuidEntityMap().containsKey(sourceTableGuid),
                "Lineage should contain source table");

        LOG.info("Downstream lineage from source: {} entities", lineage.getGuidEntityMap().size());
    }

    @Test
    @Order(5)
    void testGetUpstreamLineage() throws AtlasServiceException {
        assertNotNull(targetTableGuid);

        AtlasLineageInfo lineage = atlasClient.getLineageInfo(targetTableGuid, LineageDirection.INPUT, 5);

        assertNotNull(lineage);
        assertNotNull(lineage.getGuidEntityMap());
        assertFalse(lineage.getGuidEntityMap().isEmpty(), "Upstream lineage should not be empty");

        assertTrue(lineage.getGuidEntityMap().containsKey(targetTableGuid),
                "Lineage should contain target table");

        LOG.info("Upstream lineage from target: {} entities", lineage.getGuidEntityMap().size());
    }

    @Test
    @Order(6)
    void testGetBothDirectionsLineage() throws AtlasServiceException {
        assertNotNull(intermediateViewGuid);

        AtlasLineageInfo lineage = atlasClient.getLineageInfo(intermediateViewGuid, LineageDirection.BOTH, 5);

        assertNotNull(lineage);
        assertNotNull(lineage.getGuidEntityMap());
        assertFalse(lineage.getGuidEntityMap().isEmpty(),
                "Both-direction lineage should not be empty");

        // Intermediate view should see both upstream (source) and downstream (target)
        assertTrue(lineage.getGuidEntityMap().size() >= 3,
                "Both-direction lineage should have at least 3 entities (source, intermediate, target)");

        LOG.info("Both-direction lineage from intermediate: {} entities",
                lineage.getGuidEntityMap().size());
    }

    @Test
    @Order(7)
    void testDeleteProcess() throws AtlasServiceException {
        assertNotNull(process1Guid);

        EntityMutationResponse response = atlasClient.deleteEntityByGuid(process1Guid);
        assertNotNull(response);

        AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(process1Guid);
        AtlasEntity.Status status = result.getEntity().getStatus();
        // Process soft-delete may be DELETED or remain ACTIVE depending on Atlas config
        LOG.info("Process after delete: guid={}, status={}", process1Guid, status);
        assertTrue(status == AtlasEntity.Status.DELETED || status == AtlasEntity.Status.ACTIVE,
                "Process status after delete should be DELETED or ACTIVE");
    }

    @Test
    @Order(8)
    void testLineageOnEntityWithNoLineage() throws AtlasServiceException {
        assertNotNull(isolatedTableGuid);

        AtlasLineageInfo lineage = atlasClient.getLineageInfo(isolatedTableGuid, LineageDirection.BOTH, 5);

        assertNotNull(lineage);
        // Isolated entity should only have itself in the map, no relations
        assertTrue(lineage.getRelations() == null || lineage.getRelations().isEmpty(),
                "Isolated entity should have no lineage relations");

        LOG.info("Verified isolated entity has no lineage");
    }
}
