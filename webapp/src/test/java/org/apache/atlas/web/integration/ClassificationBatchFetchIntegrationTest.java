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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the classification batch fetch optimization.
 *
 * <p>Validates three optimization paths:</p>
 * <ol>
 *   <li><b>Batch prefetch</b>: Bulk get entities by GUIDs uses parallel Cassandra queries + short-lived local cache</li>
 *   <li><b>Names-only path</b>: Search with excludeClassifications=true returns classificationNames
 *       via a lightweight Cassandra query (tag_type_name only, no JSON deserialization)</li>
 *   <li><b>Full classifications</b>: Bulk get with full classifications returns complete objects from Cassandra</li>
 * </ol>
 *
 * <p>Requires Atlas with TagV2 enabled (Cassandra-backed classifications).</p>
 *
 * <p>Run in staging:</p>
 * <pre>
 * mvn test -pl webapp -Dtest=ClassificationBatchFetchIntegrationTest -Drat.skip=true
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ClassificationBatchFetchIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(ClassificationBatchFetchIntegrationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int ES_SYNC_WAIT_MS = 5000;

    private final long testId = System.currentTimeMillis();
    private final String tagName1 = "BatchFetchTag1_" + testId;
    private final String tagName2 = "BatchFetchTag2_" + testId;
    private final String tagName3 = "BatchFetchTag3_" + testId;

    private boolean typedefsCreated = false;
    private final List<String> entityGuids = new ArrayList<>();

    // =================== Setup: Create classification types and entities ===================

    @Test
    @Order(1)
    void testSetupClassificationTypes() throws Exception {
        AtlasClassificationDef def1 = new AtlasClassificationDef(tagName1);
        def1.setDescription("Batch fetch test tag 1");
        def1.setServiceType("atlas_core");

        AtlasClassificationDef def2 = new AtlasClassificationDef(tagName2);
        def2.setDescription("Batch fetch test tag 2");
        def2.setServiceType("atlas_core");

        AtlasClassificationDef def3 = new AtlasClassificationDef(tagName3);
        def3.setDescription("Batch fetch test tag 3");
        def3.setServiceType("atlas_core");

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setClassificationDefs(Arrays.asList(def1, def2, def3));

        AtlasTypesDef created = atlasClient.createAtlasTypeDefs(typesDef);
        assertNotNull(created);
        assertEquals(3, created.getClassificationDefs().size());

        Thread.sleep(2000);

        try {
            atlasClient.getClassificationDefByName(tagName1);
            typedefsCreated = true;
            LOG.info("Created 3 classification typedefs for batch fetch test");
        } catch (AtlasServiceException e) {
            typedefsCreated = false;
            LOG.warn("Classification typedef not retrievable: {}", e.getMessage());
        }
    }

    @Test
    @Order(2)
    void testSetupEntitiesWithClassifications() throws Exception {
        Assumptions.assumeTrue(typedefsCreated, "Typedefs not created");

        // Entity 1: has tagName1
        AtlasEntity entity1 = createTableEntity("batch-fetch-entity1-" + testId);
        entity1.setClassifications(Collections.singletonList(new AtlasClassification(tagName1)));
        EntityMutationResponse resp1 = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity1));
        entityGuids.add(resp1.getFirstEntityCreated().getGuid());

        // Entity 2: has tagName1 and tagName2
        AtlasEntity entity2 = createTableEntity("batch-fetch-entity2-" + testId);
        entity2.setClassifications(Arrays.asList(
                new AtlasClassification(tagName1),
                new AtlasClassification(tagName2)));
        EntityMutationResponse resp2 = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity2));
        entityGuids.add(resp2.getFirstEntityCreated().getGuid());

        // Entity 3: has tagName3
        AtlasEntity entity3 = createTableEntity("batch-fetch-entity3-" + testId);
        entity3.setClassifications(Collections.singletonList(new AtlasClassification(tagName3)));
        EntityMutationResponse resp3 = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity3));
        entityGuids.add(resp3.getFirstEntityCreated().getGuid());

        // Entity 4: no classifications
        AtlasEntity entity4 = createTableEntity("batch-fetch-entity4-" + testId);
        EntityMutationResponse resp4 = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity4));
        entityGuids.add(resp4.getFirstEntityCreated().getGuid());

        assertEquals(4, entityGuids.size());
        LOG.info("Created 4 entities: 3 with classifications, 1 without. GUIDs: {}", entityGuids);

        // Wait for ES indexing
        Thread.sleep(ES_SYNC_WAIT_MS);
    }

    // =================== Test 1: Bulk get entities returns correct classifications ===================

    @Test
    @Order(3)
    void testBulkGetEntitiesReturnsCorrectClassifications() throws AtlasServiceException {
        Assumptions.assumeTrue(entityGuids.size() == 4, "Entities not created");

        AtlasEntitiesWithExtInfo result = atlasClient.getEntitiesByGuids(entityGuids);

        assertNotNull(result);
        assertNotNull(result.getEntities());
        assertEquals(4, result.getEntities().size());

        for (AtlasEntity entity : result.getEntities()) {
            String guid = entity.getGuid();
            List<AtlasClassification> classifications = entity.getClassifications();

            if (guid.equals(entityGuids.get(0))) {
                // Entity 1: has tagName1
                assertNotNull(classifications, "Entity 1 should have classifications");
                assertEquals(1, classifications.size());
                assertEquals(tagName1, classifications.get(0).getTypeName());

            } else if (guid.equals(entityGuids.get(1))) {
                // Entity 2: has tagName1 and tagName2
                assertNotNull(classifications, "Entity 2 should have classifications");
                assertEquals(2, classifications.size());
                List<String> names = classifications.stream()
                        .map(AtlasClassification::getTypeName)
                        .sorted()
                        .collect(java.util.stream.Collectors.toList());
                assertTrue(names.contains(tagName1));
                assertTrue(names.contains(tagName2));

            } else if (guid.equals(entityGuids.get(2))) {
                // Entity 3: has tagName3
                assertNotNull(classifications, "Entity 3 should have classifications");
                assertEquals(1, classifications.size());
                assertEquals(tagName3, classifications.get(0).getTypeName());

            } else if (guid.equals(entityGuids.get(3))) {
                // Entity 4: no classifications
                assertTrue(classifications == null || classifications.isEmpty(),
                        "Entity 4 should have no classifications");
            }
        }

        LOG.info("Bulk get returned correct classifications for all 4 entities");
    }

    // =================== Test 2: Search with names-only returns classificationNames ===================

    @Test
    @Order(4)
    void testSearchNamesOnlyReturnsClassificationNames() throws Exception {
        Assumptions.assumeTrue(entityGuids.size() == 4, "Entities not created");

        // Use index search with excludeClassifications=true, includeClassificationNames=true
        String searchPayload = MAPPER.writeValueAsString(MAPPER.createObjectNode()
                .put("typeName", "Table")
                .put("excludeDeletedEntities", true)
                .put("includeClassificationNames", true)
                .put("excludeClassifications", true)
                .put("limit", 10)
                .set("query", MAPPER.createObjectNode()
                        .put("size", 10)
                        .set("query", MAPPER.createObjectNode()
                                .set("bool", MAPPER.createObjectNode()
                                        .set("must", MAPPER.createArrayNode()
                                                .add(MAPPER.createObjectNode()
                                                        .set("terms", MAPPER.createObjectNode()
                                                                .set("__guid", MAPPER.createArrayNode()
                                                                        .add(entityGuids.get(0))
                                                                        .add(entityGuids.get(1))))))))));

        String auth = Base64.getEncoder().encodeToString("admin:admin".getBytes());
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:21000/api/atlas/v2/search/indexsearch"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + auth)
                .POST(HttpRequest.BodyPublishers.ofString(searchPayload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "Search should return 200. Body: " + response.body());

        JsonNode searchResult = MAPPER.readTree(response.body());
        JsonNode entities = searchResult.get("entities");
        assertNotNull(entities, "Search should return entities");
        assertTrue(entities.size() >= 1, "Should find at least 1 entity");

        for (JsonNode entityNode : entities) {
            String guid = entityNode.get("guid").asText();

            // classificationNames should be populated
            JsonNode classificationNames = entityNode.get("classificationNames");
            assertNotNull(classificationNames, "classificationNames should be present for guid=" + guid);

            // classifications (full objects) should NOT be present (excludeClassifications=true)
            JsonNode classifications = entityNode.get("classifications");
            assertTrue(classifications == null || classifications.isNull() || classifications.isEmpty(),
                    "Full classifications should be excluded for guid=" + guid);

            if (guid.equals(entityGuids.get(0))) {
                assertEquals(1, classificationNames.size());
                assertEquals(tagName1, classificationNames.get(0).asText());
            } else if (guid.equals(entityGuids.get(1))) {
                assertEquals(2, classificationNames.size());
                List<String> names = new ArrayList<>();
                classificationNames.forEach(n -> names.add(n.asText()));
                assertTrue(names.contains(tagName1));
                assertTrue(names.contains(tagName2));
            }
        }

        LOG.info("Search with names-only returned classificationNames without full objects");
    }

    // =================== Test 3: Add classification and verify bulk fetch picks it up ===================

    @Test
    @Order(5)
    void testAddClassificationThenBulkFetch() throws AtlasServiceException {
        Assumptions.assumeTrue(entityGuids.size() == 4, "Entities not created");

        // Add tagName3 to entity 4 (previously had no classifications)
        atlasClient.addClassifications(entityGuids.get(3),
                Collections.singletonList(new AtlasClassification(tagName3)));

        LOG.info("Added {} to entity 4", tagName3);

        // Bulk fetch should pick up the new classification
        AtlasEntitiesWithExtInfo result = atlasClient.getEntitiesByGuids(
                Arrays.asList(entityGuids.get(3)));

        assertNotNull(result);
        assertEquals(1, result.getEntities().size());

        AtlasEntity entity = result.getEntities().get(0);
        assertNotNull(entity.getClassifications(), "Entity 4 should now have classifications");
        assertEquals(1, entity.getClassifications().size());
        assertEquals(tagName3, entity.getClassifications().get(0).getTypeName());

        LOG.info("Bulk fetch correctly returned newly added classification");
    }

    // =================== Test 4: Remove classification and verify ===================

    @Test
    @Order(6)
    void testRemoveClassificationThenBulkFetch() throws AtlasServiceException {
        Assumptions.assumeTrue(entityGuids.size() == 4, "Entities not created");

        // Remove tagName3 from entity 4
        atlasClient.deleteClassification(entityGuids.get(3), tagName3);

        LOG.info("Removed {} from entity 4", tagName3);

        // Verify it's gone
        AtlasEntitiesWithExtInfo result = atlasClient.getEntitiesByGuids(
                Collections.singletonList(entityGuids.get(3)));

        AtlasEntity entity = result.getEntities().get(0);
        assertTrue(entity.getClassifications() == null || entity.getClassifications().isEmpty(),
                "Entity 4 should have no classifications after removal");

        LOG.info("Bulk fetch correctly reflects removed classification");
    }

    // =================== Helper ===================

    private AtlasEntity createTableEntity(String name) {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", name);
        entity.setAttribute("qualifiedName", "test://integration/batchfetch/" + name);
        return entity;
    }
}
