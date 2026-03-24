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
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for MS-701: Main asset events missing for sub-asset add.
 *
 * <p>Scenario (first example from the ticket):
 * <ol>
 *   <li>Create a Table entity (parent/main asset)</li>
 *   <li>Send a bulk createOrUpdate with the SAME Table (unchanged attributes)
 *       plus a NEW Process with inputs referencing the Table</li>
 *   <li>Expect: Table should appear as UPDATED in the REST response AND
 *       receive an ENTITY_UPDATE Kafka notification</li>
 * </ol>
 *
 * <p>Bug: The Table is marked as "unchanged" by the diff check and added to
 * entitiesToSkipUpdate. When the Process creates a relationship edge back to
 * the Table, the Table's update event is suppressed by
 * RequestContext.recordEntityUpdate() checking the skip set.</p>
 *
 * <p>Run with:
 * <pre>
 * mvn install -pl webapp -am -DskipTests -Drat.skip=true
 * mvn test -pl webapp -Dtest=SubAssetAddParentUpdateNotificationTest -Drat.skip=true
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SubAssetAddParentUpdateNotificationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(SubAssetAddParentUpdateNotificationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String KAFKA_TOPIC = "ATLAS_ENTITIES";
    private static final long KAFKA_POLL_TIMEOUT_MS = 15000;

    private final long testId = System.currentTimeMillis();

    private String tableGuid;
    private String tableQualifiedName;

    @Test
    @Order(1)
    void testCreateParentTable() throws Exception {
        LOG.info("=== Step 1: Create parent Table ===");

        AtlasEntity table = new AtlasEntity("Table");
        tableQualifiedName = "test://ms701/parent-table/" + testId;
        table.setAttribute("name", "ms701-parent-table-" + testId);
        table.setAttribute("qualifiedName", tableQualifiedName);

        EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(table));

        AtlasEntityHeader created = response.getFirstEntityCreated();
        assertNotNull(created, "Table should be created");
        tableGuid = created.getGuid();
        assertNotNull(tableGuid, "Table GUID should not be null");

        LOG.info("Created parent Table, GUID: {}", tableGuid);
    }

    @Test
    @Order(2)
    void testAddSubAsset_ParentShouldAppearAsUpdated() throws Exception {
        LOG.info("=== Step 2: MS-701 - Sub-asset add should trigger parent UPDATE ===");
        assertNotNull(tableGuid, "Table must exist from previous test");

        // Create Kafka consumer BEFORE the operation to capture notifications
        KafkaConsumer<String, String> consumer = createEntityNotificationConsumer();
        long operationStartTime = System.currentTimeMillis();

        // Build the bulk payload:
        //   - The SAME Table (no attribute changes → will be marked as "unchanged")
        //   - A NEW Process with inputs=[Table] (creates a relationship edge back to the Table)
        AtlasEntity unchangedTable = new AtlasEntity("Table");
        unchangedTable.setGuid(tableGuid);
        unchangedTable.setAttribute("name", "ms701-parent-table-" + testId);
        unchangedTable.setAttribute("qualifiedName", tableQualifiedName);

        AtlasEntity newProcess = new AtlasEntity("Process");
        newProcess.setAttribute("name", "ms701-child-process-" + testId);
        newProcess.setAttribute("qualifiedName", "test://ms701/child-process/" + testId);
        newProcess.setAttribute("inputs",
                Collections.singletonList(new AtlasObjectId(tableGuid, "Table")));

        AtlasEntitiesWithExtInfo bulkEntities = new AtlasEntitiesWithExtInfo();
        bulkEntities.addEntity(unchangedTable);
        bulkEntities.addEntity(newProcess);

        LOG.info("Sending bulk request with unchanged Table + new Process");
        EntityMutationResponse response = atlasClient.createEntities(bulkEntities);

        assertNotNull(response, "Mutation response should not be null");

        // ===== REST Response Assertions =====

        // Process should be CREATED
        List<AtlasEntityHeader> createdEntities = response.getCreatedEntities();
        assertNotNull(createdEntities, "Should have created entities");
        assertFalse(createdEntities.isEmpty(), "Should have at least 1 created entity (Process)");

        boolean processCreated = createdEntities.stream()
                .anyMatch(h -> "Process".equals(h.getTypeName()));
        assertTrue(processCreated, "Process should appear in created entities");

        // Collect all UPDATED entity GUIDs from the response
        Set<String> updatedGuids = new HashSet<>();
        List<AtlasEntityHeader> updatedEntities = response.getUpdatedEntities();
        if (updatedEntities != null) {
            for (AtlasEntityHeader header : updatedEntities) {
                updatedGuids.add(header.getGuid());
                LOG.info("UPDATED entity in response: {} ({})", header.getTypeName(), header.getGuid());
            }
        }

        List<AtlasEntityHeader> partialUpdatedEntities = response.getPartialUpdatedEntities();
        if (partialUpdatedEntities != null) {
            for (AtlasEntityHeader header : partialUpdatedEntities) {
                updatedGuids.add(header.getGuid());
                LOG.info("PARTIAL_UPDATE entity in response: {} ({})", header.getTypeName(), header.getGuid());
            }
        }

        LOG.info("All updated GUIDs in REST response: {}", updatedGuids);

        // KEY ASSERTION 1: The Table should appear as UPDATED in the REST response
        assertTrue(updatedGuids.contains(tableGuid),
                "MS-701 BUG: Table should appear as UPDATED in REST response when a new " +
                "sub-asset (Process) creates a relationship to it, even though the Table's " +
                "own attributes are unchanged. Table GUID: " + tableGuid +
                ", Updated GUIDs: " + updatedGuids);

        LOG.info("REST assertion passed: Table correctly appears as UPDATED");

        // ===== Kafka Notification Assertions =====

        // Wait briefly for async notification delivery
        Thread.sleep(3000);

        List<JsonNode> tableUpdateNotifications = collectEntityUpdateNotifications(
                consumer, tableGuid, operationStartTime, KAFKA_POLL_TIMEOUT_MS);

        LOG.info("Kafka ENTITY_UPDATE notifications for Table: {}", tableUpdateNotifications.size());
        for (JsonNode notif : tableUpdateNotifications) {
            LOG.info("  Notification: {}", notif);
        }

        // KEY ASSERTION 2: Table should have received an ENTITY_UPDATE Kafka notification
        assertFalse(tableUpdateNotifications.isEmpty(),
                "MS-701 BUG: Table should receive ENTITY_UPDATE Kafka notification when a new " +
                "sub-asset (Process) creates a relationship to it. Table GUID: " + tableGuid);

        LOG.info("=== TEST PASSED: Table appears as UPDATED in both REST response and Kafka ===");
        consumer.close();
    }

    // ==================== Kafka Helper Methods ====================

    /**
     * Creates a Kafka consumer subscribed to ATLAS_ENTITIES topic.
     * Uses the same bootstrap servers configured for the in-process Atlas server.
     */
    private KafkaConsumer<String, String> createEntityNotificationConsumer() throws Exception {
        String bootstrapServers = ApplicationProperties.get().getString("atlas.kafka.bootstrap.servers");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ms701-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
        return consumer;
    }

    /**
     * Polls Kafka for ENTITY_UPDATE notifications matching the given entity GUID.
     * Filters by startTime to avoid stale messages from earlier tests.
     *
     * <p>Message format on ATLAS_ENTITIES topic:
     * <pre>
     * { "message": { "operationType": "ENTITY_UPDATE", "entity": { "guid": "...", "typeName": "..." }, "eventTime": ... } }
     * </pre>
     */
    private List<JsonNode> collectEntityUpdateNotifications(
            KafkaConsumer<String, String> consumer,
            String targetGuid,
            long startTime,
            long timeoutMs) {

        List<JsonNode> matched = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode kafkaMessage = MAPPER.readTree(record.value());

                    if (!kafkaMessage.has("message")) {
                        continue;
                    }

                    JsonNode message = kafkaMessage.get("message");
                    long eventTime = message.has("eventTime") ? message.get("eventTime").asLong() : 0;

                    // Skip stale messages
                    if (startTime > 0 && eventTime > 0 && eventTime < startTime) {
                        continue;
                    }

                    String opType = message.has("operationType") ? message.get("operationType").asText() : "";
                    String guid = message.has("entity") && message.get("entity").has("guid")
                            ? message.get("entity").get("guid").asText() : "";

                    if ("ENTITY_UPDATE".equals(opType) && targetGuid.equals(guid)) {
                        matched.add(message);
                        LOG.debug("Found ENTITY_UPDATE for target GUID {}", targetGuid);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to parse Kafka message: {}", e.getMessage());
                }
            }

            // If we found what we need, no need to keep polling
            if (!matched.isEmpty()) {
                break;
            }

            if (records.isEmpty()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        return matched;
    }
}
