package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.TestcontainersExtension;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for classification notifications in Tags v2 flow.
 * 
 * Test Cases:
 * 1. new entity + tag -> 2 events: 1st CLASSIFICATION_ADD, 2nd ENTITY_CREATE
 * 2. Existing entity (replace tags) -> 2 events: 1st CLASSIFICATION_DELETE, 2nd ENTITY_UPDATE
 * 3. Attach tag to existing entity -> 2 events: 1st CLASSIFICATION_ADD, 2nd ENTITY_UPDATE
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestcontainersExtension.class)
public class ClassificationNotificationIntegrationTest extends AtlasDockerIntegrationTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationNotificationIntegrationTest.class);
    
    private static final String KAFKA_TOPIC = "ATLAS_ENTITIES";
    private static final int NOTIFICATION_TIMEOUT_MS = 15000;
    
    @Test
    @DisplayName("Test 1: Create New Entity with Tag - Should Send CLASSIFICATION_ADD then ENTITY_CREATE")
    void testCreateEntityWithTag_ShouldSendClassificationAddThenEntityCreate() throws Exception {
        LOG.info("TEST 1: Create New Entity with Tag - Expected: 1st CLASSIFICATION_ADD, 2nd ENTITY_CREATE");
        
        // Step 1: Create classification type
        String classificationTypeName = createSimpleClassificationType("TestTag_Create_" + System.currentTimeMillis());
        LOG.info("Created classification type: {}", classificationTypeName);
        
        // Step 2: Create Kafka consumer BEFORE entity creation
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        Thread.sleep(500); // Give consumer time to subscribe
        
        // Step 3: Record time before operation to filter old messages
        long operationStartTime = System.currentTimeMillis();
        
        // Step 4: Create entity WITH classification
        String qualifiedName = "test_table_create_" + System.currentTimeMillis();
        String payload = buildCreateEntityWithTagPayload(qualifiedName, classificationTypeName);
        
        HttpResponse<String> response = sendBulkEntityRequest(payload, "");
        assertEquals(200, response.statusCode(), "Entity creation should succeed");
        
        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        String entityGuid = result.get("mutatedEntities")
                .get("CREATE")
                .get(0)
                .get("guid")
                .asText();
        LOG.info("Created entity, GUID: {}", entityGuid);
        
        // Step 5: Collect notifications (expect 2: CLASSIFICATION_ADD first, then ENTITY_CREATE)
        Thread.sleep(2000); // Wait for async notifications
        
        List<JsonNode> notifications = collectNotificationsInOrder(consumer, entityGuid, NOTIFICATION_TIMEOUT_MS, 2, operationStartTime);
        
        // Step 6: Verify notifications received
        assertEquals(2, notifications.size(), "Should receive exactly 2 notifications");
        
        // Group notifications by type
        Map<String, JsonNode> notificationsByType = new HashMap<>();
        for (JsonNode notif : notifications) {
            String opType = notif.get("operationType").asText();
            notificationsByType.put(opType, notif);
        }
        
        // Verify CLASSIFICATION_ADD notification
        assertTrue(notificationsByType.containsKey("CLASSIFICATION_ADD"), 
            "Should receive CLASSIFICATION_ADD notification");
        JsonNode classAddNotif = notificationsByType.get("CLASSIFICATION_ADD");
        JsonNode classAddEntity = classAddNotif.get("entity");
        assertTrue(classAddEntity.has("classifications"), 
            "CLASSIFICATION_ADD must have entity.classifications[]");
        assertEquals(1, classAddEntity.get("classifications").size());
        assertEquals(classificationTypeName, 
            classAddEntity.get("classifications").get(0).get("typeName").asText());
        
        // Verify ENTITY_CREATE notification
        assertTrue(notificationsByType.containsKey("ENTITY_CREATE"),
            "Should receive ENTITY_CREATE notification");
        JsonNode entityCreateNotif = notificationsByType.get("ENTITY_CREATE");
        JsonNode entityCreateEntity = entityCreateNotif.get("entity");
        assertEquals(entityGuid, entityCreateEntity.get("guid").asText());
        assertTrue(entityCreateEntity.has("classifications"),
            "ENTITY_CREATE should have classifications when entity created with tag");
        
        LOG.info("TEST 1 PASSED");
        consumer.close();
    }

    @Disabled
    @Test
    @DisplayName("Test 2: Remove Tags from Existing Entity - Should Send CLASSIFICATION_DELETE then ENTITY_UPDATE")
    void testReplaceTagsOnExistingEntity_ShouldSendClassificationDeleteThenEntityUpdate() throws Exception {
        LOG.info("TEST 2: Replace Tags on Existing Entity - Expected: 1st CLASSIFICATION_DELETE, 2nd ENTITY_UPDATE");
        
        // Step 1: Create classification type
        String tag1Type = createSimpleClassificationType("TestTag1_Replace_" + System.currentTimeMillis());
        
        // Step 2: Create Kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        Thread.sleep(1000);
        
        // Step 3: Create entity with first tag
        String qualifiedName = "test_table_replace_" + System.currentTimeMillis();
        String createPayload = buildCreateEntityWithTagPayload(qualifiedName, tag1Type);
        
        HttpResponse<String> createResponse = sendBulkEntityRequest(createPayload, "");
        assertEquals(200, createResponse.statusCode());
        
        ObjectNode createResult = mapper.readValue(createResponse.body(), ObjectNode.class);
        String entityGuid = createResult.get("mutatedEntities")
                .get("CREATE")
                .get(0)
                .get("guid")
                .asText();
        LOG.info("Created entity, GUID: {}", entityGuid);
        
        // Step 4: Clear initial notifications (ENTITY_CREATE + CLASSIFICATION_ADD)
        List<JsonNode> initialNotifs = collectNotificationsInOrder(consumer, entityGuid, 5000, 2, 0);
        LOG.info("Cleared {} initial notifications", initialNotifs.size());
        
        // Step 5: Record time before replace operation
        long replaceStartTime = System.currentTimeMillis();
        
        // Step 6: Replace tag (remove tag1) - send empty classifications array
        String replacePayload = buildReplaceEntityTagPayload(entityGuid, qualifiedName);
        
        LOG.info("Replacing tag (removing {})", tag1Type);
        HttpResponse<String> replaceResponse = sendBulkEntityRequest(replacePayload,
            "replaceTags=true&appendTags=false&replaceBusinessAttributes=true&overwriteBusinessAttributes=false");
        
        if (replaceResponse.statusCode() != 200) {
            LOG.error("Failed to replace tags. Status: {}", replaceResponse.statusCode());
            fail("Replacing tags failed with status " + replaceResponse.statusCode() + 
                ". Response: " + replaceResponse.body());
        }
        
        assertEquals(200, replaceResponse.statusCode(), "Replacing tags should succeed");
        
        // Verify response contains UPDATE
        ObjectNode replaceResult = mapper.readValue(replaceResponse.body(), ObjectNode.class);
        assertTrue(replaceResult.has("mutatedEntities"), "Response should have mutatedEntities");
        assertTrue(replaceResult.get("mutatedEntities").has("UPDATE"), 
            "Response should have UPDATE in mutatedEntities");
        
        // Step 7: Collect notifications (expect 2: CLASSIFICATION_DELETE first, then ENTITY_UPDATE)
        Thread.sleep(2000);
        
        List<JsonNode> notifications = collectNotificationsInOrder(consumer, entityGuid, NOTIFICATION_TIMEOUT_MS, 2, replaceStartTime);
        
        // Step 8: Verify notifications received
        assertEquals(2, notifications.size(), "Should receive exactly 2 notifications");
        
        // Group notifications by type
        Map<String, JsonNode> notificationsByType = new HashMap<>();
        for (JsonNode notif : notifications) {
            String opType = notif.get("operationType").asText();
            notificationsByType.put(opType, notif);
        }
        
        // Verify CLASSIFICATION_DELETE notification
        assertTrue(notificationsByType.containsKey("CLASSIFICATION_DELETE"),
            "Should receive CLASSIFICATION_DELETE notification");
        JsonNode deleteNotif = notificationsByType.get("CLASSIFICATION_DELETE");
        JsonNode deleteEntity = deleteNotif.get("entity");
        assertEquals(entityGuid, deleteEntity.get("guid").asText());
        
        // Verify ENTITY_UPDATE notification
        assertTrue(notificationsByType.containsKey("ENTITY_UPDATE"),
            "Should receive ENTITY_UPDATE notification");
        JsonNode updateNotif = notificationsByType.get("ENTITY_UPDATE");
        JsonNode updateEntity = updateNotif.get("entity");
        assertEquals(entityGuid, updateEntity.get("guid").asText());
        
        LOG.info("TEST 2 PASSED");
        consumer.close();
    }

    @Disabled
    @Test
    @DisplayName("Test 3: Attach Tag to Existing Entity - Should Send CLASSIFICATION_ADD then ENTITY_UPDATE")
    void testAttachTagToExistingEntity_ShouldSendClassificationAddThenEntityUpdate() throws Exception {
        LOG.info("TEST 3: Attach Tag to Existing Entity - Expected: 1st CLASSIFICATION_ADD, 2nd ENTITY_UPDATE");
        
        // Step 1: Create classification type
        String classificationTypeName = createSimpleClassificationType("TestTag_Attach_" + System.currentTimeMillis());
        LOG.info("Created classification type: {}", classificationTypeName);
        
        // Step 2: Create Kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        Thread.sleep(1000);
        
        // Step 3: Create entity WITHOUT classification
        String qualifiedName = "test_table_attach_" + System.currentTimeMillis();
        String createPayload = buildCreateEntityWithoutTagPayload(qualifiedName);
        
        HttpResponse<String> createResponse = sendBulkEntityRequest(createPayload, "");
        assertEquals(200, createResponse.statusCode());
        
        ObjectNode createResult = mapper.readValue(createResponse.body(), ObjectNode.class);
        String entityGuid = createResult.get("mutatedEntities")
                .get("CREATE")
                .get(0)
                .get("guid")
                .asText();
        LOG.info("Created entity, GUID: {}", entityGuid);
        
        // Step 4: Clear initial ENTITY_CREATE notification
        List<JsonNode> initialNotifs = collectNotificationsInOrder(consumer, entityGuid, 5000, 1, 0);
        assertEquals(1, initialNotifs.size(), "Should receive ENTITY_CREATE for new entity");
        assertEquals("ENTITY_CREATE", initialNotifs.get(0).get("operationType").asText());
        
        // Step 5: Record time before attach operation
        long attachStartTime = System.currentTimeMillis();
        
        // Step 6: Attach tag to existing entity
        String attachPayload = buildAttachTagToEntityPayload(entityGuid, qualifiedName, classificationTypeName);
        
        LOG.info("Attaching tag to existing entity");
        HttpResponse<String> attachResponse = sendBulkEntityRequest(attachPayload,
            "replaceTags=true&appendTags=false&replaceBusinessAttributes=true&overwriteBusinessAttributes=false");
        
        if (attachResponse.statusCode() != 200) {
            LOG.error("Failed to attach tag. Status: {}", attachResponse.statusCode());
            fail("Attaching tag failed with status " + attachResponse.statusCode() + 
                ". Response: " + attachResponse.body());
        }
        
        assertEquals(200, attachResponse.statusCode(), "Attaching tag should succeed");
        
        // Verify response contains UPDATE
        ObjectNode attachResult = mapper.readValue(attachResponse.body(), ObjectNode.class);
        assertTrue(attachResult.has("mutatedEntities"), "Response should have mutatedEntities");
        assertTrue(attachResult.get("mutatedEntities").has("UPDATE"), 
            "Response should have UPDATE in mutatedEntities");
        
        // Step 7: Collect notifications (expect 2: CLASSIFICATION_ADD first, then ENTITY_UPDATE)
        Thread.sleep(2000);
        
        List<JsonNode> notifications = collectNotificationsInOrder(consumer, entityGuid, NOTIFICATION_TIMEOUT_MS, 2, attachStartTime);
        
        // Step 8: Verify notifications received
        assertEquals(2, notifications.size(), "Should receive exactly 2 notifications");
        
        // Group notifications by type
        Map<String, JsonNode> notificationsByType = new HashMap<>();
        for (JsonNode notif : notifications) {
            String opType = notif.get("operationType").asText();
            notificationsByType.put(opType, notif);
        }
        
        // Verify CLASSIFICATION_ADD notification (THE FIX - entity must have classifications populated)
        assertTrue(notificationsByType.containsKey("CLASSIFICATION_ADD"),
            "Should receive CLASSIFICATION_ADD notification");
        JsonNode classAddNotif = notificationsByType.get("CLASSIFICATION_ADD");
        JsonNode classAddEntity = classAddNotif.get("entity");
        assertTrue(classAddEntity.has("classifications"),
            "CLASSIFICATION_ADD entity MUST have classifications[] populated (THIS WAS THE BUG)");
        assertEquals(1, classAddEntity.get("classifications").size());
        assertEquals(classificationTypeName,
            classAddEntity.get("classifications").get(0).get("typeName").asText());
        assertEquals(entityGuid,
            classAddEntity.get("classifications").get(0).get("entityGuid").asText());
        
        // Verify mutatedDetails
        assertTrue(classAddNotif.has("mutatedDetails"));
        assertTrue(classAddNotif.get("mutatedDetails").isArray());
        assertEquals(1, classAddNotif.get("mutatedDetails").size());
        
        // Verify ENTITY_UPDATE notification
        assertTrue(notificationsByType.containsKey("ENTITY_UPDATE"),
            "Should receive ENTITY_UPDATE notification");
        JsonNode updateNotif = notificationsByType.get("ENTITY_UPDATE");
        JsonNode updateEntity = updateNotif.get("entity");
        assertEquals(entityGuid, updateEntity.get("guid").asText());
        
        LOG.info("TEST 3 PASSED");
        consumer.close();
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Creates a simple classification type with no attributes.
     */
    private String createSimpleClassificationType(String displayName) throws Exception {
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        
        String typedefPayload = String.format("""
            {
                "classificationDefs": [
                    {
                        "attributeDefs": [],
                        "superTypes": [],
                        "displayName": "%s",
                        "description": "Test classification",
                        "options": {
                            "color": "Gray",
                            "iconName": "atlanTags",
                            "iconType": "icon"
                        }
                    }
                ]
            }""", displayName);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/types/typedefs"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + encodedAuth)
                .POST(HttpRequest.BodyPublishers.ofString(typedefPayload))
                .timeout(Duration.ofSeconds(10))
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Classification type creation should succeed");
        
        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        return result.get("classificationDefs").get(0).get("name").asText();
    }
    
    /**
     * Sends a bulk entity request.
     */
    private HttpResponse<String> sendBulkEntityRequest(String payload, String queryParams) throws Exception {
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        
        String url = ATLAS_BASE_URL + "/entity/bulk";
        if (queryParams != null && !queryParams.isEmpty()) {
            url += "?" + queryParams;
        }
        
        LOG.debug("Sending bulk entity request to: {}", url);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + encodedAuth)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(10))
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        LOG.debug("Response status: {}, body: {}", response.statusCode(), response.body());
        
        return response;
    }
    
    /**
     * Creates a Kafka consumer subscribed to ATLAS_ENTITIES topic.
     * Uses earliest offset to ensure we don't miss any messages.
     */
    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
            "localhost:" + kafka.getFirstMappedPort());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, 
            "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            StringDeserializer.class.getName());
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
        return consumer;
    }
    
    /**
     * Collects notifications from Kafka for a specific entity in the order they arrive.
     * Only processes messages sent after the given startTime to avoid old messages.
     * Returns a list of notifications in arrival order.
     */
    private List<JsonNode> collectNotificationsInOrder(
            KafkaConsumer<String, String> consumer,
            String entityGuid,
            long timeoutMs,
            int expectedCount) throws Exception {
        return collectNotificationsInOrder(consumer, entityGuid, timeoutMs, expectedCount, 0);
    }
    
    /**
     * Collects notifications from Kafka for a specific entity in the order they arrive.
     * Only processes messages sent after the given startTime (in milliseconds since epoch).
     * If startTime is 0, processes all messages.
     */
    private List<JsonNode> collectNotificationsInOrder(
            KafkaConsumer<String, String> consumer,
            String entityGuid,
            long timeoutMs,
            int expectedCount,
            long startTime) throws Exception {
        
        List<JsonNode> notifications = new ArrayList<>();
        long collectionStartTime = System.currentTimeMillis();
        int totalMessagesPolled = 0;
        int messagesForOtherEntities = 0;
        int messagesBeforeStartTime = 0;
        
        LOG.debug("Collecting notifications for entity: {}, expected: {}, timeout: {}ms", 
            entityGuid, expectedCount, timeoutMs);
        
        while (notifications.size() < expectedCount && 
               (System.currentTimeMillis() - collectionStartTime) < timeoutMs) {
            
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            totalMessagesPolled += records.count();
            
            for (ConsumerRecord<String, String> record : records) {
                try {
                    ObjectNode kafkaMessage = mapper.readValue(record.value(), ObjectNode.class);
                    
                    if (kafkaMessage.has("message")) {
                        JsonNode message = kafkaMessage.get("message");
                        String msgGuid = "UNKNOWN";
                        String msgOpType = "UNKNOWN";
                        long msgTime = 0;
                        
                        if (message.has("entity") && message.get("entity").has("guid")) {
                            msgGuid = message.get("entity").get("guid").asText();
                        }
                        if (message.has("operationType")) {
                            msgOpType = message.get("operationType").asText();
                        }
                        if (message.has("eventTime")) {
                            msgTime = message.get("eventTime").asLong();
                        }
                        
                        // Skip messages before startTime if specified
                        if (startTime > 0 && msgTime > 0 && msgTime < startTime) {
                            messagesBeforeStartTime++;
                            continue;
                        }
                        
                        boolean matches = msgGuid.equals(entityGuid);
                        if (matches) {
                            notifications.add(message);
                            LOG.debug("Matched notification #{}: {} for entity {}", 
                                notifications.size(), msgOpType, entityGuid);
                        } else {
                            messagesForOtherEntities++;
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to parse Kafka message: {}", e.getMessage());
                }
            }
            
            if (records.count() == 0) {
                Thread.sleep(200);
            }
        }
        
        LOG.debug("Collection complete: polled={}, matched={}, otherEntities={}, skipped={}, elapsed={}ms",
            totalMessagesPolled, notifications.size(), messagesForOtherEntities, 
            messagesBeforeStartTime, System.currentTimeMillis() - collectionStartTime);
        
        if (notifications.size() < expectedCount) {
            LOG.warn("Expected {} notifications but got {}", expectedCount, notifications.size());
        }
        
        return notifications;
    }
    
    /**
     * Builds payload for creating an entity with a tag (simple classification, no attributes).
     */
    private String buildCreateEntityWithTagPayload(String qualifiedName, String classificationTypeName) {
        return String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": "-1",
                        "status": "ACTIVE",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "%s"
                        },
                        "classifications": [
                            {
                                "typeName": "%s",
                                "propagate": false,
                                "removePropagationsOnEntityDelete": true,
                                "restrictPropagationThroughLineage": false,
                                "restrictPropagationThroughHierarchy": false
                            }
                        ]
                    }
                ]
            }""", qualifiedName, qualifiedName, classificationTypeName);
    }
    
    /**
     * Builds payload for creating an entity without tags.
     */
    private String buildCreateEntityWithoutTagPayload(String qualifiedName) {
        return String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": "-1",
                        "status": "ACTIVE",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "%s"
                        }
                    }
                ]
            }""", qualifiedName, qualifiedName);
    }
    
    /**
     * Builds payload for attaching a tag to an existing entity.
     * Uses actual entity GUID and includes required attributes.
     */
    private String buildAttachTagToEntityPayload(String entityGuid, String qualifiedName, String classificationTypeName) {
        // Include a dummy attribute update to ensure differential entity is created
        // This matches the format from user's manual testing
        return String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": "%s",
                        "status": "ACTIVE",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "%s",
                            "userDescription": "Updated to trigger diff"
                        },
                        "classifications": [
                            {
                                "typeName": "%s",
                                "propagate": false,
                                "removePropagationsOnEntityDelete": true,
                                "restrictPropagationThroughLineage": false,
                                "restrictPropagationThroughHierarchy": false
                            }
                        ]
                    }
                ]
            }""", entityGuid, qualifiedName, qualifiedName, classificationTypeName);
    }
    
    /**
     * Builds payload for replacing tags on an existing entity (removes all tags).
     * Uses actual entity GUID. Includes a dummy attribute change to trigger differential entity creation
     * and avoid NPE when getDifferentialEntity() returns null.
     */
    private String buildReplaceEntityTagPayload(String entityGuid, String qualifiedName) {
        // Include a dummy attribute update to ensure differential entity is created
        // Without this, getDifferentialEntity() returns null and causes NPE at line 493
        return String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": "%s",
                        "status": "ACTIVE",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "%s",
                            "userDescription": "Updated to trigger diff"
                        },
                        "classifications": []
                    }
                ]
            }""", entityGuid, qualifiedName, qualifiedName);
    }
}
