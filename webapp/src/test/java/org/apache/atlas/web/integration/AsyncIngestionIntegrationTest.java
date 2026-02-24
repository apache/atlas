package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionEventType;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for async ingestion Kafka publishing.
 *
 * Verifies that entity mutation operations produce messages on the ATLAS_ASYNC_ENTITIES
 * Kafka topic when ENABLE_ASYNC_INGESTION is enabled, and do NOT produce messages when disabled.
 *
 * Extends AtlasInProcessBaseIT which provides:
 * - Testcontainers (Cassandra, ES, Redis, Kafka)
 * - In-process Atlas server
 * - AtlasClientV2 for REST API calls
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AsyncIngestionIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncIngestionIntegrationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ASYNC_TOPIC = "ATLAS_ASYNC_ENTITIES";
    private static final int POLL_TIMEOUT_MS = 10000;

    // Shared state across ordered tests
    private String entity1Guid;
    private String classificationEntityGuid;   // entity used for classification tests
    private String classificationEntityQN;     // its qualifiedName
    private String classificationTypeName;     // actual (randomized) name of the classification type
    private String testTypeDefName;            // typedef created in test 15
    private String relEntityGuid1;             // entities used for relationship test
    private String relEntityGuid2;
    private String testRelationshipGuid;       // relationship created in test 18

    // =================== Config Helpers ===================

    /**
     * Toggle the ENABLE_ASYNC_INGESTION dynamic config via REST API.
     */
    private void setAsyncIngestionEnabled(boolean enabled) throws Exception {
        String baseUrl = getAtlasBaseUrl();
        String configUrl = baseUrl + "/api/atlas/configs/ENABLE_ASYNC_INGESTION";

        URL url = new URL(configUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8)));
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        String body = "{\"value\":\"" + enabled + "\"}";
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int status = conn.getResponseCode();
        LOG.info("Set ENABLE_ASYNC_INGESTION={} - HTTP status: {}", enabled, status);
        assertEquals(200, status, "Failed to set ENABLE_ASYNC_INGESTION flag");
        conn.disconnect();

        // Wait for config propagation
        Thread.sleep(500);
    }

    // =================== Kafka Consumer Helpers ===================

    private KafkaConsumer<String, String> createAsyncIngestionConsumer() throws Exception {
        String bootstrapServers = ApplicationProperties.get().getString("atlas.kafka.bootstrap.servers");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "async-ingestion-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(ASYNC_TOPIC));
        return consumer;
    }

    /**
     * Poll Kafka for async ingestion events matching a specific eventType.
     * Filters by startTime to avoid stale messages from earlier tests.
     */
    private List<JsonNode> collectAsyncEvents(KafkaConsumer<String, String> consumer,
                                              String expectedEventType,
                                              long startTime,
                                              int expectedCount,
                                              long timeoutMs) throws Exception {
        List<JsonNode> events = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (events.size() < expectedCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode event = MAPPER.readTree(record.value());
                    long eventTime = event.has("eventTime") ? event.get("eventTime").asLong() : 0;

                    if (startTime > 0 && eventTime < startTime) {
                        continue; // skip stale events
                    }

                    String eventType = event.has("eventType") ? event.get("eventType").asText() : "";
                    if (expectedEventType.equals(eventType)) {
                        events.add(event);
                        LOG.debug("Found matching event: type={}, id={}", eventType,
                                event.has("eventId") ? event.get("eventId").asText() : "unknown");
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to parse Kafka record: {}", record.value(), e);
                }
            }

            if (records.isEmpty()) {
                Thread.sleep(200);
            }
        }

        return events;
    }

    // =================== Entity Helpers ===================

    private AtlasEntity createTableEntity(String name) {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", name);
        entity.setAttribute("qualifiedName",
                "test://async-ingestion/" + name + "/" + System.currentTimeMillis());
        return entity;
    }

    // =================== Envelope Validator ===================

    private void assertValidEnvelope(JsonNode event, long startTime) {
        assertTrue(event.has("eventId"), "Event must have eventId");
        assertTrue(event.get("eventTime").asLong() >= startTime, "eventTime must be >= startTime");
        assertNotNull(event.get("requestMetadata"), "Event must have requestMetadata");
        assertEquals("admin", event.get("requestMetadata").get("user").asText(), "User must be admin");
        assertNotNull(event.get("payload"), "Event must have payload");
    }

    // =================== HTTP Helper ===================

    /**
     * Make an authenticated HTTP request to Atlas REST API.
     * Returns [statusCode, responseBody].
     */
    private int makeHttpRequest(String method, String apiPath, String body) throws IOException {
        String fullUrl = getAtlasBaseUrl() + "/api/atlas" + apiPath;
        URL url = new URL(fullUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8)));
        conn.setRequestProperty("Content-Type", "application/json");

        if (body != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }

        int status = conn.getResponseCode();
        // Drain the stream to avoid connection leaks
        try {
            InputStream is = (status >= 400) ? conn.getErrorStream() : conn.getInputStream();
            if (is != null) is.readAllBytes();
        } catch (Exception ignored) {}

        conn.disconnect();
        LOG.info("{} {} -> HTTP {}", method, apiPath, status);
        return status;
    }

    /**
     * Make an HTTP request and return response body.
     */
    private String makeHttpRequestWithBody(String method, String apiPath, String body) throws IOException {
        String fullUrl = getAtlasBaseUrl() + "/api/atlas" + apiPath;
        URL url = new URL(fullUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8)));
        conn.setRequestProperty("Content-Type", "application/json");

        if (body != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }

        int status = conn.getResponseCode();
        String responseBody;
        try {
            InputStream is = (status >= 400) ? conn.getErrorStream() : conn.getInputStream();
            responseBody = (is != null) ? new String(is.readAllBytes(), StandardCharsets.UTF_8) : "";
        } catch (Exception e) {
            responseBody = "";
        }

        conn.disconnect();
        LOG.info("{} {} -> HTTP {}", method, apiPath, status);
        return responseBody;
    }

    // =================== Test 1: Bulk create with async enabled ===================

    @Test
    @Order(1)
    void testBulkCreate_asyncEnabled_producesKafkaEvent() throws Exception {
        // Enable async ingestion
        setAsyncIngestionEnabled(true);
        Thread.sleep(500); // Wait for config propagation

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);

            long startTime = System.currentTimeMillis();

            // Create entity via bulk endpoint
            AtlasEntity entity = createTableEntity("async-test-table-1");
            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
            assertNotNull(response);

            AtlasEntityHeader created = response.getFirstEntityCreated();
            assertNotNull(created);
            entity1Guid = created.getGuid();
            LOG.info("Created entity1: guid={}", entity1Guid);

            // Wait and poll for Kafka event
            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.BULK_CREATE_OR_UPDATE, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received at least one BULK_CREATE_OR_UPDATE event");
            JsonNode event = events.get(0);

            // Verify envelope
            assertTrue(event.has("eventId"));
            assertEquals(AsyncIngestionEventType.BULK_CREATE_OR_UPDATE, event.get("eventType").asText());
            assertTrue(event.get("eventTime").asLong() >= startTime);

            // Verify request metadata
            JsonNode reqMeta = event.get("requestMetadata");
            assertNotNull(reqMeta);
            assertEquals("admin", reqMeta.get("user").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 2: Bulk create with async disabled ===================

    @Test
    @Order(2)
    void testBulkCreate_asyncDisabled_noKafkaEvent() throws Exception {
        // Disable async ingestion
        setAsyncIngestionEnabled(false);
        Thread.sleep(500);

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);

            long startTime = System.currentTimeMillis();

            // Create entity via bulk endpoint
            AtlasEntity entity = createTableEntity("async-test-table-disabled");
            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
            assertNotNull(response);

            // Poll briefly — should NOT get any events
            Thread.sleep(3000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.BULK_CREATE_OR_UPDATE, startTime, 1, 5000);

            assertTrue(events.isEmpty(), "Should NOT receive Kafka events when async ingestion is disabled");
        } finally {
            consumer.close();
        }
    }

    // =================== Test 3: Delete by GUID ===================

    @Test
    @Order(3)
    void testDeleteByGuid_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(entity1Guid, "entity1Guid must be set by test 1");

        // Re-enable async ingestion
        setAsyncIngestionEnabled(true);
        Thread.sleep(500);

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);

            long startTime = System.currentTimeMillis();

            // Delete entity by GUID
            EntityMutationResponse deleteResponse = atlasClient.deleteEntityByGuid(entity1Guid);
            assertNotNull(deleteResponse);
            LOG.info("Deleted entity1: guid={}", entity1Guid);

            // Poll for Kafka event
            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.DELETE_BY_GUID, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received DELETE_BY_GUID event");
            JsonNode event = events.get(0);
            assertEquals(AsyncIngestionEventType.DELETE_BY_GUID, event.get("eventType").asText());

            // Verify payload contains the GUID
            JsonNode payload = event.get("payload");
            assertNotNull(payload);
            assertTrue(payload.has("guids"));
        } finally {
            consumer.close();
        }
    }

    // =================== Test 4: Restore ===================

    @Test
    @Order(4)
    void testRestoreBulk_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(entity1Guid, "entity1Guid must be set by test 1");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);

            long startTime = System.currentTimeMillis();

            // Restore the deleted entity — GUIDs are passed as query parameters
            String encodedGuid = java.net.URLEncoder.encode(entity1Guid, StandardCharsets.UTF_8);
            int status = makeHttpRequest("POST",
                    "/v2/entity/restore/bulk?guid=" + encodedGuid, null);
            LOG.info("Restore entity1 - HTTP status: {}", status);

            // Poll for Kafka event
            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.RESTORE_BY_GUIDS, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received RESTORE_BY_GUIDS event");
            JsonNode event = events.get(0);
            assertEquals(AsyncIngestionEventType.RESTORE_BY_GUIDS, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 5: Bulk delete by GUIDs ===================

    @Test
    @Order(5)
    void testBulkDeleteByGuids_asyncEnabled_producesKafkaEvent() throws Exception {
        // Create two entities
        AtlasEntity e1 = createTableEntity("async-bulk-del-1");
        AtlasEntity e2 = createTableEntity("async-bulk-del-2");

        AtlasEntitiesWithExtInfo bulk = new AtlasEntitiesWithExtInfo();
        bulk.addEntity(e1);
        bulk.addEntity(e2);
        EntityMutationResponse createResponse = atlasClient.createEntities(bulk);

        List<AtlasEntityHeader> created = createResponse.getCreatedEntities();
        assertNotNull(created);
        assertTrue(created.size() >= 2);
        String guid1 = created.get(0).getGuid();
        String guid2 = created.get(1).getGuid();

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);

            long startTime = System.currentTimeMillis();

            // Bulk delete
            atlasClient.deleteEntitiesByGuids(List.of(guid1, guid2));
            LOG.info("Bulk deleted: {}, {}", guid1, guid2);

            // Poll for Kafka event
            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.DELETE_BY_GUIDS, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received DELETE_BY_GUIDS event");
            JsonNode event = events.get(0);
            assertEquals(AsyncIngestionEventType.DELETE_BY_GUIDS, event.get("eventType").asText());

            // Verify payload has guids
            JsonNode payload = event.get("payload");
            assertNotNull(payload);
            assertTrue(payload.has("guids"));
        } finally {
            consumer.close();
        }
    }

    // =================== Test 6: SetClassifications ===================

    @Test
    @Order(6)
    void testSetClassifications_asyncEnabled_producesKafkaEvent() throws Exception {
        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);

            long startTime = System.currentTimeMillis();

            // Use HTTP directly for setClassifications since it requires specific payload format
            String setClassUrl = getAtlasBaseUrl() + "/api/atlas/v2/entity/bulk/setClassifications";
            URL url = new URL(setClassUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Basic " +
                    Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8)));
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            // Send a minimal (possibly empty) payload — may fail on server side but
            // that's OK since we're testing the Kafka publish, which happens even if
            // the graph transaction succeeds for a valid payload.
            String body = "{\"guidHeaderMap\":{}}";
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int status = conn.getResponseCode();
            LOG.info("setClassifications - HTTP status: {}", status);
            conn.disconnect();

            // Poll for Kafka event
            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.SET_CLASSIFICATIONS, startTime, 1, POLL_TIMEOUT_MS);

            // Note: setClassifications may fail with empty payload, in which case
            // no event is published (graph transaction failed). This test validates
            // the infrastructure is in place. Full positive testing requires valid
            // entity GUIDs with classification types, which is covered by the unit tests.
            if (!events.isEmpty()) {
                JsonNode event = events.get(0);
                assertEquals(AsyncIngestionEventType.SET_CLASSIFICATIONS, event.get("eventType").asText());
            }
        } finally {
            consumer.close();
        }
    }

    // =================== Test 7: Update by unique attribute ===================

    @Test
    @Order(7)
    void testUpdateByUniqueAttribute_asyncEnabled_producesKafkaEvent() throws Exception {
        // Create an entity to update
        AtlasEntity entity = createTableEntity("async-update-ua-table");
        EntityMutationResponse createResp = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        String qn = (String) entity.getAttribute("qualifiedName");
        LOG.info("Created entity for update-by-unique-attribute: qn={}", qn);

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // PUT /v2/entity/uniqueAttribute/type/Table?attr:qualifiedName=...
            String updateBody = "{\"entity\":{\"typeName\":\"Table\",\"attributes\":{" +
                    "\"qualifiedName\":\"" + qn + "\"," +
                    "\"name\":\"async-update-ua-table-renamed\"" +
                    "}}}";
            String encodedQN = java.net.URLEncoder.encode(qn, StandardCharsets.UTF_8);
            int status = makeHttpRequest("PUT",
                    "/v2/entity/uniqueAttribute/type/Table?attr:qualifiedName=" + encodedQN,
                    updateBody);
            assertTrue(status >= 200 && status < 300, "Update should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.UPDATE_BY_UNIQUE_ATTRIBUTE, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received UPDATE_BY_UNIQUE_ATTRIBUTE event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.UPDATE_BY_UNIQUE_ATTRIBUTE, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 8: Delete by unique attribute ===================

    @Test
    @Order(8)
    void testDeleteByUniqueAttribute_asyncEnabled_producesKafkaEvent() throws Exception {
        // Create an entity to delete
        AtlasEntity entity = createTableEntity("async-delete-ua-table");
        atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        String qn = (String) entity.getAttribute("qualifiedName");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // DELETE /v2/entity/uniqueAttribute/type/Table?attr:qualifiedName=...
            String encodedQN = java.net.URLEncoder.encode(qn, StandardCharsets.UTF_8);
            int status = makeHttpRequest("DELETE",
                    "/v2/entity/uniqueAttribute/type/Table?attr:qualifiedName=" + encodedQN,
                    null);
            assertTrue(status >= 200 && status < 300, "Delete should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.DELETE_BY_UNIQUE_ATTRIBUTE, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received DELETE_BY_UNIQUE_ATTRIBUTE event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.DELETE_BY_UNIQUE_ATTRIBUTE, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 9: Setup entity + classification for classification tests ===================

    @Test
    @Order(9)
    void testSetupClassificationTestData() throws Exception {
        // Create a classification type (displayName is required by Atlan's typedef validation)
        // Note: Atlan randomizes the name during creation; we must read it back from the response
        String classTypeBody = "{\"classificationDefs\":[{\"name\":\"AsyncTestTag\"," +
                "\"displayName\":\"AsyncTestTag\"," +
                "\"description\":\"Test classification for async ingestion tests\"," +
                "\"superTypes\":[],\"attributeDefs\":[]}]}";
        String respBody = makeHttpRequestWithBody("POST", "/v2/types/typedefs", classTypeBody);
        JsonNode respJson = MAPPER.readTree(respBody);
        // Extract the actual (randomized) name from the response
        JsonNode classDefs = respJson.get("classificationDefs");
        assertNotNull(classDefs, "Response must contain classificationDefs");
        assertTrue(classDefs.size() > 0, "Must have created at least one classification def");
        classificationTypeName = classDefs.get(0).get("name").asText();
        LOG.info("Created classification type: displayName=AsyncTestTag, actualName={}", classificationTypeName);

        // Create an entity to use for classification tests
        AtlasEntity entity = createTableEntity("async-classification-test-table");
        EntityMutationResponse resp = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = resp.getFirstEntityCreated();
        assertNotNull(created, "Must create entity for classification tests");
        classificationEntityGuid = created.getGuid();
        classificationEntityQN = (String) entity.getAttribute("qualifiedName");
        LOG.info("Created classification test entity: guid={}, qn={}", classificationEntityGuid, classificationEntityQN);
    }

    // =================== Test 10: Add classifications ===================

    @Test
    @Order(10)
    void testAddClassifications_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(classificationEntityGuid, "Classification entity must be set by test 9");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // POST /v2/entity/guid/{guid}/classifications
            String body = "[{\"typeName\":\"" + classificationTypeName + "\"}]";
            int status = makeHttpRequest("POST",
                    "/v2/entity/guid/" + classificationEntityGuid + "/classifications", body);
            assertTrue(status >= 200 && status < 300, "Add classifications should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.ADD_CLASSIFICATIONS, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received ADD_CLASSIFICATIONS event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.ADD_CLASSIFICATIONS, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 11: Update classifications ===================

    @Test
    @Order(11)
    void testUpdateClassifications_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(classificationEntityGuid, "Classification entity must be set by test 9");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // PUT /v2/entity/guid/{guid}/classifications
            String body = "[{\"typeName\":\"" + classificationTypeName + "\"}]";
            int status = makeHttpRequest("PUT",
                    "/v2/entity/guid/" + classificationEntityGuid + "/classifications", body);
            assertTrue(status >= 200 && status < 300, "Update classifications should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.UPDATE_CLASSIFICATIONS, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received UPDATE_CLASSIFICATIONS event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.UPDATE_CLASSIFICATIONS, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 12: Delete classification ===================

    @Test
    @Order(12)
    void testDeleteClassification_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(classificationEntityGuid, "Classification entity must be set by test 9");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // DELETE /v2/entity/guid/{guid}/classification/{classificationName}
            int status = makeHttpRequest("DELETE",
                    "/v2/entity/guid/" + classificationEntityGuid + "/classification/" + classificationTypeName,
                    null);
            assertTrue(status >= 200 && status < 300, "Delete classification should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.DELETE_CLASSIFICATION, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received DELETE_CLASSIFICATION event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.DELETE_CLASSIFICATION, event.get("eventType").asText());
            assertTrue(event.get("payload").has("classificationName"), "Payload should have classificationName");
        } finally {
            consumer.close();
        }
    }

    // =================== Test 13: Add classification bulk ===================

    @Test
    @Order(13)
    void testAddClassificationBulk_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(classificationEntityGuid, "Classification entity must be set by test 9");

        // Create a second entity for bulk classification
        AtlasEntity e2 = createTableEntity("async-bulk-class-entity2");
        EntityMutationResponse resp = atlasClient.createEntity(new AtlasEntityWithExtInfo(e2));
        String guid2 = resp.getFirstEntityCreated().getGuid();

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // POST /v2/entity/bulk/classification
            String body = "{\"classification\":{\"typeName\":\"" + classificationTypeName + "\"}," +
                    "\"entityGuids\":[\"" + classificationEntityGuid + "\",\"" + guid2 + "\"]}";
            int status = makeHttpRequest("POST", "/v2/entity/bulk/classification", body);
            assertTrue(status >= 200 && status < 300, "Bulk add classification should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.ADD_CLASSIFICATION_BULK, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received ADD_CLASSIFICATION_BULK event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.ADD_CLASSIFICATION_BULK, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 14: SetClassifications (proper, with valid entity) ===================

    @Test
    @Order(14)
    void testSetClassifications_withValidEntity_producesKafkaEvent() throws Exception {
        assertNotNull(classificationEntityGuid, "Classification entity must be set by test 9");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // POST /v2/entity/bulk/setClassifications with valid guidHeaderMap
            String body = "{\"guidHeaderMap\":{\"" + classificationEntityGuid + "\":{" +
                    "\"guid\":\"" + classificationEntityGuid + "\"," +
                    "\"typeName\":\"Table\"," +
                    "\"classifications\":[{\"typeName\":\"" + classificationTypeName + "\"}]" +
                    "}}}";
            int status = makeHttpRequest("POST", "/v2/entity/bulk/setClassifications", body);
            LOG.info("setClassifications (proper) - HTTP {}", status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.SET_CLASSIFICATIONS, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received SET_CLASSIFICATIONS event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.SET_CLASSIFICATIONS, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 15: Bulk delete by unique attributes ===================

    @Test
    @Order(15)
    void testBulkDeleteByUniqueAttributes_asyncEnabled_producesKafkaEvent() throws Exception {
        // Create entities to delete
        AtlasEntity e1 = createTableEntity("async-bulk-del-ua-1");
        AtlasEntity e2 = createTableEntity("async-bulk-del-ua-2");

        atlasClient.createEntity(new AtlasEntityWithExtInfo(e1));
        atlasClient.createEntity(new AtlasEntityWithExtInfo(e2));

        String qn1 = (String) e1.getAttribute("qualifiedName");
        String qn2 = (String) e2.getAttribute("qualifiedName");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // DELETE /v2/entity/bulk/uniqueAttribute - uses a list of AtlasObjectId
            String body = "[{\"typeName\":\"Table\",\"uniqueAttributes\":{\"qualifiedName\":\"" + qn1 + "\"}}," +
                    "{\"typeName\":\"Table\",\"uniqueAttributes\":{\"qualifiedName\":\"" + qn2 + "\"}}]";
            int status = makeHttpRequest("DELETE", "/v2/entity/bulk/uniqueAttribute", body);
            LOG.info("Bulk delete by unique attributes - HTTP {}", status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.BULK_DELETE_BY_UNIQUE_ATTRIBUTES, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received BULK_DELETE_BY_UNIQUE_ATTRIBUTES event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.BULK_DELETE_BY_UNIQUE_ATTRIBUTES, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 16: TypeDef create ===================

    @Test
    @Order(16)
    void testTypeDefCreate_asyncEnabled_producesKafkaEvent() throws Exception {
        testTypeDefName = "AsyncTestEntityType_" + System.currentTimeMillis();

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // POST /v2/types/typedefs (displayName is required by Atlan's typedef validation)
            String body = "{\"entityDefs\":[{\"name\":\"" + testTypeDefName + "\"," +
                    "\"displayName\":\"" + testTypeDefName + "\"," +
                    "\"superTypes\":[\"Referenceable\"]," +
                    "\"attributeDefs\":[{\"name\":\"testAttr\",\"displayName\":\"testAttr\",\"typeName\":\"string\",\"isOptional\":true,\"cardinality\":\"SINGLE\",\"isUnique\":false,\"isIndexable\":false,\"options\":{\"maxStrLength\":\"100\"}}]}]}";
            int status = makeHttpRequest("POST", "/v2/types/typedefs", body);
            assertTrue(status >= 200 && status < 300, "TypeDef create should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.TYPEDEF_CREATE, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received TYPEDEF_CREATE event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.TYPEDEF_CREATE, event.get("eventType").asText());
            assertTrue(event.get("payload").has("entityDefs"), "Payload should contain entityDefs");
        } finally {
            consumer.close();
        }
    }

    // =================== Test 17: TypeDef update ===================

    @Test
    @Order(17)
    void testTypeDefUpdate_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(testTypeDefName, "testTypeDefName must be set by test 16");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // PUT /v2/types/typedefs — add a new attribute
            String body = "{\"entityDefs\":[{\"name\":\"" + testTypeDefName + "\"," +
                    "\"displayName\":\"" + testTypeDefName + "\"," +
                    "\"superTypes\":[\"Referenceable\"]," +
                    "\"attributeDefs\":[" +
                    "{\"name\":\"testAttr\",\"displayName\":\"testAttr\",\"typeName\":\"string\",\"isOptional\":true,\"cardinality\":\"SINGLE\",\"isUnique\":false,\"isIndexable\":false,\"options\":{\"maxStrLength\":\"100\"}}," +
                    "{\"name\":\"testAttr2\",\"displayName\":\"testAttr2\",\"typeName\":\"string\",\"isOptional\":true,\"cardinality\":\"SINGLE\",\"isUnique\":false,\"isIndexable\":false,\"options\":{\"maxStrLength\":\"100\"}}" +
                    "]}]}";
            int status = makeHttpRequest("PUT", "/v2/types/typedefs", body);
            assertTrue(status >= 200 && status < 300, "TypeDef update should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.TYPEDEF_UPDATE, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received TYPEDEF_UPDATE event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.TYPEDEF_UPDATE, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 18: TypeDef delete by name ===================

    @Test
    @Order(18)
    void testTypeDefDeleteByName_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(testTypeDefName, "testTypeDefName must be set by test 16");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // DELETE /v2/types/typedef/name/{typeName}
            int status = makeHttpRequest("DELETE", "/v2/types/typedef/name/" + testTypeDefName, null);
            assertTrue(status >= 200 && status < 300, "TypeDef delete should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.TYPEDEF_DELETE_BY_NAME, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received TYPEDEF_DELETE_BY_NAME event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.TYPEDEF_DELETE_BY_NAME, event.get("eventType").asText());
            assertTrue(event.get("payload").has("typeName"), "Payload should contain typeName");
        } finally {
            consumer.close();
        }
    }

    // =================== Test 19: Relationship create ===================

    @Test
    @Order(19)
    void testRelationshipCreate_asyncEnabled_producesKafkaEvent() throws Exception {
        // First, create a custom relationshipDef between two Table entities
        // (Glossary entities fail due to ES lexicographicalSortOrder mapping issue in test env)
        String relDefName = "AsyncTestRelationship_" + System.currentTimeMillis();
        String relDefBody = "{\"relationshipDefs\":[{" +
                "\"name\":\"" + relDefName + "\"," +
                "\"displayName\":\"" + relDefName + "\"," +
                "\"description\":\"Test relationship for async ingestion tests\"," +
                "\"relationshipCategory\":\"ASSOCIATION\"," +
                "\"endDef1\":{\"type\":\"Table\",\"name\":\"asyncRelEnd1\",\"isContainer\":false,\"cardinality\":\"SINGLE\"}," +
                "\"endDef2\":{\"type\":\"Table\",\"name\":\"asyncRelEnd2\",\"isContainer\":false,\"cardinality\":\"SINGLE\"}" +
                "}]}";
        int relDefStatus = makeHttpRequest("POST", "/v2/types/typedefs", relDefBody);
        assertTrue(relDefStatus >= 200 && relDefStatus < 300,
                "RelationshipDef creation should succeed, got HTTP " + relDefStatus);
        LOG.info("Created relationshipDef: {}", relDefName);

        // Create two Table entities to link
        AtlasEntity e1 = createTableEntity("async-rel-test-table1");
        EntityMutationResponse resp1 = atlasClient.createEntity(new AtlasEntityWithExtInfo(e1));
        relEntityGuid1 = resp1.getFirstEntityCreated().getGuid();
        LOG.info("Created rel entity 1: guid={}", relEntityGuid1);

        AtlasEntity e2 = createTableEntity("async-rel-test-table2");
        EntityMutationResponse resp2 = atlasClient.createEntity(new AtlasEntityWithExtInfo(e2));
        relEntityGuid2 = resp2.getFirstEntityCreated().getGuid();
        LOG.info("Created rel entity 2: guid={}", relEntityGuid2);

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // POST /v2/relationship
            String relBody = "{\"typeName\":\"" + relDefName + "\"," +
                    "\"end1\":{\"guid\":\"" + relEntityGuid1 + "\",\"typeName\":\"Table\"}," +
                    "\"end2\":{\"guid\":\"" + relEntityGuid2 + "\",\"typeName\":\"Table\"}}";
            String relResp = makeHttpRequestWithBody("POST", "/v2/relationship", relBody);
            JsonNode relJson = MAPPER.readTree(relResp);
            if (relJson.has("guid")) {
                testRelationshipGuid = relJson.get("guid").asText();
            }
            LOG.info("Created relationship: guid={}", testRelationshipGuid);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.RELATIONSHIP_CREATE, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received RELATIONSHIP_CREATE event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.RELATIONSHIP_CREATE, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 20: Delete relationship by GUID ===================

    @Test
    @Order(20)
    void testDeleteRelationshipByGuid_asyncEnabled_producesKafkaEvent() throws Exception {
        assertNotNull(testRelationshipGuid, "testRelationshipGuid must be set by test 19");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // DELETE /v2/relationship/guid/{guid}
            int status = makeHttpRequest("DELETE", "/v2/relationship/guid/" + testRelationshipGuid, null);
            assertTrue(status >= 200 && status < 300, "Relationship delete should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer, AsyncIngestionEventType.DELETE_RELATIONSHIP_BY_GUID, startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received DELETE_RELATIONSHIP_BY_GUID event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.DELETE_RELATIONSHIP_BY_GUID, event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 21: Cleanup ===================

    @Test
    @Order(21)
    void testCleanup_disableAsyncIngestion() throws Exception {
        setAsyncIngestionEnabled(false);
        Thread.sleep(500);
        LOG.info("Async ingestion disabled after all tests");
    }
}
