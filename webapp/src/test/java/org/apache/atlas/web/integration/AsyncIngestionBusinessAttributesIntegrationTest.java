package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionEventType;
import org.apache.atlas.model.instance.AtlasEntity;
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
 * Integration tests for async ingestion of business attribute operations.
 *
 * Business attributes require special setup (creating a businessMetadataDef first) and are
 * published from EntityREST (not EntityMutationService), so a separate test class is cleaner.
 *
 * Covers:
 * - ADD_OR_UPDATE_BUSINESS_ATTRIBUTES
 * - ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME
 * - REMOVE_BUSINESS_ATTRIBUTES
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AsyncIngestionBusinessAttributesIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncIngestionBusinessAttributesIntegrationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ASYNC_TOPIC = "ATLAS_ASYNC_ENTITIES";
    private static final int POLL_TIMEOUT_MS = 10000;

    private static final String BM_DISPLAY_NAME = "AsyncTestBM";
    private static final String BM_ATTR_DISPLAY_NAME = "asyncTestAttr";

    // Shared state — actual (randomized) names set during setup from API response
    private String bmName;
    private String bmAttrName;
    private String testEntityGuid;

    // =================== Config Helpers ===================

    private void setAsyncIngestionEnabled(boolean enabled) throws Exception {
        String configUrl = getAtlasBaseUrl() + "/api/atlas/configs/ENABLE_ASYNC_INGESTION";

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
        Thread.sleep(500);
    }

    // =================== HTTP Helper ===================

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
        try {
            InputStream is = (status >= 400) ? conn.getErrorStream() : conn.getInputStream();
            if (is != null) is.readAllBytes();
        } catch (Exception ignored) {}

        conn.disconnect();
        LOG.info("{} {} -> HTTP {}", method, apiPath, status);
        return status;
    }

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

    // =================== Kafka Consumer Helpers ===================

    private KafkaConsumer<String, String> createAsyncIngestionConsumer() throws Exception {
        String bootstrapServers = ApplicationProperties.get().getString("atlas.kafka.bootstrap.servers");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "async-bm-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(ASYNC_TOPIC));
        return consumer;
    }

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
                        continue;
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

    // =================== Envelope Validator ===================

    private void assertValidEnvelope(JsonNode event, long startTime) {
        assertTrue(event.has("eventId"), "Event must have eventId");
        assertTrue(event.get("eventTime").asLong() >= startTime, "eventTime must be >= startTime");
        assertNotNull(event.get("requestMetadata"), "Event must have requestMetadata");
        assertEquals("admin", event.get("requestMetadata").get("user").asText(), "User must be admin");
        assertNotNull(event.get("payload"), "Event must have payload");
    }

    // =================== Setup ===================

    @Test
    @Order(1)
    void testSetup_createBusinessMetadataDefAndEntity() throws Exception {
        // Create a businessMetadataDef with one string attribute applicable to Table
        // displayName and maxStrLength are required by Atlan's typedef validation
        // Note: Atlan randomizes the name during creation; we must read it back from the response
        String bmDefBody = "{\"businessMetadataDefs\":[{" +
                "\"name\":\"" + BM_DISPLAY_NAME + "\"," +
                "\"displayName\":\"" + BM_DISPLAY_NAME + "\"," +
                "\"description\":\"Test business metadata for async ingestion\"," +
                "\"attributeDefs\":[{" +
                "\"name\":\"" + BM_ATTR_DISPLAY_NAME + "\"," +
                "\"displayName\":\"" + BM_ATTR_DISPLAY_NAME + "\"," +
                "\"typeName\":\"string\"," +
                "\"isOptional\":true," +
                "\"cardinality\":\"SINGLE\"," +
                "\"isUnique\":false," +
                "\"isIndexable\":true," +
                "\"options\":{\"applicableEntityTypes\":\"[\\\"Table\\\"]\",\"maxStrLength\":\"100\"}" +
                "}]}]}";
        String bmRespBody = makeHttpRequestWithBody("POST", "/v2/types/typedefs", bmDefBody);
        JsonNode bmRespJson = MAPPER.readTree(bmRespBody);
        // Extract actual (randomized) names from the response
        JsonNode bmDefs = bmRespJson.get("businessMetadataDefs");
        assertNotNull(bmDefs, "Response must contain businessMetadataDefs");
        assertTrue(bmDefs.size() > 0, "Must have created at least one BM def");
        bmName = bmDefs.get(0).get("name").asText();
        JsonNode attrDefs = bmDefs.get(0).get("attributeDefs");
        assertNotNull(attrDefs, "BM def must have attributeDefs");
        assertTrue(attrDefs.size() > 0, "Must have at least one attribute def");
        bmAttrName = attrDefs.get(0).get("name").asText();
        LOG.info("Created businessMetadataDef: displayName={}, actualName={}, attrName={}", BM_DISPLAY_NAME, bmName, bmAttrName);

        // Create a Table entity for business attribute tests
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", "async-bm-test-table");
        entity.setAttribute("qualifiedName",
                "test://async-bm-test/" + System.currentTimeMillis());
        EntityMutationResponse resp = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        AtlasEntityHeader created = resp.getFirstEntityCreated();
        assertNotNull(created, "Must create entity for BM tests");
        testEntityGuid = created.getGuid();
        LOG.info("Created test entity for BM tests: guid={}", testEntityGuid);

        // Enable async ingestion
        setAsyncIngestionEnabled(true);
    }

    // =================== Test 2: Add/Update business attributes ===================

    @Test
    @Order(2)
    void testAddOrUpdateBusinessAttributes_producesKafkaEvent() throws Exception {
        assertNotNull(testEntityGuid, "Test entity must be set by setup");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // POST /v2/entity/guid/{guid}/businessmetadata?isOverwrite=false
            String body = "{\"" + bmName + "\":{\"" + bmAttrName + "\":\"test-value-1\"}}";
            int status = makeHttpRequest("POST",
                    "/v2/entity/guid/" + testEntityGuid + "/businessmetadata?isOverwrite=false",
                    body);
            assertTrue(status >= 200 && status < 300,
                    "Add business attributes should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer,
                    AsyncIngestionEventType.ADD_OR_UPDATE_BUSINESS_ATTRIBUTES,
                    startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received ADD_OR_UPDATE_BUSINESS_ATTRIBUTES event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.ADD_OR_UPDATE_BUSINESS_ATTRIBUTES,
                    event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 3: Add/Update business attributes by display name ===================

    @Test
    @Order(3)
    void testAddOrUpdateBusinessAttributesByDisplayName_producesKafkaEvent() throws Exception {
        assertNotNull(testEntityGuid, "Test entity must be set by setup");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // POST /v2/entity/guid/{guid}/businessmetadata/displayName — uses display names
            String body = "{\"" + BM_DISPLAY_NAME + "\":{\"" + BM_ATTR_DISPLAY_NAME + "\":\"test-value-2\"}}";
            int status = makeHttpRequest("POST",
                    "/v2/entity/guid/" + testEntityGuid + "/businessmetadata/displayName?isOverwrite=false",
                    body);
            assertTrue(status >= 200 && status < 300,
                    "Add business attributes by display name should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer,
                    AsyncIngestionEventType.ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME,
                    startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(),
                    "Should have received ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME,
                    event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Test 4: Remove business attributes ===================

    @Test
    @Order(4)
    void testRemoveBusinessAttributes_producesKafkaEvent() throws Exception {
        assertNotNull(testEntityGuid, "Test entity must be set by setup");

        KafkaConsumer<String, String> consumer = createAsyncIngestionConsumer();
        try {
            Thread.sleep(500);
            long startTime = System.currentTimeMillis();

            // DELETE /v2/entity/guid/{guid}/businessmetadata
            String body = "{\"" + bmName + "\":{\"" + bmAttrName + "\":\"test-value-2\"}}";
            int status = makeHttpRequest("DELETE",
                    "/v2/entity/guid/" + testEntityGuid + "/businessmetadata",
                    body);
            assertTrue(status >= 200 && status < 300,
                    "Remove business attributes should succeed, got HTTP " + status);

            Thread.sleep(2000);
            List<JsonNode> events = collectAsyncEvents(consumer,
                    AsyncIngestionEventType.REMOVE_BUSINESS_ATTRIBUTES,
                    startTime, 1, POLL_TIMEOUT_MS);

            assertFalse(events.isEmpty(), "Should have received REMOVE_BUSINESS_ATTRIBUTES event");
            JsonNode event = events.get(0);
            assertValidEnvelope(event, startTime);
            assertEquals(AsyncIngestionEventType.REMOVE_BUSINESS_ATTRIBUTES,
                    event.get("eventType").asText());
        } finally {
            consumer.close();
        }
    }

    // =================== Cleanup ===================

    @Test
    @Order(5)
    void testCleanup() throws Exception {
        setAsyncIngestionEnabled(false);

        // Clean up: delete test entity
        if (testEntityGuid != null) {
            try {
                atlasClient.deleteEntityByGuid(testEntityGuid);
            } catch (Exception e) {
                LOG.warn("Cleanup: failed to delete test entity {}", testEntityGuid, e);
            }
        }

        // Clean up: delete businessMetadataDef (use the actual randomized name)
        if (bmName != null) {
            try {
                makeHttpRequest("DELETE", "/v2/types/typedef/name/" + bmName, null);
            } catch (Exception e) {
                LOG.warn("Cleanup: failed to delete businessMetadataDef {}", bmName, e);
            }
        }

        LOG.info("Cleanup complete");
    }
}
