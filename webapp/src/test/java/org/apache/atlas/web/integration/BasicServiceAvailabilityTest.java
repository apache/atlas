package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.TestcontainersExtension;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestcontainersExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BasicServiceAvailabilityTest extends AtlasDockerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(BasicServiceAvailabilityTest.class);

    @Test
    @Order(1)
    @DisplayName("Test Atlas Health Check")
    void testHealthCheck() throws Exception {
        LOG.info("Testing Atlas health check...");

        // Create basic auth header
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL.replace("/api/atlas/v2", "") + "/api/atlas/admin/version"))
                .header("Authorization", "Basic "+encodedAuth)
                .GET()
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Health check response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode());
    }

    @Test
    @Order(2)
    @DisplayName("Test Get Types")
    void testGetTypes() throws Exception {
        LOG.info("Testing get types...");
        // Create basic auth header
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/types/typedefs"))
                .header("Accept", "application/json")
                .header("Authorization", "Basic "+encodedAuth)
                .GET()
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Get types response: {}", response.statusCode());
        assertEquals(200, response.statusCode());

        ObjectNode types = mapper.readValue(response.body(), ObjectNode.class);
        assertNotNull(types.get("entityDefs"));
        LOG.info("Found {} entity types", types.get("entityDefs").size());
    }

    @Test
    @Order(3)
    @DisplayName("Test Create Table Asset and Verify Kafka Notification")
    void testCreateTableAssetAndVerifyResponseAndKafkaNotification() throws Exception {
        LOG.info("Testing create asset...");

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // Create basic auth header
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

        // Create the payload
        String payload = """
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": "-1",
                        "attributes": {
                            "qualifiedName": "ANALYTICS_4",
                            "name": "ANALYTICS_4",
                            "description": "test",
                            "connectionQualifiedName": "Connection2",
                            "connectorName": "snowflake",
                            "adminGroups": ["Dev", "admin"]
                        }
                    }
                ]
            }""";

        // Create the request
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/bulk"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Basic " + encodedAuth)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(10))
                .build();

        // Send the request
        HttpResponse<String> response =
                super.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Create asset response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode());

        // Parse and verify the response
        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        assertNotNull(result.get("mutatedEntities"));
        assertTrue(result.get("mutatedEntities").has("CREATE"));
        assertEquals(1, result.get("mutatedEntities").get("CREATE").size());

        // Get the created entity's GUID
        String createdGuid = result.get("mutatedEntities")
                .get("CREATE")
                .get(0)
                .get("guid")
                .asText();

        // Wait for and verify Kafka message
        boolean messageFound = false;
        int maxAttempts = 10;
        ObjectNode kafkaMessage = null;

        for (int i = 0; i < maxAttempts && !messageFound; i++) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                ObjectNode message = mapper.readValue(record.value(), ObjectNode.class);

                // Check if this is our message by matching the GUID
                if (message.has("message") &&
                        message.get("message").has("entity") &&
                        message.get("message").get("entity").has("guid") &&
                        message.get("message").get("entity").get("guid").asText().equals(createdGuid)) {

                    messageFound = true;
                    kafkaMessage = message;
                    break;
                }
            }
        }

        assertTrue(messageFound, "Kafka message for created entity not found");
        assertNotNull(kafkaMessage, "Kafka message should not be null");

        // Verify message structure and content
        JsonNode messageNode = kafkaMessage.get("message");

        // Verify operation type
        assertEquals("ENTITY_NOTIFICATION_V2", messageNode.get("type").asText());
        assertEquals("ENTITY_CREATE", messageNode.get("operationType").asText());

        // Verify entity section
        JsonNode entityNode = messageNode.get("entity");
        assertEquals("Table", entityNode.get("typeName").asText());
        assertEquals(createdGuid, entityNode.get("guid").asText());

        // Verify attributes
        JsonNode attributesNode = entityNode.get("attributes");
        assertEquals("ANALYTICS_4", attributesNode.get("qualifiedName").asText());
        assertEquals("ANALYTICS_4", attributesNode.get("name").asText());
        assertEquals("test", attributesNode.get("description").asText());
        assertEquals("Connection2", attributesNode.get("connectionQualifiedName").asText());
        assertEquals("snowflake", attributesNode.get("connectorName").asText());

        // Verify admin groups
        assertTrue(attributesNode.get("adminGroups").isArray());
        assertEquals(2, attributesNode.get("adminGroups").size());
        assertEquals("Dev", attributesNode.get("adminGroups").get(0).asText());
        assertEquals("admin", attributesNode.get("adminGroups").get(1).asText());

        // Clean up
        consumer.close();
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafka.getFirstMappedPort());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("ATLAS_ENTITIES"));
        return consumer;
    }
}
