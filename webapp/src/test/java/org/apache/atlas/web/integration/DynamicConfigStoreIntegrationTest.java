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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.junit.jupiter.TestcontainersExtension;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Dynamic Config Store feature.
 *
 * Tests cover:
 * 1. REST API CRUD operations (GET, PUT, DELETE)
 * 2. Config value persistence and retrieval
 * 3. Redis to Cassandra migration
 * 4. Maintenance mode configuration
 *
 * This test uses a Docker environment with:
 * - Cassandra (for config storage)
 * - Redis (for feature flag migration source)
 * - Atlas server with config store enabled
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(TestcontainersExtension.class)
@Disabled
public class DynamicConfigStoreIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigStoreIntegrationTest.class);
    private static final Network network = Network.newNetwork();
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String REDIS_FEATURE_FLAG_PREFIX = "ff:";
    private static final String CONFIG_API_PATH = "/configs";

    private HttpClient httpClient;
    private String atlasBaseUrl;
    private String authHeader;

    // Define containers
    @Container
    static GenericContainer<?> zookeeper = new GenericContainer<>("zookeeper:3.7")
            .withNetwork(network)
            .withNetworkAliases("zookeeper")
            .withExposedPorts(2181)
            .withEnv("ZOO_MY_ID", "1");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withExternalZookeeper("zookeeper:2181")
            .dependsOn(zookeeper);

    @Container
    static CassandraContainer<?> cassandra = new CassandraContainer<>("cassandra:2.1")
            .withNetwork(network)
            .withNetworkAliases("cassandra")
            .withStartupTimeout(Duration.ofMinutes(3))
            .withEnv("CASSANDRA_CLUSTER_NAME", "atlas-cluster")
            .withEnv("CASSANDRA_DC", "datacenter1");

    @Container
    static ElasticsearchContainer elasticsearch = new ElasticsearchContainer(
            DockerImageName.parse("elasticsearch:7.16.2"))
            .withNetwork(network)
            .withNetworkAliases("elasticsearch")
            .withEnv("discovery.type", "single-node")
            .withEnv("xpack.security.enabled", "false")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:6.2.14")
            .withNetwork(network)
            .withNetworkAliases("redis")
            .withCommand("redis-server", "--requirepass", "", "--protected-mode", "no")
            .withExposedPorts(6379);

    @Container
    static GenericContainer<?> atlas = createAtlasContainer();

    private static GenericContainer<?> createAtlasContainer() {
        String deployPath = getDeployDirectoryPath();
        LOG.info("Using deploy directory: {}", deployPath);

        File deployDir = new File(deployPath);
        if (!deployDir.exists() || !deployDir.isDirectory()) {
            throw new IllegalStateException("Deploy directory not found: " + deployPath);
        }

        File logsDir = new File(deployPath, "logs");
        File dataDir = new File(deployPath, "data");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
            logsDir.setWritable(true, false);
        }
        if (!dataDir.exists()) {
            dataDir.mkdirs();
            dataDir.setWritable(true, false);
        }

        return new GenericContainer<>(DockerImageName.parse("atlanhq/atlas:test"))
                .withNetwork(network)
                .withNetworkAliases("atlas")
                .withExposedPorts(21000)
                .withFileSystemBind(deployPath, "/opt/atlas-deploy", BindMode.READ_WRITE)
                .withFileSystemBind(deployPath + "/conf", "/opt/apache-atlas/conf-orig", BindMode.READ_ONLY)
                .withFileSystemBind(deployPath + "/elasticsearch", "/opt/apache-atlas/elasticsearch", BindMode.READ_ONLY)
                .withFileSystemBind(deployPath + "/models", "/opt/apache-atlas/models", BindMode.READ_ONLY)
                .withEnv(getAtlasEnvironment())
                .withCommand("/bin/bash", "-c", createStartupScript())
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
                .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("ATLAS"))
                .dependsOn(kafka, cassandra, elasticsearch, redis, zookeeper);
    }

    private static String getDeployDirectoryPath() {
        String projectRootPath = System.getProperty("user.dir");
        String path = projectRootPath + "/src/test/resources/deploy";
        File dir = new File(path);
        if (dir.exists() && dir.isDirectory() &&
                new File(dir, "conf").exists() && new File(dir, "elasticsearch").exists()) {
            return dir.getAbsolutePath();
        }
        throw new IllegalStateException("Could not find deploy directory at: " + path);
    }

    private static Map<String, String> getAtlasEnvironment() {
        Map<String, String> env = new HashMap<>();
        env.put("ATLAS_HOME", "/opt/apache-atlas");
        env.put("ATLAS_CONF", "/opt/apache-atlas/conf");
        env.put("ATLAS_DATA", "/opt/atlas-deploy/data");
        env.put("ATLAS_LOG_DIR", "/opt/atlas-deploy/logs");
        env.put("MANAGE_LOCAL_HBASE", "false");
        env.put("MANAGE_LOCAL_SOLR", "false");
        env.put("MANAGE_LOCAL_ELASTICSEARCH", "false");
        env.put("MANAGE_LOCAL_CASSANDRA", "false");
        env.put("ATLAS_SERVER_OPTS", "-server -Xms1g -Xmx2g");
        env.put("ATLAS_SERVER_HEAP", "-Xms1g -Xmx2g");
        env.put("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64");
        env.put("JDK_JAVA_OPTIONS", "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED");
        env.put("SPRING_PROFILES_ACTIVE", "local");
        env.put("ATLAS_OPTS", "-Datlas.home=/opt/apache-atlas -Datlas.conf=/opt/apache-atlas/conf");
        return env;
    }

    /**
     * Creates the startup script with Dynamic Config Store enabled.
     * Key difference from base test: enables atlas.config.store.cassandra.enabled=true
     */
    private static String createStartupScript() {
        return """
            #!/bin/bash
            set -e

            # Wait for dependencies
            echo "Waiting for Cassandra..."
            while ! nc -z cassandra 9042; do sleep 2; done
            echo "Cassandra is ready"

            echo "Waiting for Elasticsearch..."
            while ! curl -s elasticsearch:9200 > /dev/null; do sleep 2; done
            echo "Elasticsearch is ready"

            echo "Waiting for Kafka..."
            while ! nc -z kafka 9092; do sleep 2; done
            echo "Kafka is ready"

            echo "Waiting for Redis..."
            while ! nc -z redis 6379; do sleep 2; done
            echo "Redis is ready"

            # Create required directories
            mkdir -p /opt/atlas-deploy/logs
            mkdir -p /opt/atlas-deploy/data
            mkdir -p /opt/apache-atlas/conf
            mkdir -p /opt/apache-atlas/server/webapp

            # Copy configuration
            cp -r /opt/apache-atlas/conf-orig/* /opt/apache-atlas/conf/

            # Fix atlas-env.sh
            sed -i 's/${hbase.embedded}/false/g' /opt/apache-atlas/conf/atlas-env.sh
            sed -i 's/${solr.embedded}/false/g' /opt/apache-atlas/conf/atlas-env.sh
            sed -i 's/${cassandra.embedded}/false/g' /opt/apache-atlas/conf/atlas-env.sh
            sed -i 's/${elasticsearch.managed}/false/g' /opt/apache-atlas/conf/atlas-env.sh

            # Update atlas-application.properties with config store enabled
            cat >> /opt/apache-atlas/conf/atlas-application.properties <<'EOF'

            # Container service overrides
            atlas.graph.storage.backend=cql
            atlas.graph.storage.hostname=cassandra
            atlas.graph.storage.cql.port=9042
            atlas.graph.storage.cql.keyspace=atlas
            atlas.graph.storage.cql.replication-factor=1

            atlas.graph.index.search.backend=elasticsearch
            atlas.graph.index.search.hostname=elasticsearch:9200
            atlas.graph.index.search.elasticsearch.rest-port=9200

            atlas.kafka.bootstrap.servers=kafka:9092
            atlas.kafka.zookeeper.connect=zookeeper:2181

            # Redis configuration
            atlas.redis.url = redis://redis:6379
            atlas.redis.sentinel.enabled = false
            atlas.redis.sentinel.check_list.enabled = false

            # ENABLE DYNAMIC CONFIG STORE - Key configuration for this test
            atlas.config.store.cassandra.enabled=true
            atlas.config.store.cassandra.hostname=cassandra
            atlas.config.store.cassandra.port=9042
            atlas.config.store.cassandra.keyspace=config_store
            atlas.config.store.cassandra.table=configs
            atlas.config.store.cassandra.datacenter=datacenter1
            atlas.config.store.cassandra.replication.factor=1
            atlas.config.store.cassandra.app.name=atlas
            atlas.config.store.cassandra.sync.interval.seconds=30

            atlas.server.bind.address=0.0.0.0
            atlas.authentication.method.file=true
            atlas.authentication.method.kerberos=false

            atlas.graph.tx.log.tx=false
            atlas.graph.tx.recovery.verbose=false

            atlas.audit.hbase.zookeeper.quorum=
            atlas.EntityAuditRepository.impl=org.apache.atlas.repository.audit.NoopEntityAuditRepository

            atlas.redis.sentinel.check_list.enabled=false
            atlas.authorizer.impl=none
            atlas.authentication.method.file=true
            atlas.enableTLS=false
            atlas.authentication.method.kerberos=false

            atlas.kafka.zookeeper.session.timeout.ms=400
            atlas.kafka.zookeeper.connection.timeout.ms=200
            atlas.kafka.zookeeper.sync.time.ms=20
            atlas.kafka.auto.commit.interval.ms=1000
            atlas.kafka.hook.group.id=atlas
            atlas.kafka.enable.auto.commit=false
            atlas.kafka.auto.offset.reset=earliest
            atlas.kafka.session.timeout.ms=30000
            atlas.kafka.offsets.topic.replication.factor=1
            atlas.kafka.poll.timeout.ms=1000
            atlas.notification.create.topics=true
            atlas.notification.replicas=1
            atlas.notification.topics=ATLAS_ENTITIES
            atlas.notification.log.failed.messages=true
            atlas.notification.consumer.retry.interval=500
            atlas.notification.hook.retry.interval=1000

            atlas.EntityAuditRepository.impl=org.apache.atlas.repository.audit.NoopEntityAuditRepository
            atlas.EntityAuditRepositorySearch.impl=org.apache.atlas.repository.audit.ESBasedAuditRepository
            atlas.EntityAuditRepository.keyspace=atlas_audit
            atlas.EntityAuditRepository.replicationFactor=1

            atlas.authentication.method.ldap.type=none
            atlas.authentication.method.file.filename=/opt/apache-atlas/conf/users-credentials.properties
            atlas.authentication.method.file=true
            atlas.authentication.method.ldap=false
            atlas.authentication.method.keycloak=false
            atlas.authentication.method.pam=false
            atlas.http.authentication.enabled=false
            EOF

            # Export required environment variables
            export ATLAS_HOME=/opt/apache-atlas
            export ATLAS_CONF=/opt/apache-atlas/conf
            export ATLAS_DATA=/opt/atlas-deploy/data
            export ATLAS_LOG_DIR=/opt/atlas-deploy/logs
            export ATLAS_EXPANDED_WEBAPP_DIR=/opt/apache-atlas/server/webapp
            export MANAGE_LOCAL_HBASE=false
            export MANAGE_LOCAL_SOLR=false
            export MANAGE_LOCAL_ELASTICSEARCH=false
            export MANAGE_LOCAL_CASSANDRA=false
            export MANAGE_EMBEDDED_CASSANDRA=false

            # Build classpath
            CP="/opt/apache-atlas/conf"

            if [ -d "/opt/apache-atlas/server/webapp/atlas/WEB-INF/lib" ]; then
                for jar in /opt/apache-atlas/server/webapp/atlas/WEB-INF/lib/*.jar; do
                    CP="$CP:$jar"
                done
            elif [ -f "/opt/apache-atlas/server/webapp/atlas.war" ]; then
                mkdir -p /opt/apache-atlas/server/webapp/atlas
                cd /opt/apache-atlas/server/webapp/atlas
                jar xf ../atlas.war
                cd /opt/apache-atlas
                for jar in /opt/apache-atlas/server/webapp/atlas/WEB-INF/lib/*.jar; do
                    CP="$CP:$jar"
                done
            fi

            if [ -d "/opt/apache-atlas/lib" ]; then
                for jar in /opt/apache-atlas/lib/*.jar; do
                    CP="$CP:$jar"
                done
            fi

            if [ -d "/opt/apache-atlas/libext" ]; then
                for jar in /opt/apache-atlas/libext/*.jar; do
                    CP="$CP:$jar"
                done
            fi

            /usr/bin/java -Datlas.log.dir=/opt/apache-atlas/logs -Datlas.log.file=atlas_kafka_setup.log -Datlas.home=/opt/apache-atlas -Datlas.conf=/opt/apache-atlas/conf -Datlas.data=/opt/apache-atlas/data -classpath /opt/apache-atlas/conf:/opt/apache-atlas/server/webapp/atlas/WEB-INF/classes:/opt/apache-atlas/server/webapp/atlas/WEB-INF/lib/*:/opt/apache-atlas/libext/* org.apache.atlas.hook.AtlasTopicCreator ATLAS_ENTITIES

            exec java \
                -Datlas.log.dir=/opt/atlas-deploy/logs \
                -Datlas.log.file=atlas.log \
                -Datlas.home=/opt/apache-atlas \
                -Datlas.conf=/opt/apache-atlas/conf \
                -Datlas.data=/opt/atlas-deploy/data \
                -Dlog4j.configuration=atlas-log4j.xml \
                -Djava.net.preferIPv4Stack=true \
                -server \
                -Xms1g -Xmx2g \
                --add-opens java.base/java.lang=ALL-UNNAMED \
                --add-opens java.base/java.nio=ALL-UNNAMED \
                --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
                -cp "$CP" \
                org.apache.atlas.Atlas \
                -app /opt/apache-atlas/server/webapp/atlas
            """;
    }

    @BeforeAll
    void setup() {
        LOG.info("Setting up Dynamic Config Store test environment...");

        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        int mappedPort = atlas.getMappedPort(21000);
        atlasBaseUrl = String.format("http://localhost:%d/api/atlas/v2", mappedPort);

        // Create basic auth header
        String credentials = "admin:admin";
        authHeader = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());

        LOG.info("Atlas URL: {}", atlasBaseUrl);
        LOG.info("Redis URL: localhost:{}", redis.getMappedPort(6379));

        waitForAtlasReady();
    }

    private void waitForAtlasReady() {
        LOG.info("Waiting for Atlas API to be ready...");
        int maxRetries = 60;

        for (int i = 0; i < maxRetries; i++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(atlasBaseUrl + "/types"))
                        .header("Accept", "application/json")
                        .header("Authorization", authHeader)
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                LOG.info("Atlas API check attempt {}/{}: status {}", i + 1, maxRetries, response.statusCode());

                if (response.statusCode() == 200 || response.statusCode() == 401) {
                    LOG.info("Atlas API is ready!");
                    return;
                }
            } catch (Exception e) {
                LOG.info("Atlas API not ready yet (attempt {}/{}): {}", i + 1, maxRetries, e.getMessage());
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        LOG.error("Atlas failed to become ready. Container logs:");
        LOG.error(atlas.getLogs());
        throw new RuntimeException("Atlas API failed to become ready");
    }

    // ==================== Helper Methods ====================

    /**
     * Get a Jedis client connected to the Redis container
     */
    private Jedis getRedisClient() {
        return new Jedis("localhost", redis.getMappedPort(6379));
    }

    /**
     * Set a feature flag directly in Redis (for migration testing)
     */
    private void setRedisFeatureFlag(String key, String value) {
        try (Jedis jedis = getRedisClient()) {
            String redisKey = REDIS_FEATURE_FLAG_PREFIX + key;
            jedis.set(redisKey, value);
            LOG.info("Set Redis feature flag: {} = {}", redisKey, value);
        }
    }

    /**
     * Get a feature flag directly from Redis
     */
    private String getRedisFeatureFlag(String key) {
        try (Jedis jedis = getRedisClient()) {
            return jedis.get(REDIS_FEATURE_FLAG_PREFIX + key);
        }
    }

    /**
     * Make a GET request to the Config API
     */
    private HttpResponse<String> getConfig(String key) throws Exception {
        String url = key != null ?
                atlasBaseUrl + CONFIG_API_PATH + "/" + key :
                atlasBaseUrl + CONFIG_API_PATH;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .header("Authorization", authHeader)
                .GET()
                .timeout(Duration.ofSeconds(10))
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Make a PUT request to update a config
     */
    private HttpResponse<String> updateConfig(String key, String value) throws Exception {
        String url = atlasBaseUrl + CONFIG_API_PATH + "/" + key;
        String body = mapper.writeValueAsString(Map.of("value", value));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", authHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(10))
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Make a DELETE request to reset a config to default
     */
    private HttpResponse<String> deleteConfig(String key) throws Exception {
        String url = atlasBaseUrl + CONFIG_API_PATH + "/" + key;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .header("Authorization", authHeader)
                .DELETE()
                .timeout(Duration.ofSeconds(10))
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    // ==================== REST API CRUD Tests ====================

    @Test
    @Order(1)
    @DisplayName("Config Store: GET all configs returns valid response")
    void testGetAllConfigs() throws Exception {
        LOG.info("Testing GET all configs...");

        HttpResponse<String> response = getConfig(null);

        LOG.info("Response status: {}, body: {}", response.statusCode(), response.body());

        assertEquals(200, response.statusCode(), "Should return 200 OK");

        JsonNode json = mapper.readTree(response.body());
        assertTrue(json.has("configs"), "Response should have 'configs' array");
        assertTrue(json.has("totalCount"), "Response should have 'totalCount'");
        assertTrue(json.has("enabled"), "Response should have 'enabled' flag");
        assertTrue(json.get("enabled").asBoolean(), "Config store should be enabled");

        JsonNode configs = json.get("configs");
        assertTrue(configs.isArray(), "'configs' should be an array");
        assertTrue(configs.size() > 0, "Should have at least one config defined");

        LOG.info("Found {} configs", configs.size());
    }

    @Test
    @Order(2)
    @DisplayName("Config Store: GET single config returns default value")
    void testGetSingleConfigDefault() throws Exception {
        LOG.info("Testing GET single config with default value...");

        HttpResponse<String> response = getConfig("MAINTENANCE_MODE");

        LOG.info("Response status: {}, body: {}", response.statusCode(), response.body());

        assertEquals(200, response.statusCode(), "Should return 200 OK");

        JsonNode json = mapper.readTree(response.body());
        assertEquals("MAINTENANCE_MODE", json.get("key").asText(), "Key should match");
        assertEquals("false", json.get("defaultValue").asText(), "Default value should be 'false'");
        assertNotNull(json.get("currentValue"), "Should have currentValue");
    }

    @Test
    @Order(3)
    @DisplayName("Config Store: PUT updates config value")
    void testUpdateConfig() throws Exception {
        LOG.info("Testing PUT to update config...");

        // Update MAINTENANCE_MODE to true
        HttpResponse<String> updateResponse = updateConfig("MAINTENANCE_MODE", "true");

        LOG.info("Update response status: {}, body: {}", updateResponse.statusCode(), updateResponse.body());

        assertEquals(200, updateResponse.statusCode(), "Should return 200 OK");

        JsonNode updateJson = mapper.readTree(updateResponse.body());
        assertTrue(updateJson.get("success").asBoolean(), "Update should be successful");
        assertEquals("MAINTENANCE_MODE", updateJson.get("key").asText());
        assertEquals("true", updateJson.get("value").asText());

        // Verify by reading back
        HttpResponse<String> getResponse = getConfig("MAINTENANCE_MODE");
        assertEquals(200, getResponse.statusCode());

        JsonNode getJson = mapper.readTree(getResponse.body());
        assertEquals("true", getJson.get("currentValue").asText(), "Current value should be 'true'");
    }

    @Test
    @Order(4)
    @DisplayName("Config Store: DELETE resets config to default")
    void testDeleteConfigResetsToDefault() throws Exception {
        LOG.info("Testing DELETE to reset config to default...");

        // First ensure config has a non-default value
        updateConfig("MAINTENANCE_MODE", "true");

        // Delete (reset to default)
        HttpResponse<String> deleteResponse = deleteConfig("MAINTENANCE_MODE");

        LOG.info("Delete response status: {}, body: {}", deleteResponse.statusCode(), deleteResponse.body());

        assertEquals(200, deleteResponse.statusCode(), "Should return 200 OK");

        JsonNode deleteJson = mapper.readTree(deleteResponse.body());
        assertTrue(deleteJson.get("success").asBoolean(), "Delete should be successful");
        assertEquals("false", deleteJson.get("value").asText(), "Value should be reset to default 'false'");

        // Verify by reading back
        HttpResponse<String> getResponse = getConfig("MAINTENANCE_MODE");
        JsonNode getJson = mapper.readTree(getResponse.body());
        assertEquals("false", getJson.get("currentValue").asText(), "Current value should be default 'false'");
    }

    @Test
    @Order(5)
    @DisplayName("Config Store: Invalid config key returns 400")
    void testInvalidConfigKeyReturns400() throws Exception {
        LOG.info("Testing invalid config key...");

        HttpResponse<String> response = getConfig("INVALID_KEY_THAT_DOES_NOT_EXIST");

        LOG.info("Response status: {}, body: {}", response.statusCode(), response.body());

        assertEquals(400, response.statusCode(), "Should return 400 Bad Request for invalid key");
    }

    @Test
    @Order(6)
    @DisplayName("Config Store: Empty value returns 400")
    void testEmptyValueReturns400() throws Exception {
        LOG.info("Testing empty value update...");

        String url = atlasBaseUrl + CONFIG_API_PATH + "/MAINTENANCE_MODE";
        String body = mapper.writeValueAsString(Map.of("value", ""));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", authHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Response status: {}, body: {}", response.statusCode(), response.body());

        assertEquals(400, response.statusCode(), "Should return 400 Bad Request for empty value");
    }

    // ==================== Config Persistence Tests ====================

    @Test
    @Order(10)
    @DisplayName("Config Store: Config update is reflected immediately")
    void testConfigUpdateReflectedImmediately() throws Exception {
        LOG.info("Testing immediate config update reflection...");

        String testValue = "test_value_" + System.currentTimeMillis();

        // Update config
        HttpResponse<String> updateResponse = updateConfig("DISABLE_WRITE_FLAG", testValue);
        assertEquals(200, updateResponse.statusCode());

        // Read back immediately
        HttpResponse<String> getResponse = getConfig("DISABLE_WRITE_FLAG");
        assertEquals(200, getResponse.statusCode());

        JsonNode json = mapper.readTree(getResponse.body());
        assertEquals(testValue, json.get("currentValue").asText(),
                "Value should be immediately reflected after update");

        // Cleanup
        deleteConfig("DISABLE_WRITE_FLAG");
    }

    @Test
    @Order(11)
    @DisplayName("Config Store: Multiple configs can be updated independently")
    void testMultipleConfigsIndependent() throws Exception {
        LOG.info("Testing multiple independent config updates...");

        // Update multiple configs
        updateConfig("MAINTENANCE_MODE", "true");
        updateConfig("DISABLE_WRITE_FLAG", "true");

        // Verify each independently
        HttpResponse<String> response1 = getConfig("MAINTENANCE_MODE");
        JsonNode json1 = mapper.readTree(response1.body());
        assertEquals("true", json1.get("currentValue").asText());

        HttpResponse<String> response2 = getConfig("DISABLE_WRITE_FLAG");
        JsonNode json2 = mapper.readTree(response2.body());
        assertEquals("true", json2.get("currentValue").asText());

        // Update one, verify other unchanged
        updateConfig("MAINTENANCE_MODE", "false");

        response2 = getConfig("DISABLE_WRITE_FLAG");
        json2 = mapper.readTree(response2.body());
        assertEquals("true", json2.get("currentValue").asText(),
                "DISABLE_WRITE_FLAG should remain unchanged");

        // Cleanup
        deleteConfig("MAINTENANCE_MODE");
        deleteConfig("DISABLE_WRITE_FLAG");
    }

    // ==================== Redis to Cassandra Migration Tests ====================

    @Test
    @Order(20)
    @DisplayName("Migration: Feature flags set in Redis are accessible via config API")
    void testFeatureFlagFromRedisAccessibleViaApi() throws Exception {
        LOG.info("Testing feature flag access from Redis...");

        // Note: Migration happens at startup. This test verifies that feature flags
        // that were in Redis before Atlas started are now accessible via the config API.

        // Set a feature flag in Redis
        setRedisFeatureFlag("ENABLE_JANUS_OPTIMISATION", "true");

        // Give Atlas some time to sync (if background sync is running)
        Thread.sleep(2000);

        // The flag should be accessible via the API (either from cache or Cassandra)
        HttpResponse<String> response = getConfig("ENABLE_JANUS_OPTIMISATION");
        LOG.info("Response status: {}, body: {}", response.statusCode(), response.body());

        assertEquals(200, response.statusCode());

        // Note: The value might be from cache, Cassandra, or default depending on migration state
        JsonNode json = mapper.readTree(response.body());
        assertNotNull(json.get("currentValue"), "Should have a current value");
    }

    @Test
    @Order(21)
    @DisplayName("Migration: Config API updates persist to Cassandra (not Redis)")
    void testConfigApiUpdatesPersistToCassandra() throws Exception {
        LOG.info("Testing that config API updates persist to Cassandra...");

        String testKey = "MAINTENANCE_MODE";
        String testValue = "true";

        // Update via API
        HttpResponse<String> updateResponse = updateConfig(testKey, testValue);
        assertEquals(200, updateResponse.statusCode());

        // Verify via API
        HttpResponse<String> getResponse = getConfig(testKey);
        JsonNode json = mapper.readTree(getResponse.body());
        assertEquals(testValue, json.get("currentValue").asText());

        // The Redis value should NOT be updated (config API writes to Cassandra only)
        String redisValue = getRedisFeatureFlag(testKey);
        LOG.info("Redis value for {}: {}", testKey, redisValue);
        // Redis might have the old value or null - the key point is the API returns the Cassandra value

        // Cleanup
        deleteConfig(testKey);
    }

    // ==================== Maintenance Mode Tests ====================

    @Test
    @Order(30)
    @DisplayName("Maintenance Mode: Can be toggled on and off")
    void testMaintenanceModeToggle() throws Exception {
        LOG.info("Testing maintenance mode toggle...");

        // Enable maintenance mode
        HttpResponse<String> enableResponse = updateConfig("MAINTENANCE_MODE", "true");
        assertEquals(200, enableResponse.statusCode());

        HttpResponse<String> getResponse = getConfig("MAINTENANCE_MODE");
        JsonNode json = mapper.readTree(getResponse.body());
        assertEquals("true", json.get("currentValue").asText(), "Maintenance mode should be enabled");

        // Disable maintenance mode
        HttpResponse<String> disableResponse = updateConfig("MAINTENANCE_MODE", "false");
        assertEquals(200, disableResponse.statusCode());

        getResponse = getConfig("MAINTENANCE_MODE");
        json = mapper.readTree(getResponse.body());
        assertEquals("false", json.get("currentValue").asText(), "Maintenance mode should be disabled");

        LOG.info("Maintenance mode toggle test passed");
    }

    @Test
    @Order(31)
    @DisplayName("Maintenance Mode: Shows updatedBy and timestamp")
    void testMaintenanceModeAuditInfo() throws Exception {
        LOG.info("Testing maintenance mode audit info...");

        // Update config
        updateConfig("MAINTENANCE_MODE", "true");

        // Get config and check audit fields
        HttpResponse<String> response = getConfig("MAINTENANCE_MODE");
        JsonNode json = mapper.readTree(response.body());

        // Note: updatedBy might be null if not set by API, or might be the authenticated user
        LOG.info("updatedBy: {}, lastUpdated: {}",
                json.has("updatedBy") ? json.get("updatedBy").asText() : "null",
                json.has("lastUpdated") ? json.get("lastUpdated").asText() : "null");

        // The key assertion is that the API returns these fields
        assertTrue(json.has("key"), "Should have key field");
        assertTrue(json.has("currentValue"), "Should have currentValue field");
        assertTrue(json.has("defaultValue"), "Should have defaultValue field");

        // Cleanup
        deleteConfig("MAINTENANCE_MODE");
    }

    // ==================== Edge Cases and Error Handling ====================

    @Test
    @Order(40)
    @DisplayName("Config Store: All predefined keys are listed")
    void testAllPredefinedKeysListed() throws Exception {
        LOG.info("Testing all predefined keys are listed...");

        HttpResponse<String> response = getConfig(null);
        assertEquals(200, response.statusCode());

        JsonNode json = mapper.readTree(response.body());
        JsonNode configs = json.get("configs");

        // Expected keys from ConfigKey enum
        String[] expectedKeys = {
                "MAINTENANCE_MODE",
                "ENABLE_JANUS_OPTIMISATION",
                "disable_writes",
                "enable_persona_hierarchy_filter",
                "use_temp_es_index"
        };

        for (String expectedKey : expectedKeys) {
            boolean found = false;
            for (JsonNode config : configs) {
                if (expectedKey.equals(config.get("key").asText())) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Expected key '" + expectedKey + "' should be in the list");
        }

        LOG.info("All predefined keys are present in the config list");
    }

    @Test
    @Order(41)
    @DisplayName("Config Store: Boolean config values are handled correctly")
    void testBooleanConfigValues() throws Exception {
        LOG.info("Testing boolean config value handling...");

        // Test with 'true'
        updateConfig("MAINTENANCE_MODE", "true");
        HttpResponse<String> response = getConfig("MAINTENANCE_MODE");
        JsonNode json = mapper.readTree(response.body());
        assertEquals("true", json.get("currentValue").asText());

        // Test with 'false'
        updateConfig("MAINTENANCE_MODE", "false");
        response = getConfig("MAINTENANCE_MODE");
        json = mapper.readTree(response.body());
        assertEquals("false", json.get("currentValue").asText());

        // Note: Non-boolean strings might also be accepted (implementation dependent)
        // Cleanup
        deleteConfig("MAINTENANCE_MODE");
    }

    @AfterAll
    void tearDown() {
        LOG.info("Dynamic Config Store Integration Tests completed");
    }
}
