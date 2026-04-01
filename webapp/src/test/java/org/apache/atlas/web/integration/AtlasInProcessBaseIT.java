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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

/**
 * Base class for in-process Atlas integration tests.
 *
 * Starts infrastructure services (Cassandra, ES, Redis) via testcontainers (singleton),
 * then starts Atlas in-process using Jetty (no Docker image build needed).
 * Tests use {@link AtlasClientV2} with basic auth to call Atlas REST APIs.
 *
 * <p>Subclasses get a ready-to-use {@link #atlasClient} after {@code @BeforeAll}.</p>
 *
 * <p>Prerequisites: {@code mvn compile -pl webapp -am -DskipTests -Drat.skip=true}</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasInProcessBaseIT.class);

    private static final int MAX_STARTUP_WAIT_SECONDS = 100; // 5 minutes

    // Singleton containers (shared across all test classes, stopped by JVM shutdown hook)
    private static final CassandraContainer<?> cassandra;
    private static final ElasticsearchContainer elasticsearch;
    private static final GenericContainer<?> redis;
    private static final KafkaContainer kafka;
    private static final GenericContainer<?> zookeeper;

    private static volatile boolean containersStarted = false;

    static {
        // Must be set before any testcontainers Docker client initialization (Docker 29+ requires API >= 1.44)
        System.setProperty("api.version", "1.44");

        cassandra = new CassandraContainer<>(DockerImageName.parse("cassandra:3.11"))
                .withStartupTimeout(Duration.ofMinutes(3))
                .withEnv("CASSANDRA_CLUSTER_NAME", "atlas-test-cluster")
                .withEnv("CASSANDRA_DC", "datacenter1")
                // TypeDef vertices (e.g. Asset) can have 30-60KB properties JSON.
                // Raise batch_size_fail_threshold from default 50KB to 200KB to match
                // typical production Cassandra configuration.
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                        "/bin/bash", "-c",
                        "sed -i 's/batch_size_fail_threshold_in_kb:.*/batch_size_fail_threshold_in_kb: 200/' /etc/cassandra/cassandra.yaml && " +
                        "exec /docker-entrypoint.sh cassandra -f"));

        elasticsearch = new ElasticsearchContainer(
                DockerImageName.parse("elasticsearch:7.17.27"))
                .withEnv("discovery.type", "single-node")
                .withEnv("xpack.security.enabled", "false")
                .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

        redis = new GenericContainer<>(DockerImageName.parse("redis:6.2.14"))
                .withExposedPorts(6379)
                .withCommand("redis-server", "--requirepass", "", "--protected-mode", "no")
                .waitingFor(Wait.forListeningPort());

        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                .withStartupTimeout(Duration.ofMinutes(2));

        zookeeper = new GenericContainer<>(DockerImageName.parse("zookeeper:3.8"))
                .withExposedPorts(2181)
                .waitingFor(Wait.forListeningPort())
                .withStartupTimeout(Duration.ofMinutes(1));
    }

    protected static AtlasClientV2 atlasClient;
    private static InProcessAtlasServer atlasServer;
    private static int atlasPort;
    private static Path tempDir;
    private static volatile boolean serverStarted = false;

    /**
     * Returns true if the system property {@code atlas.graphdb.backend} is set to {@code cassandra}.
     * Maven forwards this via {@code -Datlas.graphdb.backend=cassandra} and surefire's systemProperties.
     */
    protected static boolean isCassandraGraphBackend() {
        return "cassandra".equalsIgnoreCase(System.getProperty("atlas.graphdb.backend"));
    }

    @BeforeAll
    void startAtlas() throws Exception {
        startContainers();
        startServerOnce();
    }

    /**
     * Starts the Atlas server exactly once across all test classes.
     * Subsequent calls are no-ops. The server is stopped via a JVM shutdown hook.
     */
    private static synchronized void startServerOnce() throws Exception {
        if (serverStarted) {
            LOG.info("Atlas server already running on port {}", atlasPort);
            return;
        }

        setupConfiguration();
        initElasticsearchTemplate();
        startServer();
        waitForAtlasReady();
        createClient();
        verifyBootstrapTypeDefs();

        // Register shutdown hook so the server is stopped when the JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook: stopping Atlas server");
            try {
                if (atlasServer != null && atlasServer.isRunning()) {
                    atlasServer.stop();
                }
            } catch (Exception e) {
                LOG.warn("Error stopping Atlas server in shutdown hook", e);
            }
        }));

        serverStarted = true;
    }

    private static synchronized void startContainers() {
        if (containersStarted) {
            LOG.info("Containers already running");
            return;
        }

        LOG.info("Starting testcontainers (Cassandra, Elasticsearch, Redis)...");
        cassandra.start();
        LOG.info("Cassandra started on port {}", cassandra.getMappedPort(9042));

        elasticsearch.start();
        LOG.info("Elasticsearch started on {}", elasticsearch.getHttpHostAddress());

        redis.start();
        LOG.info("Redis started on port {}", redis.getMappedPort(6379));

        kafka.start();
        LOG.info("Kafka started on {}", kafka.getBootstrapServers());

        zookeeper.start();
        LOG.info("ZooKeeper started on port {}", zookeeper.getMappedPort(2181));

        containersStarted = true;
    }

    private static void initElasticsearchTemplate() throws IOException {
        String esAddress = elasticsearch.getHttpHostAddress();
        String deployDir = System.getProperty("atlas.home");

        // Read ES settings and mappings from deploy directory
        Path settingsPath = Path.of(deployDir, "elasticsearch", "es-settings.json");
        Path mappingsPath = Path.of(deployDir, "elasticsearch", "es-mappings.json");
        String settings = Files.readString(settingsPath, StandardCharsets.UTF_8);
        String mappings = Files.readString(mappingsPath, StandardCharsets.UTF_8);

        // Build the index template body
        String templateBody = String.format(
                "{\"index_patterns\":[\"janusgraph_vertex_index\"],\"settings\":%s,\"mappings\":%s}",
                settings, mappings);

        // PUT the template via ES REST API
        URL url = new URL("http://" + esAddress + "/_template/atlan-template");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(templateBody.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        if (responseCode == 200) {
            LOG.info("ES index template 'atlan-template' created successfully");
        } else {
            InputStream errorStream = conn.getErrorStream();
            String error = errorStream != null
                    ? new String(errorStream.readAllBytes(), StandardCharsets.UTF_8)
                    : "(no error body)";
            throw new IOException("Failed to create ES index template (HTTP " + responseCode + "): " + error);
        }
        conn.disconnect();

        // Create a second template for CassandraGraph's atlas_graph_* index pattern.
        // CassandraGraph maps each entity property directly to an ES field (unlike JanusGraph
        // which encodes properties into a small number of fields). With 952+ entity types
        // from minimal.json, field mappings easily exceed ES's default 2000 limit.
        if (isCassandraGraphBackend()) {
            // Raise the field limit from 2000 to 10000 for CassandraGraph
            String cassSettings = settings.replace("\"limit\" : \"2000\"", "\"limit\" : \"10000\"");
            String cassandraTemplateBody = String.format(
                    "{\"index_patterns\":[\"atlas_graph_*\"],\"settings\":%s,\"mappings\":%s}",
                    cassSettings, mappings);

            URL cassUrl = new URL("http://" + esAddress + "/_template/atlas-graph-template");
            HttpURLConnection cassConn = (HttpURLConnection) cassUrl.openConnection();
            cassConn.setRequestMethod("PUT");
            cassConn.setRequestProperty("Content-Type", "application/json");
            cassConn.setDoOutput(true);

            try (OutputStream cassOs = cassConn.getOutputStream()) {
                cassOs.write(cassandraTemplateBody.getBytes(StandardCharsets.UTF_8));
            }

            int cassResponseCode = cassConn.getResponseCode();
            if (cassResponseCode == 200) {
                LOG.info("ES index template 'atlas-graph-template' created successfully for CassandraGraph");
            } else {
                InputStream cassErrorStream = cassConn.getErrorStream();
                String cassError = cassErrorStream != null
                        ? new String(cassErrorStream.readAllBytes(), StandardCharsets.UTF_8)
                        : "(no error body)";
                throw new IOException("Failed to create ES index template 'atlas-graph-template' (HTTP " + cassResponseCode + "): " + cassError);
            }
            cassConn.disconnect();

            // Pre-create the atlas_graph_vertex_index so CassandraGraphManagement can add
            // field mappings (__timestamp, __modificationTimestamp, __typeName, etc.) at startup.
            // Without this, the index doesn't exist until the first _bulk write, and all field
            // mapping PUTs fail with 404.
            // The template (atlas-graph-template) applies settings+mappings automatically.
            URL createIndexUrl = new URL("http://" + esAddress + "/atlas_graph_vertex_index");
            HttpURLConnection createConn = (HttpURLConnection) createIndexUrl.openConnection();
            createConn.setRequestMethod("PUT");
            createConn.setRequestProperty("Content-Type", "application/json");
            createConn.setDoOutput(true);
            try (OutputStream createOs = createConn.getOutputStream()) {
                createOs.write("{}".getBytes(StandardCharsets.UTF_8));
            }
            int createCode = createConn.getResponseCode();
            LOG.info("Pre-created atlas_graph_vertex_index: HTTP {}", createCode);
            createConn.disconnect();

        }
    }

    private static void setupConfiguration() throws Exception {
        tempDir = Files.createTempDirectory("atlas-test-");
        Path confDir = tempDir.resolve("conf");
        Path dataDir = tempDir.resolve("data");
        Path logDir = tempDir.resolve("logs");

        Files.createDirectories(confDir);
        Files.createDirectories(dataDir);
        Files.createDirectories(logDir);

        // Resolve paths
        String deployDir = resolveDeployDir();
        atlasPort = findFreePort();

        // Copy users-credentials.properties to temp conf dir
        Path srcCredentials = Path.of(deployDir, "conf", "users-credentials.properties");
        Path dstCredentials = confDir.resolve("users-credentials.properties");
        Files.copy(srcCredentials, dstCredentials, StandardCopyOption.REPLACE_EXISTING);

        // Create dummy keycloak.json (required by AtlasKeycloakAuthenticationProvider even when disabled)
        Files.writeString(confDir.resolve("keycloak.json"),
                "{\"realm\":\"test\",\"auth-server-url\":\"http://localhost:8080\",\"resource\":\"test\",\"credentials\":{\"secret\":\"test\"}}",
                StandardCharsets.UTF_8);

        // Generate atlas-application.properties
        generateAtlasProperties(confDir, deployDir, atlasPort);

        // Set system properties
        System.setProperty("atlas.home", deployDir);
        System.setProperty("atlas.conf", confDir.toAbsolutePath().toString());
        System.setProperty("atlas.data", dataDir.toAbsolutePath().toString());
        System.setProperty("atlas.log.dir", logDir.toAbsolutePath().toString());
        System.setProperty("spring.profiles.active", "local");

        // Force reload so ApplicationProperties picks up the new config
        ApplicationProperties.forceReload();

        LOG.info("Configuration written to {}", confDir);
        LOG.info("atlas.home={}", deployDir);
        LOG.info("atlas.conf={}", confDir);
        LOG.info("atlas.data={}", dataDir);
        LOG.info("Atlas port={}", atlasPort);
    }

    private static void generateAtlasProperties(Path confDir, String deployDir, int port) throws IOException {
        File propsFile = confDir.resolve("atlas-application.properties").toFile();

        int cassandraPort = cassandra.getMappedPort(9042);
        String esAddress = elasticsearch.getHttpHostAddress(); // host:port

        try (PrintWriter w = new PrintWriter(new FileWriter(propsFile))) {
            // Graph storage - Cassandra
            w.println("atlas.graph.storage.backend=cql");
            w.println("atlas.graph.storage.hostname=localhost");
            w.println("atlas.graph.storage.cql.port=" + cassandraPort);
            w.println("atlas.graph.storage.cql.keyspace=atlas_test");
            w.println("atlas.graph.storage.cql.replication-factor=1");
            w.println("atlas.graph.storage.clustername=atlas-test-cluster");
            w.println("atlas.graph.storage.port=" + cassandraPort);
            w.println("atlas.graph.query.fast-property=true");
            w.println("atlas.graph.query.batch=true");
            w.println("query.batch.properties-mode=all-properties");
            w.println("atlas.use.index.query.to.find.entity.by.unique.attributes=true");
            w.println("atlas.graph.storage.lock.retries=5");
            w.println("atlas.graph.cache.db-cache=false");
            w.println("atlas.graph.storage.write-time=10000");

            // Graph index - Elasticsearch
            w.println("atlas.graph.index.search.backend=elasticsearch");
            w.println("atlas.graph.index.search.hostname=" + esAddress);
            w.println("atlas.graph.index.search.elasticsearch.client-only=true");
            w.println("atlas.graph.index.search.elasticsearch.retry_on_conflict=5");
            w.println("atlas.rebuild.index=true");

            // Cassandra graph backend (only active when atlas.graphdb.backend=cassandra)
            if (isCassandraGraphBackend()) {
                w.println("atlas.graphdb.backend=cassandra");
                w.println("atlas.cassandra.graph.hostname=localhost");
                w.println("atlas.cassandra.graph.port=" + cassandraPort);
                w.println("atlas.cassandra.graph.keyspace=atlas_graph");
                w.println("atlas.cassandra.graph.datacenter=datacenter1");
                w.println("atlas.graph.id.strategy=deterministic");
                w.println("atlas.graph.claim.enabled=false");
                w.println("atlas.graph.index.search.es.prefix=atlas_graph_");
            }

            // Notification - External Kafka container
            String kafkaBootstrap = kafka.getBootstrapServers();
            String zkConnect = "localhost:" + zookeeper.getMappedPort(2181);
            w.println("atlas.notification.embedded=false");
            w.println("atlas.kafka.bootstrap.servers=" + kafkaBootstrap);
            w.println("atlas.graph.kafka.bootstrap.servers=" + kafkaBootstrap);
            w.println("atlas.kafka.zookeeper.connect=" + zkConnect);
            w.println("atlas.kafka.auto.commit.interval.ms=1000");
            w.println("atlas.kafka.hook.group.id=atlas");
            w.println("atlas.kafka.enable.auto.commit=false");
            w.println("atlas.kafka.auto.offset.reset=earliest");
            w.println("atlas.kafka.session.timeout.ms=30000");
            w.println("atlas.kafka.offsets.topic.replication.factor=1");
            w.println("atlas.kafka.poll.timeout.ms=1000");
            w.println("atlas.notification.create.topics=true");
            w.println("atlas.notification.replicas=1");
            w.println("atlas.notification.topics=ATLAS_HOOK,ATLAS_ENTITIES");
            w.println("atlas.notification.log.failed.messages=true");
            w.println("atlas.notification.consumer.retry.interval=500");
            w.println("atlas.notification.hook.retry.interval=1000");

            // Redis
            w.println("atlas.redis.url=redis://localhost:" + redis.getMappedPort(6379));
            w.println("atlas.redis.sentinel.enabled=false");
            w.println("atlas.redis.sentinel.check_list.enabled=false");

            // Authentication - file-based
            w.println("atlas.authentication.method.file=true");
            w.println("atlas.authentication.method.file.filename=" +
                    confDir.resolve("users-credentials.properties").toAbsolutePath());
            w.println("atlas.authentication.method.kerberos=false");
            w.println("atlas.authentication.method.ldap=false");
            w.println("atlas.authentication.method.ldap.type=none");
            w.println("atlas.authentication.method.keycloak=false");
            w.println("atlas.authentication.method.pam=false");
            w.println("atlas.http.authentication.enabled=false");

            // Security
            w.println("atlas.enableTLS=false");
            w.println("atlas.rest-csrf.enabled=false");
            w.println("atlas.authorizer.impl=none");

            // Server
            w.println("atlas.server.http.port=" + port);
            w.println("atlas.server.bind.address=localhost");
            w.println("atlas.rest.address=http://localhost:" + port);
            w.println("atlas.server.ha.enabled=false");

            // Delete handler
            w.println("atlas.DeleteHandlerV1.impl=org.apache.atlas.repository.store.graph.v1.SoftDeleteHandlerV1");

            // Entity audit - Noop (no HBase) + ES-based search audit
            w.println("atlas.EntityAuditRepository.impl=org.apache.atlas.repository.audit.NoopEntityAuditRepository");
            w.println("atlas.EntityAuditRepositorySearch.impl=org.apache.atlas.repository.audit.ESBasedAuditRepository");

            // Search
            w.println("atlas.search.fulltext.enable=true");
            w.println("atlas.search.gremlin.enable=false");

            // Lineage
            w.println("atlas.lineage.on.demand.enabled=true");

            // Misc
            w.println("atlas.entity.audit.differential=true");
            w.println("atlas.index.audit.elasticsearch.total_field_limit=2000");
            w.println("atlas.entity.skip.optional.attributes=true");

            // DLQ
            w.println("atlas.kafka.dlq.enabled=false");

            // Index recovery - disable to avoid shutdown hang (monitor thread has while(true) without exit)
            w.println("atlas.index.recovery.enable=false");

            // Config store - DynamicConfigStore for Tags V2
            // Port falls back to atlas.graph.storage.cql.port (the mapped testcontainer port)
            w.println("atlas.config.store.cassandra.enabled=true");
            w.println("atlas.config.store.cassandra.activated=true");
            w.println("atlas.config.store.cassandra.consistency.level=LOCAL_ONE");
            w.println("atlas.config.store.cassandra.replication.factor=1");
        }
    }

    private static void startServer() throws Exception {
        String webappPath = resolveWebappPath();
        LOG.info("Starting Atlas in-process (webapp={}, port={})", webappPath, atlasPort);

        atlasServer = new InProcessAtlasServer(atlasPort, webappPath);
        atlasServer.start();
    }

    private static void waitForAtlasReady() {
        LOG.info("Waiting for Atlas to become ready (max {}s)...", MAX_STARTUP_WAIT_SECONDS);
        long deadline = System.currentTimeMillis() + MAX_STARTUP_WAIT_SECONDS * 1000L;

        while (System.currentTimeMillis() < deadline) {
            HttpURLConnection conn = null;
            try {
                conn = (HttpURLConnection)
                        new URL("http://localhost:" + atlasPort + "/api/atlas/admin/status")
                                .openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);

                int status = conn.getResponseCode();
                if (status == 200) {
                    LOG.info("Atlas is ready (HTTP {})", status);
                    return;
                }
                LOG.info("Atlas not ready yet (HTTP {}), retrying...", status);
            } catch (Exception e) {
                LOG.debug("Atlas not ready yet: {}", e.getMessage());
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Atlas", e);
            }
        }

        throw new RuntimeException("Atlas did not become ready within " + MAX_STARTUP_WAIT_SECONDS + " seconds");
    }

    private static void createClient() {
        atlasClient = new AtlasClientV2(
                new String[]{"http://localhost:" + atlasPort},
                new String[]{"admin", "admin"}
        );
        LOG.info("AtlasClientV2 created (http://localhost:{})", atlasPort);
    }

    /**
     * Diagnostic: verify that TypeDefs were loaded during bootstrap.
     * Logs counts of each TypeDef category so GHA logs show whether
     * the bootstrap succeeded or silently failed.
     */
    private static void verifyBootstrapTypeDefs() {
        try {
            AtlasTypesDef allTypes = atlasClient.getAllTypeDefs(new org.apache.atlas.model.SearchFilter());
            int entityCount  = allTypes.getEntityDefs()         != null ? allTypes.getEntityDefs().size()         : 0;
            int enumCount    = allTypes.getEnumDefs()           != null ? allTypes.getEnumDefs().size()           : 0;
            int structCount  = allTypes.getStructDefs()         != null ? allTypes.getStructDefs().size()         : 0;
            int classCount   = allTypes.getClassificationDefs() != null ? allTypes.getClassificationDefs().size() : 0;
            int relCount     = allTypes.getRelationshipDefs()   != null ? allTypes.getRelationshipDefs().size()   : 0;

            System.out.println("=== TypeDef Bootstrap Verification ===");
            System.out.println("  Entity defs:         " + entityCount);
            System.out.println("  Enum defs:           " + enumCount);
            System.out.println("  Struct defs:         " + structCount);
            System.out.println("  Classification defs: " + classCount);
            System.out.println("  Relationship defs:   " + relCount);
            System.out.println("  Backend:             " + (isCassandraGraphBackend() ? "cassandra" : "janus"));
            System.out.println("======================================");

            if (entityCount == 0) {
                System.out.println("WARNING: Zero entity defs after bootstrap! TypeDef initialization likely failed.");
            }
        } catch (Exception e) {
            System.out.println("Failed to verify bootstrap TypeDefs: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String resolveDeployDir() {
        // webapp/src/test/resources/deploy (when running from webapp module)
        String[] candidates = {
                System.getProperty("user.dir") + "/src/test/resources/deploy",
                System.getProperty("user.dir") + "/webapp/src/test/resources/deploy",
        };

        for (String candidate : candidates) {
            File dir = new File(candidate);
            if (dir.exists() && new File(dir, "models").exists()) {
                return dir.getAbsolutePath();
            }
        }

        throw new IllegalStateException(
                "Could not find deploy directory with models/. Checked: " + String.join(", ", candidates));
    }

    private static String resolveWebappPath() {
        String[] candidates = {
                System.getProperty("user.dir") + "/src/main/webapp",
                System.getProperty("user.dir") + "/webapp/src/main/webapp",
        };

        for (String candidate : candidates) {
            File dir = new File(candidate);
            if (dir.exists() && new File(dir, "WEB-INF/web.xml").exists()) {
                return dir.getAbsolutePath();
            }
        }

        throw new IllegalStateException(
                "Could not find webapp directory with WEB-INF/web.xml. Checked: " + String.join(", ", candidates));
    }

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Could not find a free port", e);
        }
    }

    protected int getAtlasPort() {
        return atlasPort;
    }

    protected String getAtlasBaseUrl() {
        return "http://localhost:" + atlasPort;
    }

    protected String getElasticsearchAddress() {
        return elasticsearch.getHttpHostAddress();
    }
}
