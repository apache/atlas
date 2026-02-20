package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.TestcontainersExtension;

import java.time.Duration;

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
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration test for Apache Atlas using Docker containers with local deploy directory.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(TestcontainersExtension.class)
public class AtlasDockerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasDockerIntegrationTest.class);
    private static final Network network = Network.newNetwork();

    static final ObjectMapper mapper = new ObjectMapper();

    HttpClient httpClient;
    public static String ATLAS_BASE_URL;
    public static String ES_URL;

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
            DockerImageName.parse("elasticsearch:7.17.27"))
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

    // Atlas container with volume mount
    @Container
    static GenericContainer<?> atlas = createAtlasContainer();

    private static GenericContainer<?> createAtlasContainer() {
        // Get the deploy directory path
        String deployPath = getDeployDirectoryPath();
        LOG.info("Using deploy directory: {}", deployPath);

        // Verify deploy directory exists
        File deployDir = new File(deployPath);
        if (!deployDir.exists() || !deployDir.isDirectory()) {
            throw new IllegalStateException("Deploy directory not found: " + deployPath +
                    ". Please ensure the deploy directory exists with conf, data, elasticsearch, etc.");
        }

        // Create logs and data directories with proper permissions
        File logsDir = new File(deployPath, "logs");
        File dataDir = new File(deployPath, "data");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
            logsDir.setWritable(true, false);
            logsDir.setReadable(true, false);
            logsDir.setExecutable(true, false);
        }
        if (!dataDir.exists()) {
            dataDir.mkdirs();
            dataDir.setWritable(true, false);
            dataDir.setReadable(true, false);
            dataDir.setExecutable(true, false);
        }

        return new GenericContainer<>(DockerImageName.parse("atlanhq/atlas:test"))
                .withNetwork(network)
                .withNetworkAliases("atlas")
                .withExposedPorts(21000)
                //# Mount the entire deploy directory
                .withFileSystemBind(deployPath, "/opt/atlas-deploy", BindMode.READ_WRITE)
                // Mount specific subdirectories to expected locations (READ_WRITE for conf)
                .withFileSystemBind(deployPath + "/conf", "/opt/apache-atlas/conf-orig", BindMode.READ_ONLY)
                .withFileSystemBind(deployPath + "/elasticsearch", "/opt/apache-atlas/elasticsearch", BindMode.READ_ONLY)
                .withFileSystemBind(deployPath + "/models", "/opt/apache-atlas/models", BindMode.READ_ONLY)
                .withEnv(getAtlasEnvironment())
                .withCommand("/bin/bash", "-c", createStartupScript())
                .waitingFor(
                        Wait.forListeningPort()
                                .withStartupTimeout(Duration.ofMinutes(2))
                )
                .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("ATLAS"))
                .dependsOn(kafka, cassandra, elasticsearch, redis, zookeeper);
    }

    private static String getDeployDirectoryPath() {
        // Try different paths where deploy directory might be
        String projectRootPath = System.getProperty("user.dir");
        String[] possiblePaths = {
                projectRootPath+ "/src/test/resources/deploy"
        };

        for (String path : possiblePaths) {
            if (path != null) {
                File dir = new File(path);
                if (dir.exists() && dir.isDirectory()) {
                    // Check if it has expected subdirectories
                    if (new File(dir, "conf").exists() &&
                            new File(dir, "elasticsearch").exists()) {
                        return dir.getAbsolutePath();
                    }
                }
            }
        }

        throw new IllegalStateException(
                "Could not find deploy directory. Please set -Datlas.deploy.dir=/path/to/deploy or ensure deploy directory exists in project root"
        );
    }

    private static Map<String, String> getAtlasEnvironment() {
        Map<String, String> env = new HashMap<>();

        // Atlas home and configuration
        env.put("ATLAS_HOME", "/opt/apache-atlas");
        env.put("ATLAS_CONF", "/opt/apache-atlas/conf");
        env.put("ATLAS_DATA", "/opt/atlas-deploy/data");
        env.put("ATLAS_LOG_DIR", "/opt/atlas-deploy/logs");

        // Tell Atlas we're managing external services
        env.put("MANAGE_LOCAL_HBASE", "false");
        env.put("MANAGE_LOCAL_SOLR", "false");
        env.put("MANAGE_LOCAL_ELASTICSEARCH", "false");
        env.put("MANAGE_LOCAL_CASSANDRA", "false");

        // JVM options
        env.put("ATLAS_SERVER_OPTS", "-server -Xms1g -Xmx2g");
        env.put("ATLAS_SERVER_HEAP", "-Xms1g -Xmx2g");

        // Java 17 options
        env.put("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64");
        env.put("JDK_JAVA_OPTIONS", "--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED");

        env.put("SPRING_PROFILES_ACTIVE", "local");

        // Set atlas.home system property
        env.put("ATLAS_OPTS", "-Datlas.home=/opt/apache-atlas -Datlas.conf=/opt/apache-atlas/conf -Datlas.data=/opt/atlas-deploy/data -Datlas.log.dir=/opt/atlas-deploy/logs");

        return env;
    }

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
            
            # Copy configuration from read-only mount to writable location
            echo "Copying configuration files..."
            cp -r /opt/apache-atlas/conf-orig/* /opt/apache-atlas/conf/
            
            # Fix atlas-env.sh by replacing template variables
            echo "Fixing atlas-env.sh..."
            sed -i 's/${hbase.embedded}/false/g' /opt/apache-atlas/conf/atlas-env.sh
            sed -i 's/${solr.embedded}/false/g' /opt/apache-atlas/conf/atlas-env.sh
            sed -i 's/${cassandra.embedded}/false/g' /opt/apache-atlas/conf/atlas-env.sh
            sed -i 's/${elasticsearch.managed}/false/g' /opt/apache-atlas/conf/atlas-env.sh
            
            # List what we have
            echo "Configuration directory contents:"
            ls -la /opt/apache-atlas/conf/
            
            echo "Elasticsearch directory contents:"
            ls -la /opt/apache-atlas/elasticsearch/
            
            # Update atlas-application.properties with container service addresses
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
            
             # Redis configuration (using local profile RedisServiceLocalImpl)
            atlas.redis.url = redis://redis:6379
            atlas.redis.sentinel.enabled = false
            atlas.redis.sentinel.check_list.enabled = false
            
            atlas.server.bind.address=0.0.0.0
            atlas.authentication.method.file=true
            atlas.authentication.method.kerberos=false
            
            # Disable HBase transaction logging since we're using Cassandra
            atlas.graph.tx.log.tx=false
            atlas.graph.tx.recovery.verbose=false
            
            # Disable HBase audit
            atlas.audit.hbase.zookeeper.quorum=
            atlas.EntityAuditRepository.impl=org.apache.atlas.repository.audit.NoopEntityAuditRepository
            
            atlas.redis.sentinel.check_list.enabled=false
            atlas.authorizer.impl=none
            #atlas.authorizer.simple.authz.policy.file=deploy/conf/atlas-simple-authz-policy.json
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
            #### user credentials file
            atlas.authentication.method.file.filename=/opt/apache-atlas/conf/users-credentials.properties
            atlas.authentication.method.file=true
            atlas.authentication.method.ldap=false
            atlas.authentication.method.keycloak=false
            atlas.authentication.method.pam=false
            atlas.authentication.method.keycloak=false
            #atlas.authentication.method.keycloak.file=${sys:atlas.home}/conf/keycloak.json
            #atlas.authentication.method.keycloak.ugi-groups=false
            #atlas.authentication.method.keycloak.groups_claim=groups
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
            
            # Check Java installation
            echo "Checking Java..."
            which java
            java -version
            
            # Check Python installation
            echo "Checking Python..."
            which python || which python2
            python --version || python2 --version
            
            # List Atlas bin directory
            echo "Atlas bin directory:"
            ls -la /opt/apache-atlas/bin/
            
            # Check if webapp exists
            echo "Checking for webapp..."
            ls -la /opt/apache-atlas/server/ || echo "Server directory not found"
            ls -la /opt/apache-atlas/ | grep -E "(war|jar)" || echo "No WAR/JAR files in atlas root"
            
            if [ -d "/opt/apache-atlas/server/webapp/atlas" ]; then
                echo "Webapp found at /opt/apache-atlas/server/webapp/atlas"
                ls -la /opt/apache-atlas/server/webapp/atlas/
            else
                echo "Webapp not found, looking for war file..."
                find /opt/apache-atlas -name "*.war" -type f || echo "No WAR files found"
            fi
            
            # Start Atlas
            cd /opt/apache-atlas
            echo "Starting Apache Atlas..."
            
            # Check if Java is available
            if ! which java > /dev/null 2>&1; then
                echo "ERROR: Java not found in PATH"
                exit 1
            fi
            
            echo "Java found at: $(which java)"
            java -version
            
            # Build classpath
            echo "Building classpath..."
            CP="/opt/apache-atlas/conf"
            
            # Add server webapp JARs
            if [ -d "/opt/apache-atlas/server/webapp/atlas/WEB-INF/lib" ]; then
                echo "Adding webapp JARs..."
                for jar in /opt/apache-atlas/server/webapp/atlas/WEB-INF/lib/*.jar; do
                    CP="$CP:$jar"
                done
            elif [ -f "/opt/apache-atlas/server/webapp/atlas.war" ]; then
                echo "Found WAR file, extracting..."
                mkdir -p /opt/apache-atlas/server/webapp/atlas
                cd /opt/apache-atlas/server/webapp/atlas
                jar xf ../atlas.war
                cd /opt/apache-atlas
                for jar in /opt/apache-atlas/server/webapp/atlas/WEB-INF/lib/*.jar; do
                    CP="$CP:$jar"
                done
            else
                echo "WARNING: No webapp found!"
                # Try to find JARs in other locations
                find /opt/apache-atlas -name "*.jar" -type f | head -10
            fi
            
            # Add lib JARs if they exist
            if [ -d "/opt/apache-atlas/lib" ]; then
                echo "Adding lib JARs..."
                for jar in /opt/apache-atlas/lib/*.jar; do
                    CP="$CP:$jar"
                done
            fi
            
            # Add libext JARs if they exist
            if [ -d "/opt/apache-atlas/libext" ]; then
                echo "Adding libext JARs..."
                for jar in /opt/apache-atlas/libext/*.jar; do
                    CP="$CP:$jar"
                done
            fi
            
            # Add any JARs from the distro directory
            if [ -d "/opt/apache-atlas/distro" ]; then
                echo "Checking distro directory..."
                for jar in /opt/apache-atlas/distro/*.jar; do
                    if [ -f "$jar" ]; then
                        CP="$CP:$jar"
                    fi
                done
            fi
            
            echo "Starting Atlas with Java directly..."
            echo "Classpath length: $(echo $CP | wc -c) characters"
            
            /usr/bin/java -Datlas.log.dir=/opt/apache-atlas/logs -Datlas.log.file=atlas_kafka_setup.log -Datlas.home=/opt/apache-atlas -Datlas.conf=/opt/apache-atlas/conf -Datlas.data=/opt/apache-atlas/data -classpath /opt/apache-atlas/conf:/opt/apache-atlas/server/webapp/atlas/WEB-INF/classes:/opt/apache-atlas/server/webapp/atlas/WEB-INF/lib/*:/opt/apache-atlas/libext/* org.apache.atlas.hook.AtlasTopicCreator ATLAS_ENTITIES
            
            # Start Atlas with exec to replace the shell process
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
        LOG.info("Setting up test environment...");
        // Setup HTTP client
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        // Setup base URL
        int mappedPort = atlas.getMappedPort(21000);
        ATLAS_BASE_URL = String.format("http://localhost:%d/api/atlas/v2", mappedPort);

        LOG.info("Atlas URL: {}", ATLAS_BASE_URL);
        LOG.info("Atlas container is running: {}", atlas.isRunning());

        // Wait for Atlas API to be ready
        waitForAtlasReady();

        ES_URL = elasticsearch.getHttpHostAddress();
        System.out.println("ES_URL: " + ES_URL);
    }

    private void waitForAtlasReady() {
        LOG.info("Waiting for Atlas API to be ready...");
        int maxRetries = 60;  // 60 retries * 5s = 5 minutes max

        for (int i = 0; i < maxRetries; i++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(ATLAS_BASE_URL + "/types"))
                        .header("Accept", "application/json")
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();

                HttpResponse<String> response =
                        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                LOG.info("Atlas API check attempt {}/{}: status {}", i+1, maxRetries, response.statusCode());

                if (response.statusCode() == 200 || response.statusCode() == 401) {
                    LOG.info("✅ Atlas API is ready!");
                    return;
                }
            } catch (Exception e) {
                LOG.info("Atlas API not ready yet (attempt {}/{}): {}", i+1, maxRetries, e.getMessage());
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        LOG.error("Atlas failed to become ready. Container logs:");
        LOG.error(atlas.getLogs());

        throw new RuntimeException("Atlas API failed to become ready after " + maxRetries + " attempts");
    }

    @AfterAll
    void tearDown() {
        LOG.info("Test completed");
    }
}