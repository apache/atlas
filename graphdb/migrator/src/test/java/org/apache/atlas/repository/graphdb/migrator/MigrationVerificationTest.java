package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;

import static org.testng.Assert.*;

/**
 * Comprehensive migration verification test.
 *
 * Creates realistic Atlas data in JanusGraph (including TypeDef vertices with enum properties,
 * entities with type-qualified property names, edges with relationship properties), runs the
 * full migration pipeline, and prints detailed verification output.
 *
 * This test specifically validates the serializer fix by checking that:
 *   - Property names are correctly resolved (not corrupted)
 *   - Edge tables are populated (not empty)
 *   - TypeDef enum properties are preserved
 *   - Index entries match runtime query patterns
 */
@Test(groups = "integration")
public class MigrationVerificationTest {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationVerificationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CassandraContainer<?> cassandra;
    private ElasticsearchContainer elasticsearch;
    private JanusGraph janusGraph;
    private File janusGraphConfigFile;
    private File migratorConfigFile;
    private CqlSession sourceSession;
    private CqlSession targetSession;

    private String cassandraHost;
    private int cassandraPort;
    private static final String SOURCE_KEYSPACE = "atlas_janus";
    private static final String TARGET_KEYSPACE = "atlas_graph_verify";

    // Track written data
    private final Map<String, Map<String, Object>> writtenEntities = new LinkedHashMap<>();
    private int expectedEdgeCount = 0;
    private int expectedVertexCount = 0;

    @BeforeClass
    public void setUp() throws Exception {
        System.setProperty("api.version", "1.44");

        // Start containers
        cassandra = new CassandraContainer<>(DockerImageName.parse("cassandra:3.11"))
                .withStartupTimeout(Duration.ofMinutes(3))
                .withEnv("CASSANDRA_CLUSTER_NAME", "verify-test")
                .withEnv("CASSANDRA_DC", "datacenter1")
                .withEnv("MAX_HEAP_SIZE", "512M")
                .withEnv("HEAP_NEWSIZE", "128M");
        cassandra.start();

        cassandraHost = cassandra.getHost();
        cassandraPort = cassandra.getMappedPort(9042);

        elasticsearch = new ElasticsearchContainer(
                DockerImageName.parse("elasticsearch:7.17.27"))
                .withEnv("discovery.type", "single-node")
                .withEnv("xpack.security.enabled", "false")
                .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m");
        elasticsearch.start();

        // Create JanusGraph config
        janusGraphConfigFile = File.createTempFile("jg-verify", ".properties");
        try (PrintWriter w = new PrintWriter(new FileWriter(janusGraphConfigFile))) {
            w.println("storage.backend=cql");
            w.println("storage.hostname=" + cassandraHost);
            w.println("storage.port=" + cassandraPort);
            w.println("storage.cql.keyspace=" + SOURCE_KEYSPACE);
            w.println("storage.cql.local-datacenter=datacenter1");
            w.println("storage.cql.replication-factor=1");
            w.println("storage.lock.wait-time=200");
            w.println("index.search.backend=lucene");
            w.println("index.search.directory=" + System.getProperty("java.io.tmpdir") + "/jg-verify-" + System.currentTimeMillis());
            // CRITICAL: Use the EXACT same custom serializers as production Atlas
            w.println("attributes.custom.attribute1.attribute-class=org.apache.atlas.typesystem.types.DataTypes$TypeCategory");
            w.println("attributes.custom.attribute1.serializer-class=org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer");
            w.println("attributes.custom.attribute2.attribute-class=java.util.ArrayList");
            w.println("attributes.custom.attribute2.serializer-class=org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer");
            w.println("attributes.custom.attribute3.attribute-class=java.math.BigInteger");
            w.println("attributes.custom.attribute3.serializer-class=org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer");
            w.println("attributes.custom.attribute4.attribute-class=java.math.BigDecimal");
            w.println("attributes.custom.attribute4.serializer-class=org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer");
            w.println("cache.db-cache=false");
            w.println("storage.batch-loading=true");
        }

        // Open JanusGraph and create schema + data
        janusGraph = JanusGraphFactory.open(janusGraphConfigFile.getAbsolutePath());
        createSchema();
        writeTestData();
        janusGraph.close();
        janusGraph = null;

        // Open CQL sessions
        sourceSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withLocalDatacenter("datacenter1")
                .build();
        targetSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withLocalDatacenter("datacenter1")
                .build();

        // Create migrator config
        migratorConfigFile = File.createTempFile("migration-verify", ".properties");
        try (PrintWriter w = new PrintWriter(new FileWriter(migratorConfigFile))) {
            w.println("source.janusgraph.config=" + janusGraphConfigFile.getAbsolutePath());
            w.println("source.cassandra.hostname=" + cassandraHost);
            w.println("source.cassandra.port=" + cassandraPort);
            w.println("source.cassandra.keyspace=" + SOURCE_KEYSPACE);
            w.println("source.cassandra.datacenter=datacenter1");
            w.println("source.cassandra.edgestore.table=edgestore");
            w.println("target.cassandra.hostname=" + cassandraHost);
            w.println("target.cassandra.port=" + cassandraPort);
            w.println("target.cassandra.keyspace=" + TARGET_KEYSPACE);
            w.println("target.cassandra.datacenter=datacenter1");
            String esAddr = elasticsearch.getHttpHostAddress();
            w.println("target.elasticsearch.hostname=" + esAddr.split(":")[0]);
            w.println("target.elasticsearch.port=" + esAddr.split(":")[1]);
            w.println("target.elasticsearch.protocol=http");
            w.println("target.elasticsearch.index=atlas_vertex_verify");
            w.println("migration.scanner.threads=4");
            w.println("migration.writer.threads=2");
            w.println("migration.writer.batch.size=10");
            w.println("migration.es.bulk.size=50");
            w.println("migration.scan.fetch.size=100");
            w.println("migration.queue.capacity=100");
            w.println("migration.resume=false");
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        if (sourceSession != null) sourceSession.close();
        if (targetSession != null) targetSession.close();
        if (janusGraph != null && janusGraph.isOpen()) janusGraph.close();
        if (janusGraphConfigFile != null) janusGraphConfigFile.delete();
        if (migratorConfigFile != null) migratorConfigFile.delete();
        if (elasticsearch != null) elasticsearch.stop();
        if (cassandra != null) cassandra.stop();
    }

    private void createSchema() {
        JanusGraphManagement mgmt = janusGraph.openManagement();

        // Entity properties (type-qualified names, as Atlas stores them)
        mgmt.makePropertyKey("__guid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__typeName").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__type").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__state").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__timestamp").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__modificationTimestamp").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__createdBy").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__modifiedBy").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__superTypeNames").dataType(String.class).cardinality(Cardinality.SET).make();
        mgmt.makePropertyKey("Referenceable.qualifiedName").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("Asset.name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("Asset.description").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("Asset.connectorName").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("Asset.connectionQualifiedName").dataType(String.class).cardinality(Cardinality.SINGLE).make();

        // TypeDef system properties (ES-encoded: dots→underscores)
        mgmt.makePropertyKey("__type_category").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__type_name").dataType(String.class).cardinality(Cardinality.SINGLE).make();

        // Enum properties (PROPERTY_PREFIX = "__type.")
        mgmt.makePropertyKey("__type.atlas_operation").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__type.atlas_operation.CREATE").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__type.atlas_operation.UPDATE").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__type.atlas_operation.DELETE").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();

        // Edge properties
        mgmt.makePropertyKey("__relationshipGuid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__r__typeName").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("__r__state").dataType(String.class).cardinality(Cardinality.SINGLE).make();

        // Edge labels
        mgmt.makeEdgeLabel("__Table.columns").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("__Schema.tables").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("__Process.inputs").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("__Process.outputs").multiplicity(Multiplicity.MULTI).make();

        mgmt.commit();
    }

    private void writeTestData() {
        // === Entity vertices ===
        Vertex connection = addEntity("Connection", "default/snowflake/12345", "snowflake-conn", "snowflake");
        Vertex db = addEntity("Database", "default/snowflake/12345/PROD_DB", "PROD_DB", "snowflake");
        Vertex schema = addEntity("Schema", "default/snowflake/12345/PROD_DB/PUBLIC", "PUBLIC", "snowflake");

        List<Vertex> tables = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            tables.add(addEntity("Table",
                    "default/snowflake/12345/PROD_DB/PUBLIC/USERS_" + i,
                    "USERS_" + i, "snowflake"));
        }

        List<Vertex> columns = new ArrayList<>();
        for (int t = 0; t < tables.size(); t++) {
            for (int c = 1; c <= 2; c++) {
                columns.add(addEntity("Column",
                        "default/snowflake/12345/PROD_DB/PUBLIC/USERS_" + (t+1) + "/COL_" + c,
                        "COL_" + c, "snowflake"));
            }
        }

        Vertex process = addEntity("Process", "default/snowflake/12345/etl_job", "etl_job", "snowflake");

        // === TypeDef vertex (typeSystem, not asset) ===
        Vertex tableTypeDef = janusGraph.addVertex();
        String typeDefGuid = UUID.randomUUID().toString();
        tableTypeDef.property("__guid", typeDefGuid);
        tableTypeDef.property("__typeName", "Table");
        tableTypeDef.property("__type", "typeSystem");
        tableTypeDef.property("__type_name", "Table");
        tableTypeDef.property("__type_category", 1);  // ENTITY category
        tableTypeDef.property("__state", "ACTIVE");
        expectedVertexCount++;

        // === Enum TypeDef vertex (atlas_operation) ===
        Vertex enumTypeDef = janusGraph.addVertex();
        String enumGuid = UUID.randomUUID().toString();
        enumTypeDef.property("__guid", enumGuid);
        enumTypeDef.property("__typeName", "atlas_operation");
        enumTypeDef.property("__type", "typeSystem");
        enumTypeDef.property("__type_name", "atlas_operation");
        enumTypeDef.property("__type_category", 0);  // ENUM category
        enumTypeDef.property("__state", "ACTIVE");
        // Enum values stored as __type.atlas_operation property
        enumTypeDef.property("__type.atlas_operation", "[\"CREATE\",\"UPDATE\",\"DELETE\"]");
        enumTypeDef.property("__type.atlas_operation.CREATE", 0);
        enumTypeDef.property("__type.atlas_operation.UPDATE", 1);
        enumTypeDef.property("__type.atlas_operation.DELETE", 2);
        expectedVertexCount++;

        // Store enum info for verification
        Map<String, Object> enumInfo = new LinkedHashMap<>();
        enumInfo.put("guid", enumGuid);
        enumInfo.put("typeName", "atlas_operation");
        enumInfo.put("enumValues", "CREATE,UPDATE,DELETE");
        writtenEntities.put("ENUM:" + enumGuid, enumInfo);

        // Store TypeDef info
        Map<String, Object> typeDefInfo = new LinkedHashMap<>();
        typeDefInfo.put("guid", typeDefGuid);
        typeDefInfo.put("typeName", "Table");
        typeDefInfo.put("vertexType", "typeSystem");
        writtenEntities.put("TYPEDEF:" + typeDefGuid, typeDefInfo);

        // === Edges ===
        addEdge(schema, tables.get(0), "__Schema.tables", "schema_tables");
        addEdge(schema, tables.get(1), "__Schema.tables", "schema_tables");
        addEdge(schema, tables.get(2), "__Schema.tables", "schema_tables");
        for (int t = 0; t < tables.size(); t++) {
            addEdge(tables.get(t), columns.get(t * 2), "__Table.columns", "table_columns");
            addEdge(tables.get(t), columns.get(t * 2 + 1), "__Table.columns", "table_columns");
        }
        addEdge(process, tables.get(0), "__Process.inputs", "process_inputs");
        addEdge(process, tables.get(1), "__Process.outputs", "process_outputs");

        janusGraph.tx().commit();
        LOG.info("Test data: {} vertices, {} edges", expectedVertexCount, expectedEdgeCount);
    }

    private Vertex addEntity(String typeName, String qualifiedName, String name, String connector) {
        String guid = UUID.randomUUID().toString();
        Vertex v = janusGraph.addVertex();
        v.property("__guid", guid);
        v.property("__typeName", typeName);
        v.property("__type", "asset");
        v.property("__state", "ACTIVE");
        v.property("__timestamp", System.currentTimeMillis());
        v.property("__modificationTimestamp", System.currentTimeMillis());
        v.property("__createdBy", "test-user");
        v.property("__modifiedBy", "test-user");
        v.property("Referenceable.qualifiedName", qualifiedName);
        v.property("Asset.name", name);
        v.property("Asset.description", "Description for " + name);
        v.property("Asset.connectorName", connector);
        v.property("Asset.connectionQualifiedName", "default/snowflake/12345");
        v.property("__superTypeNames", "Referenceable");
        v.property("__superTypeNames", "Asset");

        Map<String, Object> info = new LinkedHashMap<>();
        info.put("guid", guid);
        info.put("typeName", typeName);
        info.put("qualifiedName", qualifiedName);
        info.put("name", name);
        info.put("description", "Description for " + name);
        info.put("connectorName", connector);
        writtenEntities.put(guid, info);
        expectedVertexCount++;
        return v;
    }

    private void addEdge(Vertex out, Vertex in, String label, String relType) {
        Edge edge = out.addEdge(label, in);
        edge.property("__relationshipGuid", UUID.randomUUID().toString());
        edge.property("__r__typeName", relType);
        edge.property("__r__state", "ACTIVE");
        expectedEdgeCount++;
    }

    // =================== RUN MIGRATION ===================

    @Test
    public void runMigrationAndVerify() throws Exception {
        LOG.info("============================================================");
        LOG.info("=== STARTING FULL MIGRATION VERIFICATION ===");
        LOG.info("============================================================");

        // Run migration
        MigratorConfig config = new MigratorConfig(migratorConfigFile.getAbsolutePath());
        MigrationMetrics metrics = new MigrationMetrics();
        metrics.start();

        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, targetSession);
        // Retry init() to handle Cassandra cold-start timeouts
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                writer.init();
                break;
            } catch (Exception e) {
                LOG.warn("writer.init() attempt {} failed: {}", attempt, e.getMessage());
                if (attempt == 3) throw e;
                Thread.sleep(5000);
            }
        }
        MigrationStateStore stateStore = new MigrationStateStore(targetSession, TARGET_KEYSPACE);
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                stateStore.init();
                break;
            } catch (Exception e) {
                LOG.warn("stateStore.init() attempt {} failed: {}", attempt, e.getMessage());
                if (attempt == 3) throw e;
                Thread.sleep(3000);
            }
        }
        writer.startWriters();

        JanusGraphScanner scanner = new JanusGraphScanner(config, metrics, sourceSession);
        scanner.scanAll(vertex -> writer.enqueue(vertex), stateStore, "scan");
        writer.signalScanComplete();
        writer.awaitCompletion();
        scanner.close();
        writer.close();

        LOG.info("Migration metrics: {}", metrics.summary());

        // ============ VERIFICATION ============

        StringBuilder report = new StringBuilder();
        report.append("\n");
        report.append("============================================================\n");
        report.append("=== MIGRATION VERIFICATION REPORT ===\n");
        report.append("============================================================\n\n");

        // 1. Table counts
        long vertexCount = count(TARGET_KEYSPACE + ".vertices");
        long edgesOutCount = count(TARGET_KEYSPACE + ".edges_out");
        long edgesInCount = count(TARGET_KEYSPACE + ".edges_in");
        long edgesByIdCount = count(TARGET_KEYSPACE + ".edges_by_id");
        long guidIndexCount = count(TARGET_KEYSPACE + ".vertex_index");
        long propIndexCount = count(TARGET_KEYSPACE + ".vertex_property_index");

        report.append("--- TABLE COUNTS ---\n");
        report.append(String.format("  vertices:              %d (expected >= %d)\n", vertexCount, expectedVertexCount));
        report.append(String.format("  edges_out:             %d (expected >= %d)\n", edgesOutCount, expectedEdgeCount));
        report.append(String.format("  edges_in:              %d (expected >= %d)\n", edgesInCount, expectedEdgeCount));
        report.append(String.format("  edges_by_id:           %d (expected >= %d)\n", edgesByIdCount, expectedEdgeCount));
        report.append(String.format("  vertex_index:          %d\n", guidIndexCount));
        report.append(String.format("  vertex_property_index: %d\n", propIndexCount));
        report.append("\n");

        assertTrue(vertexCount >= expectedVertexCount, "vertex count");
        assertTrue(edgesOutCount >= expectedEdgeCount, "edges_out count");
        assertTrue(edgesInCount >= expectedEdgeCount, "edges_in count");
        assertTrue(edgesByIdCount >= expectedEdgeCount, "edges_by_id count");
        assertEquals(edgesOutCount, edgesInCount, "edges_out == edges_in");
        assertEquals(edgesByIdCount, edgesOutCount, "edges_by_id == edges_out");

        report.append("  [PASS] All table counts correct\n");
        report.append("  [PASS] Edge tables consistent (out == in == by_id)\n\n");

        // 2. Verify property names are NOT corrupted
        report.append("--- PROPERTY NAME VERIFICATION ---\n");
        int propsChecked = 0;
        int propsCorrect = 0;
        List<String> propertyErrors = new ArrayList<>();

        ResultSet rs = targetSession.execute("SELECT vertex_id, properties, type_name FROM " + TARGET_KEYSPACE + ".vertices");
        for (Row row : rs) {
            String propsJson = row.getString("properties");
            if (propsJson == null || propsJson.equals("{}")) continue;

            Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
            String guid = props.get("__guid") != null ? props.get("__guid").toString() : null;
            if (guid == null) continue;

            Map<String, Object> expected = writtenEntities.get(guid);
            if (expected == null) continue;

            propsChecked++;

            // Check property names are correctly normalized
            // After normalization: "Referenceable.qualifiedName" → "qualifiedName"
            // After normalization: "Asset.name" → "name"
            // After normalization: "Asset.connectorName" → "connectorName"
            String typeName = props.get("__typeName") != null ? props.get("__typeName").toString() : null;
            String qn = props.get("qualifiedName") != null ? props.get("qualifiedName").toString() : null;
            String name = props.get("name") != null ? props.get("name").toString() : null;
            String connector = props.get("connectorName") != null ? props.get("connectorName").toString() : null;
            String desc = props.get("description") != null ? props.get("description").toString() : null;

            boolean correct = true;
            StringBuilder vertexReport = new StringBuilder();
            vertexReport.append(String.format("  Vertex %s (guid=%s):\n", row.getString("vertex_id"), guid));

            if (expected.containsKey("qualifiedName")) {
                String expectedQn = expected.get("qualifiedName").toString();
                if (expectedQn.equals(qn)) {
                    vertexReport.append(String.format("    qualifiedName: '%s' [OK]\n", qn));
                } else {
                    vertexReport.append(String.format("    qualifiedName: '%s' [WRONG, expected '%s']\n", qn, expectedQn));
                    correct = false;
                }
            }

            if (expected.containsKey("name")) {
                String expectedName = expected.get("name").toString();
                if (expectedName.equals(name)) {
                    vertexReport.append(String.format("    name: '%s' [OK]\n", name));
                } else {
                    vertexReport.append(String.format("    name: '%s' [WRONG, expected '%s']\n", name, expectedName));
                    correct = false;
                }
            }

            if (expected.containsKey("connectorName")) {
                String expectedConn = expected.get("connectorName").toString();
                if (expectedConn.equals(connector)) {
                    vertexReport.append(String.format("    connectorName: '%s' [OK]\n", connector));
                } else {
                    vertexReport.append(String.format("    connectorName: '%s' [WRONG, expected '%s']\n", connector, expectedConn));
                    correct = false;
                }
            }

            if (expected.containsKey("description")) {
                String expectedDesc = expected.get("description").toString();
                if (expectedDesc.equals(desc)) {
                    vertexReport.append(String.format("    description: '%s' [OK]\n", desc));
                } else {
                    vertexReport.append(String.format("    description: '%s' [WRONG, expected '%s']\n", desc, expectedDesc));
                    correct = false;
                }
            }

            if (expected.containsKey("typeName")) {
                String expectedType = expected.get("typeName").toString();
                if (expectedType.equals(typeName)) {
                    vertexReport.append(String.format("    __typeName: '%s' [OK]\n", typeName));
                } else {
                    vertexReport.append(String.format("    __typeName: '%s' [WRONG, expected '%s']\n", typeName, expectedType));
                    correct = false;
                }
            }

            if (correct) {
                propsCorrect++;
            } else {
                propertyErrors.add(vertexReport.toString());
            }

            // Print first 5 vertices to report
            if (propsChecked <= 5) {
                report.append(vertexReport);
            }
        }

        report.append(String.format("\n  Checked: %d vertices, Correct: %d, Errors: %d\n", propsChecked, propsCorrect, propertyErrors.size()));
        if (!propertyErrors.isEmpty()) {
            report.append("  ERRORS:\n");
            for (String err : propertyErrors) {
                report.append(err);
            }
        }
        report.append(String.format("  [%s] Property names correctly normalized\n\n",
                propertyErrors.isEmpty() ? "PASS" : "FAIL"));

        assertEquals(propertyErrors.size(), 0, "Property name errors: " + propertyErrors);

        // 3. Verify NO corrupted property names exist (e.g., __type.Asset.*)
        report.append("--- CORRUPTED PROPERTY NAME CHECK ---\n");
        int corruptedVertices = 0;
        rs = targetSession.execute("SELECT vertex_id, properties FROM " + TARGET_KEYSPACE + ".vertices");
        for (Row row : rs) {
            String propsJson = row.getString("properties");
            if (propsJson == null) continue;
            Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
            for (String key : props.keySet()) {
                // __type.Asset.*, __type.Referenceable.* are CORRUPTION indicators
                if (key.startsWith("__type.Asset.") || key.startsWith("__type.Referenceable.") ||
                    key.startsWith("__type.SQL.") || key.startsWith("__type.Catalog.")) {
                    report.append(String.format("  CORRUPTED: vertex %s has key '%s'\n",
                            row.getString("vertex_id"), key));
                    corruptedVertices++;
                    break;
                }
            }
        }
        report.append(String.format("  Corrupted vertices: %d\n", corruptedVertices));
        report.append(String.format("  [%s] No corrupted __type.Asset.* / __type.Referenceable.* property names\n\n",
                corruptedVertices == 0 ? "PASS" : "FAIL"));
        assertEquals(corruptedVertices, 0, "Should have no corrupted property names");

        // 4. Verify TypeDef enum properties
        report.append("--- TYPEDEF ENUM VERIFICATION ---\n");
        boolean enumFound = false;
        rs = targetSession.execute("SELECT vertex_id, properties FROM " + TARGET_KEYSPACE + ".vertices");
        for (Row row : rs) {
            String propsJson = row.getString("properties");
            if (propsJson == null) continue;
            Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
            if ("typeSystem".equals(props.get("__type")) && "atlas_operation".equals(props.get("__type_name"))) {
                enumFound = true;
                report.append(String.format("  Found enum TypeDef vertex (vertex_id=%s):\n", row.getString("vertex_id")));

                Object enumValues = props.get("__type.atlas_operation");
                Object createOrd = props.get("__type.atlas_operation.CREATE");
                Object updateOrd = props.get("__type.atlas_operation.UPDATE");
                Object deleteOrd = props.get("__type.atlas_operation.DELETE");

                report.append(String.format("    __type.atlas_operation: %s [%s]\n",
                        enumValues, enumValues != null ? "OK" : "MISSING"));
                report.append(String.format("    __type.atlas_operation.CREATE: %s [%s]\n",
                        createOrd, createOrd != null ? "OK" : "MISSING"));
                report.append(String.format("    __type.atlas_operation.UPDATE: %s [%s]\n",
                        updateOrd, updateOrd != null ? "OK" : "MISSING"));
                report.append(String.format("    __type.atlas_operation.DELETE: %s [%s]\n",
                        deleteOrd, deleteOrd != null ? "OK" : "MISSING"));

                assertNotNull(enumValues, "Enum values list should exist");
                assertNotNull(createOrd, "CREATE ordinal should exist");
                assertNotNull(updateOrd, "UPDATE ordinal should exist");
                assertNotNull(deleteOrd, "DELETE ordinal should exist");
                break;
            }
        }
        report.append(String.format("  [%s] Enum TypeDef properties preserved\n\n", enumFound ? "PASS" : "FAIL"));
        assertTrue(enumFound, "Should have found the atlas_operation enum TypeDef vertex");

        // 5. Verify edge properties
        report.append("--- EDGE VERIFICATION ---\n");
        int edgesWithRelGuid = 0;
        int edgesWithRelType = 0;
        rs = targetSession.execute("SELECT edge_id, edge_label, out_vertex_id, in_vertex_id, properties FROM " +
                TARGET_KEYSPACE + ".edges_by_id");
        int edgeSample = 0;
        for (Row row : rs) {
            String propsJson = row.getString("properties");
            if (propsJson != null && !propsJson.equals("{}")) {
                Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
                if (props.containsKey("__relationshipGuid")) edgesWithRelGuid++;
                if (props.containsKey("__r__typeName")) edgesWithRelType++;

                if (edgeSample++ < 3) {
                    report.append(String.format("  Edge %s: %s -[%s]-> %s\n",
                            row.getString("edge_id"),
                            row.getString("out_vertex_id"),
                            row.getString("edge_label"),
                            row.getString("in_vertex_id")));
                    report.append(String.format("    __relationshipGuid: %s\n", props.get("__relationshipGuid")));
                    report.append(String.format("    __r__typeName: %s\n", props.get("__r__typeName")));
                    report.append(String.format("    __r__state: %s\n", props.get("__r__state")));
                }
            }
        }
        report.append(String.format("\n  Edges with __relationshipGuid: %d\n", edgesWithRelGuid));
        report.append(String.format("  Edges with __r__typeName: %d\n", edgesWithRelType));
        report.append(String.format("  [%s] Edge properties preserved\n\n",
                edgesWithRelGuid > 0 && edgesWithRelType > 0 ? "PASS" : "FAIL"));
        assertTrue(edgesWithRelGuid > 0, "Edges should have __relationshipGuid");

        // 6. Verify indexes
        report.append("--- INDEX VERIFICATION ---\n");

        // GUID index
        int guidIndexFound = 0;
        for (Map.Entry<String, Map<String, Object>> entry : writtenEntities.entrySet()) {
            String guid = entry.getValue().get("guid").toString();
            ResultSet idxRs = targetSession.execute("SELECT vertex_id FROM " + TARGET_KEYSPACE +
                    ".vertex_index WHERE index_name = '__guid_idx' AND index_value = '" + guid + "'");
            if (idxRs.one() != null) guidIndexFound++;
        }
        report.append(String.format("  GUID index: %d/%d found\n", guidIndexFound, writtenEntities.size()));

        // TypeDef 1:1 index
        ResultSet typeIdxRs = targetSession.execute("SELECT vertex_id FROM " + TARGET_KEYSPACE +
                ".vertex_index WHERE index_name = 'type_typename_idx' AND index_value = 'typeSystem:Table'");
        boolean typeIdxFound = typeIdxRs.one() != null;
        report.append(String.format("  TypeDef index (typeSystem:Table): %s\n", typeIdxFound ? "FOUND" : "MISSING"));

        // Enum TypeDef 1:1 index
        ResultSet enumIdxRs = targetSession.execute("SELECT vertex_id FROM " + TARGET_KEYSPACE +
                ".vertex_index WHERE index_name = 'type_typename_idx' AND index_value = 'typeSystem:atlas_operation'");
        boolean enumIdxFound = enumIdxRs.one() != null;
        report.append(String.format("  TypeDef index (typeSystem:atlas_operation): %s\n", enumIdxFound ? "FOUND" : "MISSING"));

        report.append(String.format("  [%s] Index entries correct\n\n",
                guidIndexFound == writtenEntities.size() && typeIdxFound && enumIdxFound ? "PASS" : "FAIL"));

        assertEquals(guidIndexFound, writtenEntities.size(), "All GUIDs should be indexed");
        assertTrue(typeIdxFound, "TypeDef index should exist");
        assertTrue(enumIdxFound, "Enum TypeDef index should exist");

        // 7. Migration metrics
        report.append("--- MIGRATION METRICS ---\n");
        report.append(String.format("  Vertices scanned: %d\n", metrics.getVerticesScanned()));
        report.append(String.format("  Vertices written: %d\n", metrics.getVerticesWritten()));
        report.append(String.format("  Edges written: %d\n", metrics.getEdgesWritten()));
        report.append(String.format("  Indexes written: %d\n", metrics.getIndexesWritten()));
        report.append(String.format("  Decode errors: %d\n", metrics.getDecodeErrors()));
        report.append(String.format("  Write errors: %d\n", metrics.getWriteErrors()));
        report.append(String.format("  [%s] Zero write errors\n\n",
                metrics.getWriteErrors() == 0 ? "PASS" : "FAIL"));

        assertEquals(metrics.getWriteErrors(), 0L, "Zero write errors");

        report.append("============================================================\n");
        report.append("=== ALL VERIFICATIONS PASSED ===\n");
        report.append("============================================================\n");

        // Print the full report
        LOG.info(report.toString());
        System.out.println(report);
    }

    private long count(String table) {
        try {
            Row row = targetSession.execute("SELECT count(*) FROM " + table).one();
            return row != null ? row.getLong(0) : 0;
        } catch (Exception e) {
            return -1;
        }
    }
}
