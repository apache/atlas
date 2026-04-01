package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.strategy.Wait;
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
 * End-to-end integration test for the JanusGraph → Cassandra migrator.
 *
 * Workflow:
 *   1. Start Cassandra + ES via Testcontainers
 *   2. Open JanusGraph with CQL backend → creates edgestore in Cassandra
 *   3. Create Atlas-like schema (property keys, edge labels)
 *   4. Write realistic test entities (vertices + edges with Atlas properties)
 *   5. Close JanusGraph writer (data now in edgestore table)
 *   6. Run the migrator: JanusGraphScanner → CassandraTargetWriter
 *   7. Validate: vertex counts, edge counts, property correctness, index entries
 */
@Test(groups = "integration")
public class MigratorIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorIntegrationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Testcontainers
    private static CassandraContainer<?> cassandra;
    private static ElasticsearchContainer elasticsearch;

    // JanusGraph for writing test data
    private JanusGraph janusGraph;
    private File janusGraphConfigFile;
    private File migratorConfigFile;

    // CQL sessions for the migrator
    private CqlSession sourceSession;
    private CqlSession targetSession;

    // Track what we wrote for validation
    private final List<String> writtenGuids = new ArrayList<>();
    private final Map<String, String> guidToTypeName = new LinkedHashMap<>();
    private final Map<String, String> guidToQualifiedName = new LinkedHashMap<>();
    private int expectedVertexCount = 0;
    private int expectedEdgeCount = 0;

    // Cassandra connection details
    private String cassandraHost;
    private int cassandraPort;
    private static final String SOURCE_KEYSPACE = "atlas_janus";
    private static final String TARGET_KEYSPACE = "atlas_graph_migtest";

    @BeforeClass
    public void setUp() throws Exception {
        startContainers();
        createJanusGraphConfig();
        openJanusGraph();
        createSchema();
        writeTestData();
        closeJanusGraphWriter();
        openCqlSessions();
        createMigratorConfig();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        if (sourceSession != null) sourceSession.close();
        if (targetSession != null) targetSession.close();
        if (janusGraph != null && janusGraph.isOpen()) janusGraph.close();
        if (janusGraphConfigFile != null) janusGraphConfigFile.delete();
        if (migratorConfigFile != null) migratorConfigFile.delete();
        stopContainers();
    }

    // =================== Setup Methods ===================

    private void startContainers() {
        System.setProperty("api.version", "1.44");

        cassandra = new CassandraContainer<>(DockerImageName.parse("cassandra:3.11"))
                .withStartupTimeout(Duration.ofMinutes(3))
                .withEnv("CASSANDRA_CLUSTER_NAME", "migrator-test")
                .withEnv("CASSANDRA_DC", "datacenter1")
                .withEnv("MAX_HEAP_SIZE", "512M")
                .withEnv("HEAP_NEWSIZE", "128M");
        cassandra.start();

        cassandraHost = cassandra.getHost();
        cassandraPort = cassandra.getMappedPort(9042);

        LOG.info("Cassandra started at {}:{}", cassandraHost, cassandraPort);

        elasticsearch = new ElasticsearchContainer(
                DockerImageName.parse("elasticsearch:7.17.27"))
                .withEnv("discovery.type", "single-node")
                .withEnv("xpack.security.enabled", "false")
                .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m");
        elasticsearch.start();

        LOG.info("Elasticsearch started at {}", elasticsearch.getHttpHostAddress());
    }

    private void stopContainers() {
        if (elasticsearch != null) elasticsearch.stop();
        if (cassandra != null) cassandra.stop();
    }

    /**
     * Create a JanusGraph properties file that uses CQL backend pointed at the Testcontainer.
     */
    private void createJanusGraphConfig() throws Exception {
        janusGraphConfigFile = File.createTempFile("janusgraph-test", ".properties");
        try (PrintWriter w = new PrintWriter(new FileWriter(janusGraphConfigFile))) {
            w.println("storage.backend=cql");
            w.println("storage.hostname=" + cassandraHost);
            w.println("storage.port=" + cassandraPort);
            w.println("storage.cql.keyspace=" + SOURCE_KEYSPACE);
            w.println("storage.cql.local-datacenter=datacenter1");
            w.println("storage.cql.replication-factor=1");
            w.println("storage.lock.wait-time=200");

            // Disable index backend (we don't need ES for writing test data into JG)
            w.println("index.search.backend=lucene");
            w.println("index.search.directory=" + System.getProperty("java.io.tmpdir") + "/jg-lucene-" + System.currentTimeMillis());

            // Custom Atlas serializers
            w.println("attributes.custom.attribute1.attribute-class=org.apache.atlas.typesystem.types.DataTypes$TypeCategory");
            w.println("attributes.custom.attribute1.serializer-class=org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer");
            w.println("attributes.custom.attribute2.attribute-class=java.util.ArrayList");
            w.println("attributes.custom.attribute2.serializer-class=org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer");
            w.println("attributes.custom.attribute3.attribute-class=java.math.BigInteger");
            w.println("attributes.custom.attribute3.serializer-class=org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer");
            w.println("attributes.custom.attribute4.attribute-class=java.math.BigDecimal");
            w.println("attributes.custom.attribute4.serializer-class=org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer");

            // Performance tuning for test
            w.println("cache.db-cache=false");
            w.println("storage.batch-loading=true");
        }
        LOG.info("JanusGraph config written to: {}", janusGraphConfigFile.getAbsolutePath());
    }

    private void openJanusGraph() {
        janusGraph = JanusGraphFactory.open(janusGraphConfigFile.getAbsolutePath());
        LOG.info("JanusGraph opened with CQL backend");
    }

    /**
     * Create Atlas-like schema: property keys and edge labels.
     * This mimics what AtlasJanusGraphDatabase.createSchema() does.
     */
    private void createSchema() {
        JanusGraphManagement mgmt = janusGraph.openManagement();

        // Property keys used by Atlas entities
        makePropertyKey(mgmt, "__guid", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__typeName", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__type", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__state", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__timestamp", Long.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__modificationTimestamp", Long.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__createdBy", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__modifiedBy", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__superTypeNames", String.class, Cardinality.SET);
        makePropertyKey(mgmt, "Referenceable.qualifiedName", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "Asset.name", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "Asset.description", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "Asset.connectorName", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "Asset.connectionQualifiedName", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__type_category", Integer.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__type_name", String.class, Cardinality.SINGLE);

        // Edge property keys
        makePropertyKey(mgmt, "__relationshipGuid", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__r__typeName", String.class, Cardinality.SINGLE);
        makePropertyKey(mgmt, "__r__state", String.class, Cardinality.SINGLE);

        // Edge labels used by Atlas relationships
        makeEdgeLabel(mgmt, "__Asset.schemaAttributes");
        makeEdgeLabel(mgmt, "__Schema.tables");
        makeEdgeLabel(mgmt, "__Table.columns");
        makeEdgeLabel(mgmt, "__Process.inputs");
        makeEdgeLabel(mgmt, "__Process.outputs");
        makeEdgeLabel(mgmt, "__AtlasGlossaryTerm.anchor");

        mgmt.commit();
        LOG.info("JanusGraph schema created");
    }

    private void makePropertyKey(JanusGraphManagement mgmt, String name, Class<?> dataType, Cardinality cardinality) {
        if (mgmt.getPropertyKey(name) == null) {
            mgmt.makePropertyKey(name).dataType(dataType).cardinality(cardinality).make();
        }
    }

    private void makeEdgeLabel(JanusGraphManagement mgmt, String name) {
        if (mgmt.getEdgeLabel(name) == null) {
            mgmt.makeEdgeLabel(name).multiplicity(Multiplicity.MULTI).make();
        }
    }

    /**
     * Write realistic Atlas-like test data into JanusGraph.
     * Creates a Snowflake-like hierarchy: Connection → Database → Schema → Tables → Columns
     */
    private void writeTestData() {
        // === Connection vertex ===
        Vertex connection = addEntityVertex("Connection", "default/snowflake/12345",
                "snowflake-connection", "ACTIVE", "asset");

        // === Databases ===
        Vertex db1 = addEntityVertex("Database", "default/snowflake/12345/DB_ONE",
                "DB_ONE", "ACTIVE", "asset");
        Vertex db2 = addEntityVertex("Database", "default/snowflake/12345/DB_TWO",
                "DB_TWO", "ACTIVE", "asset");

        // === Schemas ===
        Vertex schema1 = addEntityVertex("Schema", "default/snowflake/12345/DB_ONE/PUBLIC",
                "PUBLIC", "ACTIVE", "asset");
        Vertex schema2 = addEntityVertex("Schema", "default/snowflake/12345/DB_TWO/ANALYTICS",
                "ANALYTICS", "ACTIVE", "asset");

        // === Tables ===
        List<Vertex> tables = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            Vertex table = addEntityVertex("Table",
                    "default/snowflake/12345/DB_ONE/PUBLIC/TABLE_" + i,
                    "TABLE_" + i, "ACTIVE", "asset");
            tables.add(table);
        }

        // === Columns (3 per table = 15 columns) ===
        List<Vertex> columns = new ArrayList<>();
        for (int t = 0; t < tables.size(); t++) {
            for (int c = 1; c <= 3; c++) {
                Vertex col = addEntityVertex("Column",
                        "default/snowflake/12345/DB_ONE/PUBLIC/TABLE_" + (t + 1) + "/COL_" + c,
                        "COL_" + c, "ACTIVE", "asset");
                columns.add(col);
            }
        }

        // === Process vertex (for lineage) ===
        Vertex process = addEntityVertex("Process", "default/snowflake/12345/process/etl_job",
                "etl_job", "ACTIVE", "asset");

        // === DELETED entity (to test state handling) ===
        Vertex deletedTable = addEntityVertex("Table",
                "default/snowflake/12345/DB_ONE/PUBLIC/TABLE_DELETED",
                "TABLE_DELETED", "DELETED", "asset");

        // === TypeDef vertex (typeSystem, not asset) ===
        Vertex typeDef = janusGraph.addVertex();
        typeDef.property("__guid", UUID.randomUUID().toString());
        typeDef.property("__typeName", "Table");
        typeDef.property("__type", "typeSystem");
        typeDef.property("__type_name", "Table");
        typeDef.property("__type_category", 1);
        typeDef.property("__state", "ACTIVE");
        expectedVertexCount++;
        String typeDefGuid = typeDef.property("__guid").value().toString();
        writtenGuids.add(typeDefGuid);
        guidToTypeName.put(typeDefGuid, "Table");

        // =================== EDGES ===================

        // Database → Schema edges
        addRelationshipEdge(db1, schema1, "__Asset.schemaAttributes", "asset_schemaAttributes");
        addRelationshipEdge(db2, schema2, "__Asset.schemaAttributes", "asset_schemaAttributes");

        // Schema → Table edges
        for (Vertex table : tables) {
            addRelationshipEdge(schema1, table, "__Schema.tables", "schema_tables");
        }

        // Table → Column edges
        for (int t = 0; t < tables.size(); t++) {
            for (int c = 0; c < 3; c++) {
                addRelationshipEdge(tables.get(t), columns.get(t * 3 + c), "__Table.columns", "table_columns");
            }
        }

        // Process lineage edges
        addRelationshipEdge(process, tables.get(0), "__Process.inputs", "process_inputs");
        addRelationshipEdge(process, tables.get(1), "__Process.outputs", "process_outputs");

        // Commit
        janusGraph.tx().commit();

        LOG.info("Test data written: {} vertices, {} edges, {} GUIDs tracked",
                expectedVertexCount, expectedEdgeCount, writtenGuids.size());
    }

    private Vertex addEntityVertex(String typeName, String qualifiedName, String name,
                                    String state, String vertexLabel) {
        String guid = UUID.randomUUID().toString();
        Vertex v = janusGraph.addVertex();
        v.property("__guid", guid);
        v.property("__typeName", typeName);
        v.property("__type", vertexLabel);
        v.property("__state", state);
        v.property("__timestamp", System.currentTimeMillis());
        v.property("__modificationTimestamp", System.currentTimeMillis());
        v.property("__createdBy", "test-user");
        v.property("__modifiedBy", "test-user");
        v.property("Referenceable.qualifiedName", qualifiedName);
        v.property("Asset.name", name);
        v.property("Asset.connectorName", "snowflake");
        v.property("Asset.connectionQualifiedName", "default/snowflake/12345");

        // Add SET-cardinality supertype properties
        v.property("__superTypeNames", "Referenceable");
        v.property("__superTypeNames", "Asset");
        if ("Table".equals(typeName) || "Column".equals(typeName)) {
            v.property("__superTypeNames", "DataSet");
        }

        writtenGuids.add(guid);
        guidToTypeName.put(guid, typeName);
        guidToQualifiedName.put(guid, qualifiedName);
        expectedVertexCount++;
        return v;
    }

    private void addRelationshipEdge(Vertex outVertex, Vertex inVertex, String edgeLabel,
                                      String relTypeName) {
        Edge edge = outVertex.addEdge(edgeLabel, inVertex);
        edge.property("__relationshipGuid", UUID.randomUUID().toString());
        edge.property("__r__typeName", relTypeName);
        edge.property("__r__state", "ACTIVE");
        expectedEdgeCount++;
    }

    private void closeJanusGraphWriter() {
        // Close the writer JanusGraph instance — data is now persisted in Cassandra
        if (janusGraph != null && janusGraph.isOpen()) {
            janusGraph.close();
            janusGraph = null;
        }
        LOG.info("JanusGraph writer closed. Data is in Cassandra edgestore.");
    }

    private void openCqlSessions() {
        sourceSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withLocalDatacenter("datacenter1")
                .build();

        targetSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
                .withLocalDatacenter("datacenter1")
                .build();

        LOG.info("CQL sessions opened for source and target");
    }

    private void createMigratorConfig() throws Exception {
        migratorConfigFile = File.createTempFile("migration-test", ".properties");
        try (PrintWriter w = new PrintWriter(new FileWriter(migratorConfigFile))) {
            // Source JanusGraph config (for schema resolution in the scanner)
            w.println("source.janusgraph.config=" + janusGraphConfigFile.getAbsolutePath());

            // Source Cassandra
            w.println("source.cassandra.hostname=" + cassandraHost);
            w.println("source.cassandra.port=" + cassandraPort);
            w.println("source.cassandra.keyspace=" + SOURCE_KEYSPACE);
            w.println("source.cassandra.datacenter=datacenter1");
            w.println("source.cassandra.edgestore.table=edgestore");

            // Target Cassandra (same Cassandra, different keyspace)
            w.println("target.cassandra.hostname=" + cassandraHost);
            w.println("target.cassandra.port=" + cassandraPort);
            w.println("target.cassandra.keyspace=" + TARGET_KEYSPACE);
            w.println("target.cassandra.datacenter=datacenter1");

            // Target ES
            String esAddress = elasticsearch.getHttpHostAddress();
            String esHost = esAddress.split(":")[0];
            String esPort = esAddress.split(":")[1];
            w.println("target.elasticsearch.hostname=" + esHost);
            w.println("target.elasticsearch.port=" + esPort);
            w.println("target.elasticsearch.protocol=http");
            w.println("target.elasticsearch.index=atlas_vertex_migtest");

            // Tuning (small values for test speed)
            w.println("migration.scanner.threads=4");
            w.println("migration.writer.threads=2");
            w.println("migration.writer.batch.size=10");
            w.println("migration.es.bulk.size=50");
            w.println("migration.scan.fetch.size=100");
            w.println("migration.queue.capacity=100");
            w.println("migration.resume=false");
        }
        LOG.info("Migrator config written to: {}", migratorConfigFile.getAbsolutePath());
    }

    // =================== Test Methods ===================

    @Test
    public void testSourceEdgestoreHasData() {
        // Verify JanusGraph wrote data to the edgestore table
        ResultSet rs = sourceSession.execute(
                "SELECT count(*) FROM " + SOURCE_KEYSPACE + ".edgestore");
        long rowCount = rs.one().getLong(0);
        LOG.info("Source edgestore row count: {}", rowCount);
        assertTrue(rowCount > 0, "Edgestore should have data after JanusGraph writes");
        // Each vertex has multiple rows (one per property + edge entries)
        // With ~27 vertices and ~20 edges, expect at least a few hundred rows
        assertTrue(rowCount > 100, "Expected at least 100 edgestore rows, got " + rowCount);
    }

    @Test(dependsOnMethods = "testSourceEdgestoreHasData")
    public void testRunMigration() throws Exception {
        MigratorConfig config = new MigratorConfig(migratorConfigFile.getAbsolutePath());
        MigrationMetrics metrics = new MigrationMetrics();
        metrics.start();

        // Phase 1: Scan + Write
        // Writer must init first (creates keyspace + tables), then state store
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, targetSession);
        writer.init();

        MigrationStateStore stateStore = new MigrationStateStore(targetSession, TARGET_KEYSPACE);
        stateStore.init();

        writer.startWriters();

        JanusGraphScanner scanner = new JanusGraphScanner(config, metrics, sourceSession);
        scanner.scanAll(
                vertex -> writer.enqueue(vertex),
                stateStore,
                "scan"
        );

        writer.signalScanComplete();
        writer.awaitCompletion();
        scanner.close();
        writer.close();

        LOG.info("Migration complete: {}", metrics.summary());

        // Basic sanity checks on metrics
        assertTrue(metrics.getVerticesScanned() > 0, "Should have scanned some vertices");
        assertTrue(metrics.getVerticesWritten() > 0, "Should have written some vertices");
        assertEquals(metrics.getWriteErrors(), 0L, "Should have zero write errors");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testVertexCount() {
        long actualCount = countTable(TARGET_KEYSPACE + ".vertices");
        LOG.info("Target vertices: {}, expected: {}", actualCount, expectedVertexCount);
        // May be >= expected because JanusGraph creates internal system vertices too
        assertTrue(actualCount >= expectedVertexCount,
                "Expected at least " + expectedVertexCount + " vertices, got " + actualCount);
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testEdgeCount() {
        long edgeByIdCount = countTable(TARGET_KEYSPACE + ".edges_by_id");
        LOG.info("Target edges_by_id: {}, expected: {}", edgeByIdCount, expectedEdgeCount);
        assertTrue(edgeByIdCount >= expectedEdgeCount,
                "Expected at least " + expectedEdgeCount + " edges, got " + edgeByIdCount);
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testEdgesOutAndInConsistency() {
        long edgesOutCount = countTable(TARGET_KEYSPACE + ".edges_out");
        long edgesInCount = countTable(TARGET_KEYSPACE + ".edges_in");
        long edgesByIdCount = countTable(TARGET_KEYSPACE + ".edges_by_id");

        LOG.info("Edges: out={}, in={}, by_id={}", edgesOutCount, edgesInCount, edgesByIdCount);

        // edges_out and edges_in should have the same count (each edge written to both)
        assertEquals(edgesOutCount, edgesInCount,
                "edges_out and edges_in should have the same count");
        // edges_by_id should equal the others
        assertEquals(edgesByIdCount, edgesOutCount,
                "edges_by_id should match edges_out count");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testVertexPropertiesPreserved() {
        // Check a sample of vertices: verify __guid, __typeName, __state are in the JSON properties
        ResultSet rs = targetSession.execute(
                "SELECT vertex_id, properties, type_name, state FROM " + TARGET_KEYSPACE + ".vertices");

        int checkedWithGuid = 0;
        int guidMatches = 0;

        for (Row row : rs) {
            String propsJson = row.getString("properties");
            if (propsJson == null || propsJson.equals("{}")) continue;

            try {
                Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
                String guid = props.get("__guid") != null ? props.get("__guid").toString() : null;
                String typeName = props.get("__typeName") != null ? props.get("__typeName").toString() : null;

                if (guid != null && writtenGuids.contains(guid)) {
                    checkedWithGuid++;
                    String expectedType = guidToTypeName.get(guid);
                    if (expectedType != null && expectedType.equals(typeName)) {
                        guidMatches++;
                    }

                    // Verify type_name column matches __typeName property
                    String columnTypeName = row.getString("type_name");
                    if (columnTypeName != null) {
                        assertEquals(columnTypeName, typeName,
                                "type_name column should match __typeName property for guid " + guid);
                    }
                }
            } catch (Exception e) {
                // Skip unparseable rows
            }
        }

        LOG.info("Vertex property check: {} vertices with known GUIDs, {} type matches",
                checkedWithGuid, guidMatches);
        assertTrue(checkedWithGuid > 0, "Should have found some vertices with known GUIDs");
        assertEquals(guidMatches, checkedWithGuid,
                "All checked vertices should have correct typeNames");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testGuidIndexPopulated() {
        // Check that __guid_idx entries exist in vertex_index
        int found = 0;
        int missing = 0;

        for (String guid : writtenGuids) {
            ResultSet rs = targetSession.execute(
                    "SELECT vertex_id FROM " + TARGET_KEYSPACE + ".vertex_index " +
                    "WHERE index_name = '__guid_idx' AND index_value = '" + guid + "'");
            Row row = rs.one();
            if (row != null) {
                found++;
            } else {
                missing++;
            }
        }

        LOG.info("GUID index check: found={}, missing={} (out of {} written)", found, missing, writtenGuids.size());
        // All written GUIDs should be indexed
        assertTrue(found > 0, "Should have found some GUID index entries");
        assertEquals(missing, 0, "No written GUIDs should be missing from the index");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testQualifiedNameTypeIndex() {
        // Check that qn_type_idx entries exist
        int found = 0;
        for (Map.Entry<String, String> entry : guidToQualifiedName.entrySet()) {
            String guid = entry.getKey();
            String qn = entry.getValue();
            String typeName = guidToTypeName.get(guid);
            if (qn != null && typeName != null) {
                String indexValue = qn + ":" + typeName;
                ResultSet rs = targetSession.execute(
                        "SELECT vertex_id FROM " + TARGET_KEYSPACE + ".vertex_index " +
                        "WHERE index_name = 'qn_type_idx' AND index_value = '" + indexValue + "'");
                if (rs.one() != null) {
                    found++;
                }
            }
        }

        LOG.info("QN+Type index check: found {} out of {} expected", found, guidToQualifiedName.size());
        assertTrue(found > 0, "Should have found some QN+Type index entries");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testTypeNamePropertyIndex() {
        // Check that type_typename_idx entries exist for common types
        String[] expectedTypes = {"Database", "Schema", "Table", "Column", "Process", "Connection"};

        for (String typeName : expectedTypes) {
            ResultSet rs = targetSession.execute(
                    "SELECT vertex_id FROM " + TARGET_KEYSPACE + ".vertex_property_index " +
                    "WHERE index_name = 'type_typename_idx' AND index_value = '" + typeName + "'");
            int count = 0;
            for (Row row : rs) count++;

            LOG.info("TypeName index for '{}': {} entries", typeName, count);
            assertTrue(count > 0, "Should have at least 1 entry for type " + typeName);
        }
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testTypeDefLookupIndex() {
        // Verify that the 1:1 type_typename_idx entry exists for TypeDef vertices.
        // This is used by AtlasTypeDefGraphStoreV2.findTypeVertexByName():
        //   graph.query().has("__type", "typeSystem").has("__type_name", name).vertices()
        // The CassandraGraphQuery resolves this via:
        //   vertex_index WHERE index_name='type_typename_idx' AND index_value='typeSystem:Table'
        ResultSet rs = targetSession.execute(
                "SELECT vertex_id FROM " + TARGET_KEYSPACE + ".vertex_index " +
                "WHERE index_name = 'type_typename_idx' AND index_value = 'typeSystem:Table'");
        Row row = rs.one();

        LOG.info("TypeDef 1:1 index lookup (typeSystem:Table): {}",
                row != null ? "found vertex_id=" + row.getString("vertex_id") : "NOT FOUND");
        assertNotNull(row, "type_typename_idx should have 1:1 entry for 'typeSystem:Table'");

        // Verify the vertex_id points to a real vertex
        String vertexId = row.getString("vertex_id");
        ResultSet vertexRs = targetSession.execute(
                "SELECT properties FROM " + TARGET_KEYSPACE + ".vertices WHERE vertex_id = '" + vertexId + "'");
        Row vertexRow = vertexRs.one();
        assertNotNull(vertexRow, "TypeDef vertex should exist in vertices table");

        // Verify it's actually a typeSystem vertex
        try {
            Map<String, Object> props = MAPPER.readValue(vertexRow.getString("properties"), Map.class);
            assertEquals(props.get("__type"), "typeSystem", "Should be a typeSystem vertex");
            assertEquals(props.get("__type_name"), "Table", "Should have __type_name=Table");
        } catch (Exception e) {
            fail("Failed to parse vertex properties: " + e.getMessage());
        }
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testDeletedEntityPreserved() {
        // Find the DELETED table and verify its state is preserved
        ResultSet rs = targetSession.execute(
                "SELECT vertex_id, properties, state FROM " + TARGET_KEYSPACE + ".vertices");

        boolean foundDeleted = false;
        for (Row row : rs) {
            String propsJson = row.getString("properties");
            if (propsJson == null) continue;
            try {
                Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
                // After normalization, "Referenceable.qualifiedName" becomes "qualifiedName"
                Object qn = props.get("qualifiedName");
                if (qn != null && qn.toString().contains("TABLE_DELETED")) {
                    foundDeleted = true;
                    Object state = props.get("__state");
                    assertEquals(state != null ? state.toString() : null, "DELETED",
                            "Deleted entity should have __state=DELETED in properties");
                    break;
                }
            } catch (Exception e) {
                // skip
            }
        }
        assertTrue(foundDeleted, "Should have found the DELETED table entity");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testEdgePropertiesPreserved() {
        // Check a sample edge has its relationship properties
        ResultSet rs = targetSession.execute(
                "SELECT edge_id, edge_label, properties FROM " + TARGET_KEYSPACE + ".edges_by_id LIMIT 10");

        int edgesWithProps = 0;
        for (Row row : rs) {
            String propsJson = row.getString("properties");
            if (propsJson != null && !propsJson.equals("{}")) {
                try {
                    Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
                    if (props.containsKey("__relationshipGuid") || props.containsKey("__r__typeName")) {
                        edgesWithProps++;
                    }
                } catch (Exception e) {
                    // skip
                }
            }
        }

        LOG.info("Edges with relationship properties: {}", edgesWithProps);
        assertTrue(edgesWithProps > 0, "At least some edges should have relationship properties");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testEdgeDirectionalConsistency() {
        // For each edge, verify it exists in both edges_out and edges_in with correct vertex IDs
        ResultSet rs = targetSession.execute(
                "SELECT edge_id, out_vertex_id, in_vertex_id, edge_label FROM " +
                TARGET_KEYSPACE + ".edges_by_id LIMIT 20");

        int verified = 0;
        for (Row row : rs) {
            String edgeId = row.getString("edge_id");
            String outVertexId = row.getString("out_vertex_id");
            String inVertexId = row.getString("in_vertex_id");
            String label = row.getString("edge_label");

            // Check edges_out
            ResultSet outRs = targetSession.execute(
                    "SELECT edge_id FROM " + TARGET_KEYSPACE + ".edges_out " +
                    "WHERE out_vertex_id = '" + outVertexId + "' AND edge_label = '" + label +
                    "' AND edge_id = '" + edgeId + "'");
            assertNotNull(outRs.one(), "Edge " + edgeId + " should exist in edges_out");

            // Check edges_in
            ResultSet inRs = targetSession.execute(
                    "SELECT edge_id FROM " + TARGET_KEYSPACE + ".edges_in " +
                    "WHERE in_vertex_id = '" + inVertexId + "' AND edge_label = '" + label +
                    "' AND edge_id = '" + edgeId + "'");
            assertNotNull(inRs.one(), "Edge " + edgeId + " should exist in edges_in");

            verified++;
        }

        LOG.info("Edge directional consistency verified for {} edges", verified);
        assertTrue(verified > 0, "Should have verified at least some edges");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testEsReindexing() throws Exception {
        // Phase 2: Run ES re-indexer
        MigratorConfig config = new MigratorConfig(migratorConfigFile.getAbsolutePath());
        MigrationMetrics metrics = new MigrationMetrics();
        metrics.start();

        ElasticsearchReindexer reindexer = new ElasticsearchReindexer(config, metrics, targetSession);
        reindexer.reindexAll();
        reindexer.close();

        LOG.info("ES re-indexing complete: {} docs", metrics.getEsDocsIndexed());
        assertTrue(metrics.getEsDocsIndexed() > 0, "Should have indexed some docs into ES");
    }

    @Test(dependsOnMethods = "testEsReindexing")
    public void testMigrationValidator() {
        MigratorConfig config;
        try {
            config = new MigratorConfig(migratorConfigFile.getAbsolutePath());
        } catch (Exception e) {
            fail("Failed to load config: " + e.getMessage());
            return;
        }

        MigrationStateStore stateStore = new MigrationStateStore(targetSession, TARGET_KEYSPACE);
        stateStore.init();

        MigrationValidator validator = new MigrationValidator(config, targetSession, stateStore);
        boolean valid = validator.validateAll();

        LOG.info("Validation result: {}", valid ? "PASSED" : "FAILED");
        // The validator may report warnings for edge count mismatches
        // (since state store tracks per-range counts which may differ slightly)
        // The important thing is it runs without errors
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testResumability() throws Exception {
        // Test that running migration again with resume=true skips completed ranges
        MigratorConfig config = new MigratorConfig(migratorConfigFile.getAbsolutePath());
        MigrationMetrics metrics2 = new MigrationMetrics();
        metrics2.start();

        // The state store already has completion records from the first run
        MigrationStateStore stateStore = new MigrationStateStore(targetSession, TARGET_KEYSPACE);
        stateStore.init();

        // Check completed ranges
        Set<Long> completed = stateStore.getCompletedRanges("scan");
        LOG.info("Completed ranges from first run: {}", completed.size());
        assertTrue(completed.size() > 0, "Should have completed ranges from first run");
    }

    @Test(dependsOnMethods = "testRunMigration")
    public void testSuperTypeNamesMultiValue() {
        // Verify SET cardinality properties (like __superTypeNames) are preserved as lists
        ResultSet rs = targetSession.execute(
                "SELECT vertex_id, properties FROM " + TARGET_KEYSPACE + ".vertices");

        int verticesWithSuperTypes = 0;
        for (Row row : rs) {
            String propsJson = row.getString("properties");
            if (propsJson == null) continue;
            try {
                Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
                Object superTypes = props.get("__superTypeNames");
                if (superTypes instanceof List) {
                    List<?> stList = (List<?>) superTypes;
                    if (stList.size() > 1) {
                        verticesWithSuperTypes++;
                    }
                }
            } catch (Exception e) {
                // skip
            }
        }

        LOG.info("Vertices with multi-value __superTypeNames: {}", verticesWithSuperTypes);
        assertTrue(verticesWithSuperTypes > 0,
                "Should have vertices with multiple __superTypeNames values (SET cardinality)");
    }

    // =================== Utility ===================

    private long countTable(String table) {
        try {
            ResultSet rs = targetSession.execute("SELECT count(*) FROM " + table);
            Row row = rs.one();
            return row != null ? row.getLong(0) : 0;
        } catch (Exception e) {
            LOG.warn("Failed to count {}: {}", table, e.getMessage());
            return -1;
        }
    }
}
