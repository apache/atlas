package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraTargetWriter using mocked CqlSession.
 * Verifies schema creation, enqueue/dequeue behavior, and vertex/edge writing logic.
 */
public class CassandraTargetWriterTest {

    @Mock private CqlSession mockSession;
    @Mock private PreparedStatement mockPreparedStmt;
    @Mock private BoundStatement mockBoundStmt;
    @Mock private ResultSet mockResultSet;

    private MigratorConfig config;
    private MigrationMetrics metrics;
    private File tempConfigFile;
    private AutoCloseable mocks;

    @BeforeMethod
    public void setUp() throws IOException {
        mocks = MockitoAnnotations.openMocks(this);
        metrics = new MigrationMetrics();

        // Create a minimal config
        Properties props = new Properties();
        props.setProperty("target.cassandra.keyspace", "test_ks");
        props.setProperty("migration.writer.threads", "2");
        props.setProperty("migration.writer.batch.size", "10");
        props.setProperty("migration.queue.capacity", "100");

        tempConfigFile = File.createTempFile("migration-test", ".properties");
        try (FileWriter fw = new FileWriter(tempConfigFile)) {
            props.store(fw, "test");
        }
        config = new MigratorConfig(tempConfigFile.getAbsolutePath());

        // Mock session to return prepared statements
        when(mockSession.execute(anyString())).thenReturn(mockResultSet);
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStmt);
        when(mockPreparedStmt.bind(any())).thenReturn(mockBoundStmt);
        when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);
        when(mockSession.execute(any(com.datastax.oss.driver.api.core.cql.BatchStatement.class))).thenReturn(mockResultSet);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (tempConfigFile != null && tempConfigFile.exists()) {
            tempConfigFile.delete();
        }
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    public void testInitCreatesSchemaAndPreparesStatements() {
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, mockSession);
        writer.init();

        // Verify schema creation: keyspace + 7 tables
        verify(mockSession, atLeast(8)).execute(anyString());

        // Verify prepared statements: 6 statements
        verify(mockSession, times(6)).prepare(anyString());

        writer.close();
    }

    @Test
    public void testSchemaCreatesAllTables() {
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, mockSession);
        writer.init();

        // Verify specific CQL statements were executed
        verify(mockSession).execute(contains("CREATE KEYSPACE IF NOT EXISTS test_ks"));
        verify(mockSession).execute(contains("CREATE TABLE IF NOT EXISTS test_ks.vertices"));
        verify(mockSession).execute(contains("CREATE TABLE IF NOT EXISTS test_ks.edges_out"));
        verify(mockSession).execute(contains("CREATE TABLE IF NOT EXISTS test_ks.edges_in"));
        verify(mockSession).execute(contains("CREATE TABLE IF NOT EXISTS test_ks.edges_by_id"));
        verify(mockSession).execute(contains("CREATE TABLE IF NOT EXISTS test_ks.vertex_index"));
        verify(mockSession).execute(contains("CREATE TABLE IF NOT EXISTS test_ks.vertex_property_index"));
        verify(mockSession).execute(contains("CREATE TABLE IF NOT EXISTS test_ks.schema_registry"));

        writer.close();
    }

    @Test
    public void testEnqueueAndSignalComplete() throws InterruptedException {
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, mockSession);
        writer.init();
        writer.startWriters();

        // Enqueue a vertex
        DecodedVertex vertex = new DecodedVertex(12345L);
        vertex.addProperty("__typeName", "Table");
        vertex.addProperty("__guid", "guid-123");
        vertex.addProperty("__state", "ACTIVE");

        writer.enqueue(vertex);

        // Signal completion and wait
        writer.signalScanComplete();
        writer.awaitCompletion();
        writer.close();

        // Vertex should have been written (vertex stmt executed)
        verify(mockSession, atLeastOnce()).execute(any(BoundStatement.class));
    }

    @Test
    public void testVertexWithEdgesWritten() throws InterruptedException {
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, mockSession);
        writer.init();
        writer.startWriters();

        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("__typeName", "Process");
        vertex.addProperty("__guid", "guid-proc");
        vertex.addProperty("__state", "ACTIVE");

        DecodedEdge edge = new DecodedEdge(100L, 1L, 2L, "__Process.inputs");
        edge.addProperty("__state", "ACTIVE");
        vertex.addOutEdge(edge);

        writer.enqueue(vertex);
        writer.signalScanComplete();
        writer.awaitCompletion();
        writer.close();

        // Should have written vertex + edge batch + indexes
        verify(mockSession, atLeastOnce()).execute(any(BoundStatement.class));
        verify(mockSession, atLeastOnce()).execute(any(com.datastax.oss.driver.api.core.cql.BatchStatement.class));
    }

    @Test
    public void testMultipleVerticesProcessed() throws InterruptedException {
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, mockSession);
        writer.init();
        writer.startWriters();

        for (int i = 0; i < 20; i++) {
            DecodedVertex vertex = new DecodedVertex(i);
            vertex.addProperty("__typeName", "Table");
            vertex.addProperty("__guid", "guid-" + i);
            vertex.addProperty("__state", "ACTIVE");
            writer.enqueue(vertex);
        }

        writer.signalScanComplete();
        writer.awaitCompletion();
        writer.close();

        // All 20 vertices should have been written
        assertEquals(metrics.getVerticesWritten(), 20L);
    }

    @Test
    public void testMetricsTrackedCorrectly() throws InterruptedException {
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, mockSession);
        writer.init();
        writer.startWriters();

        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("__typeName", "Table");
        vertex.addProperty("__guid", "guid-1");
        vertex.addProperty("__state", "ACTIVE");
        vertex.addProperty("Referenceable.qualifiedName", "db.schema.table1");

        vertex.addOutEdge(new DecodedEdge(100L, 1L, 2L, "edge1"));
        vertex.addOutEdge(new DecodedEdge(101L, 1L, 3L, "edge2"));

        writer.enqueue(vertex);
        writer.signalScanComplete();
        writer.awaitCompletion();
        writer.close();

        assertEquals(metrics.getVerticesWritten(), 1L);
        assertEquals(metrics.getEdgesWritten(), 2L);
        // Indexes: __guid_idx + qn_type_idx + type_typename_idx = 3
        assertEquals(metrics.getDecodeErrors(), 0L);
    }

    @Test
    public void testTypeDefVertexIndexWritten() throws InterruptedException {
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, mockSession);
        writer.init();
        writer.startWriters();

        // Create a TypeDef vertex (like AtlasTypeDefGraphStoreV2.createTypeVertex())
        DecodedVertex typeDefVertex = new DecodedVertex(999L);
        typeDefVertex.addProperty("__guid", "typedef-guid-1");
        typeDefVertex.addProperty("__typeName", "Table");
        typeDefVertex.addProperty("__type", "typeSystem");
        typeDefVertex.addProperty("__type_name", "Table");
        typeDefVertex.addProperty("__type_category", 1);
        typeDefVertex.addProperty("__state", "ACTIVE");

        writer.enqueue(typeDefVertex);
        writer.signalScanComplete();
        writer.awaitCompletion();
        writer.close();

        // Verify that __guid_idx, type_typename_idx (1:1), type_category_idx (1:N) were all written
        // With mocks, we can verify the insertIndexStmt was bound with the correct values
        assertEquals(metrics.getVerticesWritten(), 1L);
        // Indexes: __guid_idx + type_typename_idx(1:N for typeName) + type_category_idx(1:N)
        //          + type_typename_idx(1:1 for typeSystem:Table) = 4
        assertTrue(metrics.getIndexesWritten() >= 3,
                "TypeDef vertex should generate at least 3 index entries (guid + type_typename 1:N + category), got "
                + metrics.getIndexesWritten());
    }

    @Test
    public void testCloseIsIdempotent() {
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, mockSession);
        writer.close();
        writer.close(); // Should not throw
    }
}
