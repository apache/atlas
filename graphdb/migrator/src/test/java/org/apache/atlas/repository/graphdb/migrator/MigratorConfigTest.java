package org.apache.atlas.repository.graphdb.migrator;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.*;

public class MigratorConfigTest {

    private File tempFile;

    @AfterMethod
    public void cleanup() {
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    private File createTempConfig(Properties props) throws IOException {
        tempFile = File.createTempFile("migration-test", ".properties");
        try (FileWriter fw = new FileWriter(tempFile)) {
            props.store(fw, "test config");
        }
        return tempFile;
    }

    @Test
    public void testDefaultValues() throws IOException {
        Properties props = new Properties();
        File configFile = createTempConfig(props);
        MigratorConfig config = new MigratorConfig(configFile.getAbsolutePath());

        // Source defaults
        assertEquals(config.getSourceCassandraHostname(), "localhost");
        assertEquals(config.getSourceCassandraPort(), 9042);
        assertEquals(config.getSourceCassandraKeyspace(), "atlas_janus");
        assertEquals(config.getSourceCassandraDatacenter(), "datacenter1");
        assertEquals(config.getSourceCassandraUsername(), "");
        assertEquals(config.getSourceCassandraPassword(), "");
        assertEquals(config.getSourceEdgestoreTable(), "edgestore");

        // Target defaults
        assertEquals(config.getTargetCassandraHostname(), "localhost");
        assertEquals(config.getTargetCassandraPort(), 9042);
        assertEquals(config.getTargetCassandraKeyspace(), "atlas_graph");
        assertEquals(config.getTargetCassandraDatacenter(), "datacenter1");
        assertEquals(config.getTargetCassandraUsername(), "");
        assertEquals(config.getTargetCassandraPassword(), "");

        // ES defaults
        assertEquals(config.getTargetEsHostname(), "localhost");
        assertEquals(config.getTargetEsPort(), 9200);
        assertEquals(config.getTargetEsProtocol(), "http");
        assertEquals(config.getTargetEsIndex(), "janusgraph_vertex_index");
        assertEquals(config.getTargetEsUsername(), "");
        assertEquals(config.getTargetEsPassword(), "");

        // Tuning defaults
        assertEquals(config.getScannerThreads(), 32);
        assertEquals(config.getWriterThreads(), 8);
        assertEquals(config.getWriterBatchSize(), 500);
        assertEquals(config.getEsBulkSize(), 1000);
        assertEquals(config.getScanFetchSize(), 5000);
        assertEquals(config.getQueueCapacity(), 10000);
        assertTrue(config.isResume());
    }

    @Test
    public void testCustomSourceConfig() throws IOException {
        Properties props = new Properties();
        props.setProperty("source.janusgraph.config", "/opt/atlas/conf/atlas-application.properties");
        props.setProperty("source.cassandra.hostname", "cass-source.example.com");
        props.setProperty("source.cassandra.port", "9043");
        props.setProperty("source.cassandra.keyspace", "my_janus_ks");
        props.setProperty("source.cassandra.datacenter", "us-east-1");
        props.setProperty("source.cassandra.username", "admin");
        props.setProperty("source.cassandra.password", "secret123");
        props.setProperty("source.cassandra.edgestore.table", "edgestore_custom");

        File configFile = createTempConfig(props);
        MigratorConfig config = new MigratorConfig(configFile.getAbsolutePath());

        assertEquals(config.getSourceJanusGraphConfig(), "/opt/atlas/conf/atlas-application.properties");
        assertEquals(config.getSourceCassandraHostname(), "cass-source.example.com");
        assertEquals(config.getSourceCassandraPort(), 9043);
        assertEquals(config.getSourceCassandraKeyspace(), "my_janus_ks");
        assertEquals(config.getSourceCassandraDatacenter(), "us-east-1");
        assertEquals(config.getSourceCassandraUsername(), "admin");
        assertEquals(config.getSourceCassandraPassword(), "secret123");
        assertEquals(config.getSourceEdgestoreTable(), "edgestore_custom");
    }

    @Test
    public void testCustomTargetConfig() throws IOException {
        Properties props = new Properties();
        props.setProperty("target.cassandra.hostname", "cass-target.example.com");
        props.setProperty("target.cassandra.port", "9044");
        props.setProperty("target.cassandra.keyspace", "atlas_graph_prod");
        props.setProperty("target.cassandra.datacenter", "eu-west-1");
        props.setProperty("target.cassandra.username", "target_user");
        props.setProperty("target.cassandra.password", "target_pass");

        File configFile = createTempConfig(props);
        MigratorConfig config = new MigratorConfig(configFile.getAbsolutePath());

        assertEquals(config.getTargetCassandraHostname(), "cass-target.example.com");
        assertEquals(config.getTargetCassandraPort(), 9044);
        assertEquals(config.getTargetCassandraKeyspace(), "atlas_graph_prod");
        assertEquals(config.getTargetCassandraDatacenter(), "eu-west-1");
        assertEquals(config.getTargetCassandraUsername(), "target_user");
        assertEquals(config.getTargetCassandraPassword(), "target_pass");
    }

    @Test
    public void testCustomEsConfig() throws IOException {
        Properties props = new Properties();
        props.setProperty("target.elasticsearch.hostname", "es.example.com");
        props.setProperty("target.elasticsearch.port", "443");
        props.setProperty("target.elasticsearch.protocol", "https");
        props.setProperty("target.elasticsearch.index", "atlas_vertex_index");
        props.setProperty("target.elasticsearch.username", "es_user");
        props.setProperty("target.elasticsearch.password", "es_pass");

        File configFile = createTempConfig(props);
        MigratorConfig config = new MigratorConfig(configFile.getAbsolutePath());

        assertEquals(config.getTargetEsHostname(), "es.example.com");
        assertEquals(config.getTargetEsPort(), 443);
        assertEquals(config.getTargetEsProtocol(), "https");
        assertEquals(config.getTargetEsIndex(), "atlas_vertex_index");
        assertEquals(config.getTargetEsUsername(), "es_user");
        assertEquals(config.getTargetEsPassword(), "es_pass");
    }

    @Test
    public void testCustomTuningParams() throws IOException {
        Properties props = new Properties();
        props.setProperty("migration.scanner.threads", "64");
        props.setProperty("migration.writer.threads", "16");
        props.setProperty("migration.writer.batch.size", "1000");
        props.setProperty("migration.es.bulk.size", "5000");
        props.setProperty("migration.scan.fetch.size", "10000");
        props.setProperty("migration.queue.capacity", "50000");
        props.setProperty("migration.resume", "false");

        File configFile = createTempConfig(props);
        MigratorConfig config = new MigratorConfig(configFile.getAbsolutePath());

        assertEquals(config.getScannerThreads(), 64);
        assertEquals(config.getWriterThreads(), 16);
        assertEquals(config.getWriterBatchSize(), 1000);
        assertEquals(config.getEsBulkSize(), 5000);
        assertEquals(config.getScanFetchSize(), 10000);
        assertEquals(config.getQueueCapacity(), 50000);
        assertFalse(config.isResume());
    }

    @Test(expectedExceptions = IOException.class)
    public void testMissingConfigFile() throws IOException {
        new MigratorConfig("/nonexistent/path/config.properties");
    }

    @Test
    public void testPortWithWhitespace() throws IOException {
        Properties props = new Properties();
        props.setProperty("source.cassandra.port", "  9043  ");

        File configFile = createTempConfig(props);
        MigratorConfig config = new MigratorConfig(configFile.getAbsolutePath());

        assertEquals(config.getSourceCassandraPort(), 9043);
    }

    @Test
    public void testBooleanParsing() throws IOException {
        Properties props = new Properties();
        props.setProperty("migration.resume", "true");

        File configFile = createTempConfig(props);
        MigratorConfig config = new MigratorConfig(configFile.getAbsolutePath());
        assertTrue(config.isResume());

        // Test false
        props.setProperty("migration.resume", "false");
        File configFile2 = createTempConfig(props);
        MigratorConfig config2 = new MigratorConfig(configFile2.getAbsolutePath());
        assertFalse(config2.isResume());
    }
}
