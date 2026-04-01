package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.*;

/**
 * Main entry point for the JanusGraph → Cassandra+ES migrator.
 *
 * Orchestrates three phases:
 *   Phase 1: Scan JanusGraph edgestore via CQL, decode with JG EdgeSerializer,
 *            write vertices + edges + indexes to new Cassandra schema
 *   Phase 2: Re-index all vertices into Elasticsearch
 *   Phase 3: Validate migration completeness
 *
 * Usage:
 *   java -jar atlas-graphdb-migrator.jar /path/to/migration.properties
 *
 * The migrator is designed to:
 *   - Run as a separate process (not inside Atlas pods)
 *   - Be fully resumable (tracks completed token ranges in Cassandra)
 *   - Apply backpressure (bounded queue between scanner and writer)
 *   - Report live progress metrics
 */
public class MigratorMain {

    private static final Logger LOG = LoggerFactory.getLogger(MigratorMain.class);

    private static final String PHASE_SCAN   = "scan";
    private static final String PHASE_ES     = "es_reindex";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar atlas-graphdb-migrator.jar <config-file>");
            System.err.println("       java -jar atlas-graphdb-migrator.jar <config-file> --validate-only");
            System.err.println("       java -jar atlas-graphdb-migrator.jar <config-file> --es-only");
            System.err.println("       java -jar atlas-graphdb-migrator.jar <config-file> --fresh (clears state, starts over)");
            System.exit(1);
        }

        String configPath = args[0];
        boolean validateOnly = args.length > 1 && "--validate-only".equals(args[1]);
        boolean esOnly = args.length > 1 && "--es-only".equals(args[1]);
        boolean fresh = args.length > 1 && "--fresh".equals(args[1]);

        MigratorConfig config = new MigratorConfig(configPath);
        MigrationMetrics metrics = new MigrationMetrics();

        LOG.info("=== JanusGraph → Cassandra Migrator ===");
        LOG.info("Source Cassandra: {}:{}/{}", config.getSourceCassandraHostname(),
                 config.getSourceCassandraPort(), config.getSourceCassandraKeyspace());
        LOG.info("Target Cassandra: {}:{}/{}", config.getTargetCassandraHostname(),
                 config.getTargetCassandraPort(), config.getTargetCassandraKeyspace());
        LOG.info("Target ES: {}://{}:{}/{}", config.getTargetEsProtocol(),
                 config.getTargetEsHostname(), config.getTargetEsPort(), config.getTargetEsIndex());
        LOG.info("Scanner threads: {}, Writer threads: {}, Batch size: {}",
                 config.getScannerThreads(), config.getWriterThreads(), config.getWriterBatchSize());
        LOG.info("Async writes: maxInflight/thread={}, edgesOutOnly={}, maxEdges/batch={}",
                 config.getMaxInflightPerThread(), config.isEdgesOutOnly(), config.getMaxEdgesPerBatch());
        LOG.info("Resume: {}", config.isResume());
        LOG.info("ID strategy: {}, claim enabled: {}", config.getIdStrategy(), config.isClaimEnabled());
        LOG.info("Skip flags: esReindex={}, classifications={}, tasks={}",
                 config.isSkipEsReindex(), config.isSkipClassifications(), config.isSkipTasks());

        // Open Cassandra sessions (source gets long timeout for token-range scans)
        CqlSession sourceSession = buildCqlSession(
            config.getSourceCassandraHostname(), config.getSourceCassandraPort(),
            config.getSourceCassandraDatacenter(), config.getSourceCassandraKeyspace(),
            config.getSourceCassandraUsername(), config.getSourceCassandraPassword(),
            false, Duration.ofSeconds(120));

        CqlSession targetSession = buildCqlSession(
            config.getTargetCassandraHostname(), config.getTargetCassandraPort(),
            config.getTargetCassandraDatacenter(), config.getTargetCassandraKeyspace(),
            config.getTargetCassandraUsername(), config.getTargetCassandraPassword(),
            true);

        // Create target keyspace + tables first (must happen before stateStore.init()
        // since stateStore creates its table in the same keyspace)
        CassandraTargetWriter writer = new CassandraTargetWriter(config, metrics, targetSession);
        writer.init();

        MigrationStateStore stateStore = new MigrationStateStore(targetSession, config.getTargetCassandraKeyspace());
        stateStore.init();

        if (fresh) {
            stateStore.clearState(PHASE_SCAN);
            stateStore.clearState(PHASE_ES);
        }

        try {
            if (validateOnly) {
                runValidation(config, targetSession, stateStore);
            } else if (esOnly) {
                runEsReindex(config, metrics, targetSession);
            } else {
                runFullMigration(config, metrics, sourceSession, targetSession, stateStore, writer);
            }
        } finally {
            sourceSession.close();
            targetSession.close();
        }
    }

    private static void runFullMigration(MigratorConfig config, MigrationMetrics metrics,
                                          CqlSession sourceSession, CqlSession targetSession,
                                          MigrationStateStore stateStore,
                                          CassandraTargetWriter writer) throws Exception {
        metrics.start();

        // Start progress reporter — logs every 10 seconds
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        reporter.scheduleAtFixedRate(metrics::logProgress, 10, 10, TimeUnit.SECONDS);

        try {
            // ========== Phase 1: Scan + Write ==========
            LOG.info("========================================");
            LOG.info("=== Phase 1/3: Scan JanusGraph edgestore + Write to target ===");
            LOG.info("  Source: {}.{}", config.getSourceCassandraKeyspace(), config.getSourceEdgestoreTable());
            LOG.info("  Target keyspace: {}", config.getTargetCassandraKeyspace());
            LOG.info("  Scanner threads: {}, Writer threads: {}", config.getScannerThreads(), config.getWriterThreads());
            LOG.info("========================================");

            writer.startWriters();

            LOG.info("Initializing JanusGraph schema resolution...");
            JanusGraphScanner scanner = new JanusGraphScanner(config, metrics, sourceSession);
            LOG.info("JanusGraph schema resolution ready. Starting CQL token-range scan...");

            scanner.scanAll(
                vertex -> writer.enqueue(vertex),   // Scanner → queue → writer
                stateStore,
                PHASE_SCAN
            );

            LOG.info("All token ranges scanned. Waiting for writer threads to drain queue...");
            writer.signalScanComplete();
            writer.awaitCompletion();
            scanner.close();
            writer.close();

            LOG.info("========================================");
            LOG.info("Phase 1 complete: {}", metrics.summary());
            LOG.info("========================================");

            // ========== Phase 2: ES Re-index ==========
            if (!config.isSkipEsReindex()) {
                LOG.info("========================================");
                LOG.info("=== Phase 2/3: Elasticsearch re-indexing ===");
                LOG.info("  Source: {}.vertices", config.getTargetCassandraKeyspace());
                LOG.info("  Target ES index: {}", config.getTargetEsIndex());
                LOG.info("  Bulk size: {}", config.getEsBulkSize());
                LOG.info("========================================");

                ElasticsearchReindexer esReindexer = new ElasticsearchReindexer(config, metrics, targetSession);
                esReindexer.reindexAll();
                esReindexer.close();

                LOG.info("Phase 2 complete: {} ES docs indexed", String.format("%,d", metrics.getEsDocsIndexed()));
            } else {
                LOG.info("========================================");
                LOG.info("=== Phase 2 SKIPPED (migration.skip.es.reindex=true) ===");
                LOG.info("  Existing ES index will be reused by the new graph layer");
                LOG.info("========================================");
            }

            // ========== Phase 3: Validation ==========
            LOG.info("========================================");
            LOG.info("=== Phase 3/3: Validation ===");
            LOG.info("========================================");

            MigrationValidator validator = new MigrationValidator(config, targetSession, stateStore);
            boolean valid = validator.validateAll();

            // Final summary
            LOG.info("========================================");
            LOG.info("========================================");
            LOG.info("  {}", metrics.summary());
            LOG.info("  Validation: {}", valid ? "PASSED" : "FAILED — review warnings above");
            LOG.info("========================================");
            LOG.info("========================================");

            if (!valid) {
                LOG.warn("Migration completed with validation warnings. Review logs before cutover.");
            }
        } finally {
            reporter.shutdownNow();
        }
    }

    private static void runEsReindex(MigratorConfig config, MigrationMetrics metrics,
                                      CqlSession targetSession) throws IOException {
        metrics.start();
        LOG.info("=== ES Re-index Only Mode ===");

        ElasticsearchReindexer reindexer = new ElasticsearchReindexer(config, metrics, targetSession);
        reindexer.reindexAll();
        reindexer.close();

        LOG.info("ES re-indexing complete: {} docs", metrics.getEsDocsIndexed());
    }

    private static void runValidation(MigratorConfig config, CqlSession targetSession,
                                       MigrationStateStore stateStore) {
        LOG.info("=== Validation Only Mode ===");

        MigrationValidator validator = new MigrationValidator(config, targetSession, stateStore);
        boolean valid = validator.validateAll();

        LOG.info("Validation: {}", valid ? "PASSED" : "FAILED");
    }

    /**
     * Build a CqlSession. Does NOT specify keyspace at session level so we can
     * create keyspaces and query across them.
     *
     * @param tuneForWrites  if true, configure connection pool for high-throughput async writes:
     *                       4 connections per local node, 1024 max concurrent requests per connection
     */
    private static CqlSession buildCqlSession(String hostname, int port, String datacenter,
                                               String keyspace, String username, String password) {
        return buildCqlSession(hostname, port, datacenter, keyspace, username, password, false, null);
    }

    private static CqlSession buildCqlSession(String hostname, int port, String datacenter,
                                               String keyspace, String username, String password,
                                               boolean tuneForWrites) {
        return buildCqlSession(hostname, port, datacenter, keyspace, username, password, tuneForWrites, null);
    }

    private static CqlSession buildCqlSession(String hostname, int port, String datacenter,
                                               String keyspace, String username, String password,
                                               boolean tuneForWrites, Duration requestTimeout) {
        CqlSessionBuilder builder = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(hostname, port))
            .withLocalDatacenter(datacenter);

        if (username != null && !username.isEmpty()) {
            builder.withAuthCredentials(username, password);
        }

        // Apply driver config if any tuning is needed
        if (tuneForWrites || requestTimeout != null) {
            var configBuilder = DriverConfigLoader.programmaticBuilder();

            if (tuneForWrites) {
                // 3 nodes × 4 connections × 1024 requests = 12,288 max concurrent requests
                configBuilder
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
                    .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 2)
                    .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 1024);
                LOG.info("Session tuned for writes: 4 connections/node, 1024 max requests/connection");
            }

            if (requestTimeout != null) {
                configBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, requestTimeout);
                LOG.info("Session request timeout: {}s", requestTimeout.getSeconds());
            } else if (tuneForWrites) {
                configBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30));
            }

            builder.withConfigLoader(configBuilder.build());
        }

        CqlSession session = builder.build();
        LOG.info("Connected to Cassandra at {}:{} (datacenter: {})", hostname, port, datacenter);
        return session;
    }
}
