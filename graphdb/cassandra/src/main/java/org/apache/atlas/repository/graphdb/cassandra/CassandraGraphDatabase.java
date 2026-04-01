package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra+Elasticsearch implementation of GraphDatabase.
 * This replaces JanusGraph's GraphDatabase implementation.
 *
 * Configure via atlas-application.properties:
 *   atlas.graphdb.backend=cassandra
 *   atlas.GraphDatabase.impl=org.apache.atlas.repository.graphdb.cassandra.CassandraGraphDatabase
 *   atlas.cassandra.graph.hostname=localhost
 *   atlas.cassandra.graph.port=9042
 *   atlas.cassandra.graph.keyspace=atlas_graph
 *   atlas.cassandra.graph.datacenter=datacenter1
 */
public class CassandraGraphDatabase implements GraphDatabase<CassandraVertex, CassandraEdge> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraphDatabase.class);

    private static volatile CassandraGraph graphInstance;
    private static volatile boolean esIndexesVerified = false;

    @Override
    public boolean isGraphLoaded() {
        return graphInstance != null;
    }

    @Override
    public AtlasGraph<CassandraVertex, CassandraEdge> getGraph() {
        if (graphInstance == null) {
            synchronized (CassandraGraphDatabase.class) {
                if (graphInstance == null) {
                    graphInstance = createGraph();
                }
            }
        }
        ensureRequiredESIndexes();
        return graphInstance;
    }

    /**
     * Ensures the required ES indexes exist. Called lazily because during initial
     * graph creation the ES client may not yet be available (AtlasElasticsearchDatabase
     * static initializer hasn't run). On subsequent calls (e.g., when handling requests),
     * the ES client is ready and indexes will be created if missing.
     */
    private static void ensureRequiredESIndexes() {
        if (esIndexesVerified) {
            return;
        }
        try {
            CassandraGraph.ensureESIndexExists(Constants.VERTEX_INDEX_NAME);
            CassandraGraph.ensureESIndexExists(Constants.EDGE_INDEX_NAME);
            esIndexesVerified = true;
        } catch (Throwable t) {
            // ES client may not be ready yet; will retry on next call
            LOG.debug("ES indexes not yet verified (will retry): {}", t.getMessage());
        }
    }

    @Override
    public AtlasGraph<CassandraVertex, CassandraEdge> getGraphBulkLoading() {
        // For bulk loading, return the same graph instance
        // (in JanusGraph, this returns a graph with different tx settings)
        return getGraph();
    }

    @Override
    public void initializeTestGraph() {
        graphInstance = createGraph();
    }

    @Override
    public void cleanup() {
        if (graphInstance != null) {
            graphInstance.clear();
            graphInstance.shutdown();
            graphInstance = null;
        }
    }

    private CassandraGraph createGraph() {
        try {
            LOG.info("Creating CassandraGraph...");
            Configuration configuration = ApplicationProperties.get();
            CqlSession session = CassandraSessionProvider.getSession(configuration);
            RuntimeIdStrategy idStrategy = RuntimeIdStrategy.from(
                    configuration.getString("atlas.graph.id.strategy", "legacy"));
            boolean claimEnabled = configuration.getBoolean("atlas.graph.claim.enabled", false);
            LOG.info("CassandraGraph config: idStrategy={}, claimEnabled={}", idStrategy, claimEnabled);
            CassandraGraph graph = new CassandraGraph(session, idStrategy, claimEnabled);
            LOG.info("CassandraGraph created successfully.");

            // Register JVM shutdown hook for graceful cleanup
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("JVM shutdown hook: shutting down CassandraGraph gracefully");
                try {
                    graph.shutdown();
                } catch (Exception e) {
                    LOG.warn("Error during CassandraGraph shutdown: {}", e.getMessage());
                }
            }, "cassandra-graph-shutdown"));

            return graph;
        } catch (AtlasException e) {
            throw new RuntimeException("Failed to create CassandraGraph", e);
        }
    }
}
