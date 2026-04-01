package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class CassandraSessionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSessionProvider.class);

    private static final String CONFIG_PREFIX    = "atlas.cassandra.graph.";
    private static final String DEFAULT_KEYSPACE = "atlas_graph";
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int    DEFAULT_PORT     = 9042;
    private static final String DEFAULT_DC       = "datacenter1";

    private static volatile CqlSession session;

    // Shared non-keyspace-bound session for use by TagDAO, ConfigDAO, etc.
    // Avoids creating 3 separate Cassandra connections on startup.
    private static volatile CqlSession sharedSession;
    private static volatile String configuredHostname;
    private static volatile int configuredPort;
    private static volatile String configuredDc;

    public static CqlSession getSession(Configuration configuration) {
        if (session == null || session.isClosed()) {
            synchronized (CassandraSessionProvider.class) {
                if (session == null || session.isClosed()) {
                    session = createSession(configuration);
                }
            }
        }
        return session;
    }

    /**
     * Returns a shared non-keyspace-bound CqlSession, reusing the same contact point
     * and datacenter as the graph session. This avoids creating multiple Cassandra
     * connection pools during startup (TagDAO, ConfigDAO, etc.).
     *
     * Falls back to creating its own session if the graph session hasn't been initialized yet.
     */
    public static CqlSession getSharedSession(String hostname, int port, String datacenter) {
        if (sharedSession == null || sharedSession.isClosed()) {
            synchronized (CassandraSessionProvider.class) {
                if (sharedSession == null || sharedSession.isClosed()) {
                    // Use configured values from graph session if available, otherwise use provided values
                    String host = configuredHostname != null ? configuredHostname : hostname;
                    int p = configuredPort > 0 ? configuredPort : port;
                    String dc = configuredDc != null ? configuredDc : datacenter;

                    LOG.info("Creating shared (non-keyspace) Cassandra session: host={}, port={}, dc={}", host, p, dc);
                    sharedSession = CqlSession.builder()
                            .addContactPoint(new InetSocketAddress(host, p))
                            .withLocalDatacenter(dc)
                            .build();
                    LOG.info("Shared Cassandra session created successfully");
                }
            }
        }
        return sharedSession;
    }

    private static CqlSession createSession(Configuration configuration) {
        String hostname = configuration.getString(CONFIG_PREFIX + "hostname", DEFAULT_HOSTNAME);
        int    port     = configuration.getInt(CONFIG_PREFIX + "port", DEFAULT_PORT);
        String keyspace = configuration.getString(CONFIG_PREFIX + "keyspace", DEFAULT_KEYSPACE);
        String dc       = configuration.getString(CONFIG_PREFIX + "datacenter", DEFAULT_DC);

        // Store connection config for shared session reuse by TagDAO, ConfigDAO, etc.
        configuredHostname = hostname;
        configuredPort = port;
        configuredDc = dc;

        int    rf       = configuration.getInt(CONFIG_PREFIX + "replication.factor", 1);
        String strategy = configuration.getString(CONFIG_PREFIX + "replication.strategy", "SimpleStrategy");

        LOG.info("Initializing Cassandra session: host={}, port={}, keyspace={}, dc={}, rf={}, strategy={}",
                hostname, port, keyspace, dc, rf, strategy);

        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, port))
                .withLocalDatacenter(dc);

        CqlSession initSession = builder.build();

        // Create keyspace if it doesn't exist
        String replicationConfig;
        if ("NetworkTopologyStrategy".equalsIgnoreCase(strategy)) {
            replicationConfig = "{'class': 'NetworkTopologyStrategy', '" + dc + "': " + rf + "}";
        } else {
            replicationConfig = "{'class': 'SimpleStrategy', 'replication_factor': " + rf + "}";
        }
        initSession.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
            " WITH replication = " + replicationConfig
        );
        initSession.close();

        // Reconnect with keyspace
        CqlSession keyspaceSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname, port))
                .withLocalDatacenter(dc)
                .withKeyspace(keyspace)
                .build();

        try {
            createTables(keyspaceSession);
        } catch (Exception e) {
            LOG.error("Failed to create Cassandra tables, closing session: {}", e.getMessage(), e);
            keyspaceSession.close();
            throw e;
        }

        LOG.info("Cassandra session initialized successfully for keyspace: {}", keyspace);

        return keyspaceSession;
    }

    private static void createTables(CqlSession session) {
        // Vertex table: stores all vertex properties as a JSON blob
        session.execute(
            "CREATE TABLE IF NOT EXISTS vertices (" +
            "  vertex_id text PRIMARY KEY," +
            "  properties text," +
            "  vertex_label text," +
            "  type_name text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp" +
            ")"
        );

        // Edges by out-vertex (for outgoing traversals)
        session.execute(
            "CREATE TABLE IF NOT EXISTS edges_out (" +
            "  out_vertex_id text," +
            "  edge_label text," +
            "  edge_id text," +
            "  in_vertex_id text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp," +
            "  PRIMARY KEY ((out_vertex_id), edge_label, edge_id)" +
            ") WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC)"
        );

        // Edges by in-vertex (for incoming traversals)
        session.execute(
            "CREATE TABLE IF NOT EXISTS edges_in (" +
            "  in_vertex_id text," +
            "  edge_label text," +
            "  edge_id text," +
            "  out_vertex_id text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp," +
            "  PRIMARY KEY ((in_vertex_id), edge_label, edge_id)" +
            ") WITH CLUSTERING ORDER BY (edge_label ASC, edge_id ASC)"
        );

        // Edge lookup by ID
        session.execute(
            "CREATE TABLE IF NOT EXISTS edges_by_id (" +
            "  edge_id text PRIMARY KEY," +
            "  out_vertex_id text," +
            "  in_vertex_id text," +
            "  edge_label text," +
            "  properties text," +
            "  state text," +
            "  created_at timestamp," +
            "  modified_at timestamp" +
            ")"
        );

        // Composite index simulation table (1:1 unique lookups)
        session.execute(
            "CREATE TABLE IF NOT EXISTS vertex_index (" +
            "  index_name text," +
            "  index_value text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((index_name, index_value))" +
            ")"
        );

        // Edge property index table (1:1 lookups, e.g., relationship GUID → edge_id)
        session.execute(
            "CREATE TABLE IF NOT EXISTS edge_index (" +
            "  index_name text," +
            "  index_value text," +
            "  edge_id text," +
            "  PRIMARY KEY ((index_name, index_value))" +
            ")"
        );

        // Property index table (1:N lookups - multiple vertices per index key)
        session.execute(
            "CREATE TABLE IF NOT EXISTS vertex_property_index (" +
            "  index_name text," +
            "  index_value text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((index_name, index_value), vertex_id)" +
            ")"
        );

        // Identity claims table: enforces one winner vertex_id per logical entity identity key
        session.execute(
            "CREATE TABLE IF NOT EXISTS entity_claims (" +
            "  identity_key text PRIMARY KEY," +
            "  vertex_id text," +
            "  claimed_at timestamp," +
            "  source text" +
            ")"
        );

        // Schema registry for property keys
        session.execute(
            "CREATE TABLE IF NOT EXISTS schema_registry (" +
            "  property_name text PRIMARY KEY," +
            "  property_class text," +
            "  cardinality text," +
            "  created_at timestamp" +
            ")"
        );

        // Dedicated TypeDef storage: fast primary-key lookup by type_name
        session.execute(
            "CREATE TABLE IF NOT EXISTS type_definitions (" +
            "  type_name text PRIMARY KEY," +
            "  type_category text," +
            "  vertex_id text," +
            "  created_at timestamp," +
            "  modified_at timestamp" +
            ")"
        );

        // TypeDef lookup by category (1:N — e.g. all ENTITY typedefs)
        session.execute(
            "CREATE TABLE IF NOT EXISTS type_definitions_by_category (" +
            "  type_category text," +
            "  type_name text," +
            "  vertex_id text," +
            "  PRIMARY KEY ((type_category), type_name)" +
            ") WITH CLUSTERING ORDER BY (type_name ASC)"
        );

        // ES outbox: tracks vertices that need ES sync. Partitioned by status so that
        // "SELECT ... WHERE status = 'PENDING'" is a direct partition scan (no ALLOW FILTERING).
        // This scales to millions of rows — scan cost is O(PENDING) not O(total).
        // gc_grace_seconds=3600 (1 hour) keeps tombstones from completed entries short-lived.
        // All rows are written with a 7-day TTL for auto-cleanup of stuck entries.
        session.execute(
            "CREATE TABLE IF NOT EXISTS es_outbox (" +
            "  status text," +
            "  vertex_id text," +
            "  es_action text," +
            "  properties_json text," +
            "  attempt_count int," +
            "  created_at timestamp," +
            "  last_attempted_at timestamp," +
            "  PRIMARY KEY ((status), vertex_id)" +
            ") WITH gc_grace_seconds = 3600"
        );

        // Distributed lease coordination: background jobs use LWT INSERT IF NOT EXISTS
        // with TTL to acquire exclusive leases. Crashed pods auto-release via TTL expiry.
        session.execute(
            "CREATE TABLE IF NOT EXISTS job_leases (" +
            "  job_name text PRIMARY KEY," +
            "  owner text," +
            "  acquired_at timestamp" +
            ")"
        );

        // Cursor tracking for progressive background scans. Stores the last processed
        // Cassandra token so that orphan cleanup can resume after crash or restart.
        session.execute(
            "CREATE TABLE IF NOT EXISTS repair_progress (" +
            "  job_name text PRIMARY KEY," +
            "  last_token bigint," +
            "  last_run_at timestamp," +
            "  rows_processed bigint," +
            "  cycle_complete boolean" +
            ")"
        );

        LOG.info("Cassandra graph tables created/verified.");
    }

    public static void shutdown() {
        if (sharedSession != null && !sharedSession.isClosed()) {
            sharedSession.close();
            sharedSession = null;
        }
        if (session != null && !session.isClosed()) {
            session.close();
            session = null;
        }
    }
}
