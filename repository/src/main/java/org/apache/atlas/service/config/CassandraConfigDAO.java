package org.apache.atlas.service.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.cassandra.CassandraSessionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Data Access Object for dynamic configuration storage in Cassandra.
 *
 * Design principles:
 * - Singleton pattern (lazy initialization)
 * - Auto-creates keyspace and table on first use
 * - Uses LOCAL_QUORUM consistency for reads and writes
 * - Retry with exponential backoff for transient failures
 *
 * Schema:
 * CREATE TABLE config_store.configs (
 *     app_name     text,          -- Partition key (e.g., 'atlas')
 *     config_key   text,          -- Clustering key (e.g., 'MAINTENANCE_MODE')
 *     config_value text,          -- The config value
 *     updated_by   text,          -- Who made the change (audit)
 *     last_updated timestamp,     -- When the change was made
 *     PRIMARY KEY ((app_name), config_key)
 * );
 */
public class CassandraConfigDAO implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraConfigDAO.class);

    // Retry configuration
    private static final int MAX_RETRIES = 3;
    private static final Duration INITIAL_BACKOFF = Duration.ofMillis(100);

    // Connection configuration
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

    // Singleton instance
    private static volatile CassandraConfigDAO INSTANCE;
    private static volatile boolean initialized = false;
    private static volatile Exception initializationException = null;

    private final CqlSession session;
    private final boolean ownsSession; // false if using shared session from CassandraSessionProvider
    private final String keyspace;
    private final String table;
    private final String appName;
    private final DefaultConsistencyLevel consistencyLevel;

    // Prepared statements
    private final PreparedStatement selectAllStmt;
    private final PreparedStatement selectOneStmt;
    private final PreparedStatement upsertStmt;
    private final PreparedStatement deleteStmt;
    private final PreparedStatement healthCheckStmt;

    /**
     * Initialize the singleton instance with configuration.
     * Must be called before getInstance().
     *
     * @param config the configuration
     * @throws AtlasBaseException if initialization fails
     */
    public static synchronized void initialize(DynamicConfigStoreConfig config) throws AtlasBaseException {
        if (initialized) {
            LOG.debug("CassandraConfigDAO already initialized");
            return;
        }

        try {
            INSTANCE = new CassandraConfigDAO(config);
            initialized = true;
            initializationException = null;
            LOG.info("CassandraConfigDAO singleton initialized successfully");
        } catch (Exception e) {
            initializationException = e;
            LOG.error("Failed to initialize CassandraConfigDAO", e);
            throw new AtlasBaseException("Failed to initialize CassandraConfigDAO", e);
        }
    }

    /**
     * Get the singleton instance.
     * @return the singleton instance
     * @throws IllegalStateException if not initialized
     */
    public static CassandraConfigDAO getInstance() {
        if (!initialized) {
            if (initializationException != null) {
                throw new IllegalStateException("CassandraConfigDAO initialization failed previously", initializationException);
            }
            throw new IllegalStateException("CassandraConfigDAO not initialized. Call initialize() first.");
        }
        return INSTANCE;
    }

    /**
     * Check if the DAO is initialized.
     * @return true if initialized, false otherwise
     */
    public static boolean isInitialized() {
        return initialized;
    }

    private CassandraConfigDAO(DynamicConfigStoreConfig config) throws AtlasBaseException {
        this.keyspace = config.getKeyspace();
        this.table = config.getTable();
        this.appName = config.getAppName();
        this.consistencyLevel = parseConsistencyLevel(config.getConsistencyLevel());

        try {
            LOG.info("Initializing CassandraConfigDAO - hostname: {}, keyspace: {}, table: {}, consistencyLevel: {}",
                    config.getHostname(), keyspace, table, consistencyLevel);

            // Try to reuse the shared Cassandra session from the graph provider to avoid creating
            // a separate connection pool (saves ~0.2s startup and reduces resource usage).
            // ConfigDAO already uses fully-qualified table names (keyspace.table), so no keyspace binding needed.
            CqlSession sharedSession = null;
            try {
                String backend = ApplicationProperties.get().getString(
                        ApplicationProperties.GRAPHDB_BACKEND_CONF, ApplicationProperties.DEFAULT_GRAPHDB_BACKEND);
                if (ApplicationProperties.GRAPHDB_BACKEND_CASSANDRA.equalsIgnoreCase(backend)) {
                    sharedSession = CassandraSessionProvider.getSharedSession(
                            config.getHostname(), config.getCassandraPort(), config.getDatacenter());
                    LOG.info("CassandraConfigDAO: Reusing shared Cassandra session from CassandraSessionProvider");
                }
            } catch (Exception e) {
                LOG.warn("CassandraConfigDAO: Failed to get shared Cassandra session, creating own session", e);
            }

            if (sharedSession != null) {
                session = sharedSession;
                ownsSession = false;
            } else {
                DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                        .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, CONNECTION_TIMEOUT)
                        .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, CONNECTION_TIMEOUT)
                        .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT)
                        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
                        .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 2)
                        .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL)
                        .build();

                session = CqlSession.builder()
                        .addContactPoint(new InetSocketAddress(config.getHostname(), config.getCassandraPort()))
                        .withConfigLoader(configLoader)
                        .withLocalDatacenter(config.getDatacenter())
                        .build();
                ownsSession = true;
            }

            // Initialize schema
            initializeSchema(config.getReplicationFactor());

            // Prepare statements
            selectAllStmt = prepare(String.format(
                    "SELECT config_key, config_value, updated_by, last_updated FROM %s.%s WHERE app_name = ?",
                    keyspace, table));

            selectOneStmt = prepare(String.format(
                    "SELECT config_value, updated_by, last_updated FROM %s.%s WHERE app_name = ? AND config_key = ?",
                    keyspace, table));

            upsertStmt = prepare(String.format(
                    "INSERT INTO %s.%s (app_name, config_key, config_value, updated_by, last_updated) VALUES (?, ?, ?, ?, ?)",
                    keyspace, table));

            deleteStmt = prepare(String.format(
                    "DELETE FROM %s.%s WHERE app_name = ? AND config_key = ?",
                    keyspace, table));

            healthCheckStmt = prepare("SELECT release_version FROM system.local");

            LOG.info("CassandraConfigDAO initialized successfully");

        } catch (Exception e) {
            LOG.error("Failed to initialize CassandraConfigDAO", e);
            throw new AtlasBaseException("Failed to initialize CassandraConfigDAO", e);
        }
    }

    private PreparedStatement prepare(String cql) {
        return session.prepare(SimpleStatement.builder(cql)
                .setConsistencyLevel(consistencyLevel)
                .build());
    }

    /**
     * Parse consistency level string to DefaultConsistencyLevel enum.
     * Supported values: LOCAL_ONE, ONE, LOCAL_QUORUM, QUORUM, ALL
     *
     * @param level the consistency level string
     * @return the parsed DefaultConsistencyLevel, defaults to LOCAL_QUORUM if invalid
     */
    private static DefaultConsistencyLevel parseConsistencyLevel(String level) {
        if (level == null || level.isEmpty()) {
            return DefaultConsistencyLevel.LOCAL_QUORUM;
        }
        try {
            return DefaultConsistencyLevel.valueOf(level.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOG.warn("Invalid consistency level '{}', defaulting to LOCAL_QUORUM. Valid values: LOCAL_ONE, ONE, LOCAL_QUORUM, QUORUM, ALL", level);
            return DefaultConsistencyLevel.LOCAL_QUORUM;
        }
    }

    private void initializeSchema(int replicationFactor) throws AtlasBaseException {
        String createKeyspaceQuery = String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '%d'} AND durable_writes = true",
                keyspace, replicationFactor);
        executeWithRetry(SimpleStatement.builder(createKeyspaceQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                .build());
        LOG.info("Ensured keyspace {} exists", keyspace);

        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                        "app_name text, " +
                        "config_key text, " +
                        "config_value text, " +
                        "updated_by text, " +
                        "last_updated timestamp, " +
                        "PRIMARY KEY ((app_name), config_key)" +
                        ")",
                keyspace, table);
        executeWithRetry(SimpleStatement.builder(createTableQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                .build());
        LOG.info("Ensured table {}.{} exists", keyspace, table);
    }

    /**
     * Get all configs for the application.
     * @return map of config key to config entry
     * @throws AtlasBaseException if query fails
     */
    public Map<String, DynamicConfigCacheStore.ConfigEntry> getAllConfigs() throws AtlasBaseException {
        Map<String, DynamicConfigCacheStore.ConfigEntry> configs = new HashMap<>();
        try {
            BoundStatement bound = selectAllStmt.bind(appName);
            ResultSet rs = executeWithRetry(bound);

            for (Row row : rs) {
                String key = row.getString("config_key");
                String value = row.getString("config_value");
                String updatedBy = row.getString("updated_by");
                Instant lastUpdated = row.getInstant("last_updated");

                configs.put(key, new DynamicConfigCacheStore.ConfigEntry(value, updatedBy, lastUpdated));
            }

            LOG.debug("Retrieved {} configs from Cassandra", configs.size());
            return configs;

        } catch (Exception e) {
            LOG.error("Error fetching all configs", e);
            throw new AtlasBaseException("Error fetching all configs", e);
        }
    }

    /**
     * Get a single config by key.
     * @param key the config key
     * @return config entry or null if not found
     * @throws AtlasBaseException if query fails
     */
    public DynamicConfigCacheStore.ConfigEntry getConfig(String key) throws AtlasBaseException {
        try {
            BoundStatement bound = selectOneStmt.bind(appName, key);
            ResultSet rs = executeWithRetry(bound);
            Row row = rs.one();

            if (row == null) {
                LOG.debug("Config not found for key: {}", key);
                return null;
            }

            String value = row.getString("config_value");
            String updatedBy = row.getString("updated_by");
            Instant lastUpdated = row.getInstant("last_updated");

            return new DynamicConfigCacheStore.ConfigEntry(value, updatedBy, lastUpdated);

        } catch (Exception e) {
            LOG.error("Error fetching config for key: {}", key, e);
            throw new AtlasBaseException("Error fetching config for key: " + key, e);
        }
    }

    /**
     * Upsert a config value.
     * @param key the config key
     * @param value the config value
     * @param updatedBy who is making the update
     * @throws AtlasBaseException if update fails
     */
    public void putConfig(String key, String value, String updatedBy) throws AtlasBaseException {
        try {
            BoundStatement bound = upsertStmt.bind(appName, key, value, updatedBy, Instant.now());
            executeWithRetry(bound);
            LOG.info("Config updated - key: {}, value: {}, updatedBy: {}", key, value, updatedBy);

        } catch (Exception e) {
            LOG.error("Error updating config - key: {}, value: {}", key, value, e);
            throw new AtlasBaseException("Error updating config for key: " + key, e);
        }
    }

    /**
     * Delete a config.
     * @param key the config key
     * @throws AtlasBaseException if delete fails
     */
    public void deleteConfig(String key) throws AtlasBaseException {
        try {
            BoundStatement bound = deleteStmt.bind(appName, key);
            executeWithRetry(bound);
            LOG.info("Config deleted - key: {}", key);

        } catch (Exception e) {
            LOG.error("Error deleting config for key: {}", key, e);
            throw new AtlasBaseException("Error deleting config for key: " + key, e);
        }
    }

    /**
     * Check if Cassandra is healthy.
     * @return true if healthy, false otherwise
     */
    public boolean isHealthy() {
        try {
            Instant start = Instant.now();
            ResultSet rs = session.execute(healthCheckStmt.bind());
            boolean hasResults = rs.iterator().hasNext();
            Duration duration = Duration.between(start, Instant.now());

            if (hasResults) {
                LOG.debug("Cassandra health check successful in {}ms", duration.toMillis());
                return true;
            } else {
                LOG.warn("Cassandra health check failed - no results returned");
                return false;
            }

        } catch (DriverTimeoutException e) {
            LOG.warn("Cassandra health check failed due to timeout: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            LOG.warn("Cassandra health check failed: {}", e.getMessage());
            return false;
        }
    }

    private <T extends Statement<T>> ResultSet executeWithRetry(Statement<T> statement) throws AtlasBaseException {
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount < MAX_RETRIES) {
            try {
                return session.execute(statement);
            } catch (RuntimeException e) {
                // Check if it's a retryable exception (DriverTimeoutException extends RuntimeException)
                boolean isRetryable = e instanceof DriverTimeoutException ||
                    (e.getCause() != null && (e.getCause() instanceof WriteTimeoutException ||
                    e.getCause() instanceof NoHostAvailableException));
                if (isRetryable) {
                    lastException = e;
                    retryCount++;
                    LOG.warn("Retry attempt {} for statement execution due to: {}", retryCount, e.getMessage());

                    if (retryCount < MAX_RETRIES) {
                        try {
                            long backoff = INITIAL_BACKOFF.toMillis() * (long) Math.pow(2, retryCount - 1);
                            Thread.sleep(backoff);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new AtlasBaseException("Interrupted during retry backoff", ie);
                        }
                    }
                } else {
                    throw new AtlasBaseException("Non-retryable exception during statement execution", e);
                }
            }
        }

        LOG.error("Failed to execute statement after {} retries", MAX_RETRIES, lastException);
        throw new AtlasBaseException("Failed to execute statement after " + MAX_RETRIES + " retries", lastException);
    }

    @Override
    public void close() {
        // Only close the session if we own it (not shared from CassandraSessionProvider)
        if (ownsSession && session != null && !session.isClosed()) {
            session.close();
            LOG.info("CassandraConfigDAO session closed");
        }
    }

    /**
     * Shutdown the singleton instance.
     */
    public static synchronized void shutdown() {
        if (INSTANCE != null) {
            INSTANCE.close();
            INSTANCE = null;
            initialized = false;
            LOG.info("CassandraConfigDAO singleton shut down");
        }
    }
}
