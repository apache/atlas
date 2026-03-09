package org.apache.atlas.service.config;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Configuration class for the Dynamic Config Store backed by Cassandra.
 *
 * Loads configuration properties from atlas-application.properties:
 * - atlas.config.store.cassandra.enabled: Enable/disable Cassandra config store (default: false)
 * - atlas.config.store.cassandra.activated: Activate Cassandra for reads instead of Redis (default: false)
 * - atlas.config.store.sync.interval.ms: Background sync interval in milliseconds (default: 60000)
 * - atlas.config.store.cassandra.keyspace: Cassandra keyspace name (default: config_store)
 * - atlas.config.store.cassandra.table: Cassandra table name (default: configs)
 * - atlas.config.store.app.name: Application name for partitioning (default: atlas)
 * - atlas.config.store.cassandra.hostname: Cassandra hostname (falls back to atlas.graph.storage.hostname)
 * - atlas.config.store.cassandra.replication.factor: Replication factor (default: 3)
 *
 * Migration Strategy:
 * 1. Set enabled=true, activated=false: Enables Cassandra connectivity and data sync from Redis
 * 2. Set enabled=true, activated=true: Switches reads to use Cassandra instead of Redis
 */
@Component
public class DynamicConfigStoreConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigStoreConfig.class);

    // Property keys
    public static final String PROP_ENABLED = "atlas.config.store.cassandra.enabled";
    public static final String PROP_ACTIVATED = "atlas.config.store.cassandra.activated";
    public static final String PROP_SYNC_INTERVAL_MS = "atlas.config.store.sync.interval.ms";
    public static final String PROP_KEYSPACE = "atlas.config.store.cassandra.keyspace";
    public static final String PROP_TABLE = "atlas.config.store.cassandra.table";
    public static final String PROP_APP_NAME = "atlas.config.store.app.name";
    public static final String PROP_HOSTNAME = "atlas.config.store.cassandra.hostname";
    public static final String PROP_REPLICATION_FACTOR = "atlas.config.store.cassandra.replication.factor";
    public static final String PROP_DATACENTER = "atlas.config.store.cassandra.datacenter";
    public static final String PROP_CONSISTENCY_LEVEL = "atlas.config.store.cassandra.consistency.level";
    public static final String PROP_PORT = "atlas.config.store.cassandra.port";

    // Fallback property key (reuse existing Cassandra hostname config)
    private static final String PROP_GRAPH_STORAGE_HOSTNAME = "atlas.graph.storage.hostname";
    private static final String PROP_GRAPH_STORAGE_CQL_PORT = "atlas.graph.storage.cql.port";

    // Default values - DynamicConfigStore is enabled and activated by default
    // This replaces the legacy FeatureFlagStore
    private static final boolean DEFAULT_ENABLED = true;
    private static final boolean DEFAULT_ACTIVATED = true;
    private static final long DEFAULT_SYNC_INTERVAL_MS = 60000L; // 60 seconds
    private static final String DEFAULT_KEYSPACE = "config_store";
    private static final String DEFAULT_TABLE = "configs";
    private static final String DEFAULT_APP_NAME = "atlas";
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final String DEFAULT_DATACENTER = "datacenter1";
    // Consistency level: LOCAL_QUORUM for production (requires 2+ nodes), LOCAL_ONE for local dev (single node)
    private static final String DEFAULT_CONSISTENCY_LEVEL = "LOCAL_QUORUM";
    private static final int DEFAULT_CASSANDRA_PORT = 9042;

    private final boolean enabled;
    private final boolean activated;
    private final long syncIntervalMs;
    private final String keyspace;
    private final String table;
    private final String appName;
    private final String hostname;
    private final int cassandraPort;
    private final int replicationFactor;
    private final String datacenter;
    private final String consistencyLevel;

    public DynamicConfigStoreConfig() throws AtlasException {
        Configuration props = ApplicationProperties.get();

        this.enabled = props.getBoolean(PROP_ENABLED, DEFAULT_ENABLED);
        this.activated = props.getBoolean(PROP_ACTIVATED, DEFAULT_ACTIVATED);
        this.syncIntervalMs = props.getLong(PROP_SYNC_INTERVAL_MS, DEFAULT_SYNC_INTERVAL_MS);
        this.keyspace = props.getString(PROP_KEYSPACE, DEFAULT_KEYSPACE);
        this.table = props.getString(PROP_TABLE, DEFAULT_TABLE);
        this.appName = props.getString(PROP_APP_NAME, DEFAULT_APP_NAME);
        this.replicationFactor = props.getInt(PROP_REPLICATION_FACTOR, DEFAULT_REPLICATION_FACTOR);
        this.datacenter = props.getString(PROP_DATACENTER, DEFAULT_DATACENTER);
        this.consistencyLevel = props.getString(PROP_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);

        // Hostname: use dedicated property or fall back to graph storage hostname
        String configuredHostname = props.getString(PROP_HOSTNAME, null);
        if (configuredHostname != null && !configuredHostname.isEmpty()) {
            this.hostname = configuredHostname;
        } else {
            this.hostname = props.getString(PROP_GRAPH_STORAGE_HOSTNAME, DEFAULT_HOSTNAME);
        }

        // Port: use dedicated property or fall back to graph storage CQL port
        int configuredPort = props.getInt(PROP_PORT, -1);
        if (configuredPort > 0) {
            this.cassandraPort = configuredPort;
        } else {
            this.cassandraPort = props.getInt(PROP_GRAPH_STORAGE_CQL_PORT, DEFAULT_CASSANDRA_PORT);
        }

        LOG.info("DynamicConfigStoreConfig initialized - enabled: {}, activated: {}, keyspace: {}, table: {}, hostname: {}, port: {}, syncInterval: {}ms, consistencyLevel: {}",
                enabled, activated, keyspace, table, hostname, cassandraPort, syncIntervalMs, consistencyLevel);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isActivated() {
        return activated;
    }

    public long getSyncIntervalMs() {
        return syncIntervalMs;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public String getAppName() {
        return appName;
    }

    public String getHostname() {
        return hostname;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public int getCassandraPort() {
        return cassandraPort;
    }

    public String getConsistencyLevel() {
        return consistencyLevel;
    }
}
