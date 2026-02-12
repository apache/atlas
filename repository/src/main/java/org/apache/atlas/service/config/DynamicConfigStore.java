package org.apache.atlas.service.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.FeatureFlag;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.service.config.DynamicConfigCacheStore.ConfigEntry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.Map;
import java.util.Objects;

/**
 * Dynamic Configuration Store backed by Cassandra.
 *
 * This is the main coordinator that:
 * - Provides static methods for getting/setting configs
 * - Manages the in-memory cache
 * - Coordinates with CassandraConfigDAO for persistence
 *
 * Design principles:
 * - Reads ALWAYS come from cache (never hit Cassandra on read path)
 * - Writes update Cassandra first, then update local cache
 * - Background sync (ConfigSyncScheduler) refreshes cache periodically
 * - If Cassandra is disabled, operations are no-ops (fall back to existing behavior)
 *
 * Configuration:
 * - atlas.config.store.cassandra.enabled=true to enable Cassandra connectivity and sync
 * - atlas.config.store.cassandra.activated=true to use Cassandra for reads (instead of Redis)
 *
 * Migration Strategy:
 * 1. Set enabled=true, activated=false: Enables Cassandra connectivity and data sync from Redis
 *    - Feature flag helper methods fall back to FeatureFlagStore (Redis)
 * 2. Set enabled=true, activated=true: Switches reads to use Cassandra instead of Redis
 *    - Feature flag helper methods read from Cassandra cache
 */
@Component
@DependsOn("featureFlagStore")
public class DynamicConfigStore implements ApplicationContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigStore.class);

    private static ApplicationContext context;

    private final DynamicConfigStoreConfig config;
    private final DynamicConfigCacheStore cacheStore;
    private final ConfigCacheRefresher cacheRefresher;

    private volatile boolean initialized = false;
    private volatile boolean cassandraAvailable = false;

    private static final String METRIC_COMPONENT = "atlas_classification";
    private static final String METRIC_PREFIX = "atlas_config_store";
    private static final String METRIC_FLAG_VALUE = METRIC_PREFIX + "_flag_value";
    private static final String METRIC_DEFAULT_FALLBACK = METRIC_PREFIX + "_default_fallback_total";
    private static final String METRIC_REDIS_RECOVERY = METRIC_PREFIX + "_redis_recovery_total";

    @Inject
    public DynamicConfigStore(DynamicConfigStoreConfig config, DynamicConfigCacheStore cacheStore,
                              ConfigCacheRefresher cacheRefresher) {
        this.config = Objects.requireNonNull(config, "DynamicConfigStoreConfig cannot be null");
        this.cacheStore = Objects.requireNonNull(cacheStore, "DynamicConfigCacheStore cannot be null");
        this.cacheRefresher = cacheRefresher; // Can be null in tests

        LOG.info("DynamicConfigStore created - enabled: {}, activated: {}", config.isEnabled(), config.isActivated());

    }

    @PostConstruct
    public void initialize() {
        if (!config.isEnabled()) {
            LOG.info("Dynamic config store is disabled (atlas.config.store.cassandra.enabled=false)");
            initialized = true;
            return;
        }

        LOG.info("Initializing DynamicConfigStore with Cassandra backend...");
        long startTime = System.currentTimeMillis();

        try {
            // Initialize Cassandra DAO
            CassandraConfigDAO.initialize(config);
            cassandraAvailable = true;

            // Phase 1 (enabled=true, activated=false): Sync feature flags from Redis to Cassandra
            // This ensures Cassandra has the current Redis values before activation
            // Phase 2 (enabled=true, activated=true): Skip Redis sync, Cassandra is the source of truth
            if (!config.isActivated()) {
                syncFeatureFlagsFromRedis();
            } else {
                LOG.info("Cassandra config store is activated - skipping Redis sync, using Cassandra as source of truth");
            }

            // Load initial data into cache from Cassandra
            loadAllConfigsIntoCache();

            // DEFENSIVE CHECK: If activated but Cassandra has no/partial rows, a previous
            // Phase 1 deployment may have been missed (e.g., ArgoCD sync gap). Recover by
            // syncing from Redis so we don't serve empty defaults.
            if (config.isActivated()) {
                int loadedCount = cacheStore.size();
                int expectedCount = ConfigKey.values().length;

                if (loadedCount < expectedCount) {
                    LOG.warn("CONFIG STORE RECOVERY: Activated store has {}/{} config rows in Cassandra. " +
                            "Phase 1 (Redis sync) may have been missed for this tenant. " +
                            "Performing recovery sync from Redis...", loadedCount, expectedCount);

                    syncFeatureFlagsFromRedis();
                    loadAllConfigsIntoCache();

                    int recoveredCount = cacheStore.size();
                    LOG.warn("CONFIG STORE RECOVERY: After Redis sync, store has {}/{} config rows",
                            recoveredCount, expectedCount);

                    recordRecoveryMetric();
                }
            }

            initialized = true;
            long duration = System.currentTimeMillis() - startTime;
            LOG.info("DynamicConfigStore initialization completed in {}ms - {} configs loaded",
                    duration, cacheStore.size());

            // Log all flag values with their source for debugging
            logAllFlagValues();

            // Register all metrics
            registerMetrics();

        } catch (Exception e) {
            LOG.error("Failed to initialize DynamicConfigStore - Cassandra config store will be unavailable", e);
            // Fail-fast if Cassandra is enabled but unavailable
            throw new RuntimeException("DynamicConfigStore initialization failed - Cassandra unavailable", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (config.isEnabled()) {
            CassandraConfigDAO.shutdown();
            LOG.info("DynamicConfigStore shutdown complete");
        }
    }

    /**
     * Load all configs from Cassandra into cache.
     * Called during initialization and by the sync scheduler.
     */
    public void loadAllConfigsIntoCache() throws AtlasBaseException {
        if (!config.isEnabled() || !cassandraAvailable) {
            return;
        }

        try {
            CassandraConfigDAO dao = CassandraConfigDAO.getInstance();
            Map<String, ConfigEntry> configs = dao.getAllConfigs();
            cacheStore.replaceAll(configs);
            LOG.debug("Loaded {} configs into cache", configs.size());
        } catch (Exception e) {
            LOG.error("Failed to load configs from Cassandra", e);
            throw e;
        }
    }

    /**
     * Sync feature flags from Redis to Cassandra and seed defaults for all ConfigKeys.
     * This is Phase 1 of the migration: populate Cassandra with current Redis values.
     * Also seeds any ConfigKey that does not have a corresponding Redis flag with its default value,
     * ensuring Cassandra always has a complete set of rows after sync.
     */
    private void syncFeatureFlagsFromRedis() {
        LOG.info("Starting feature flag sync from Redis to Cassandra...");
        int syncedFromRedis = 0;
        int seededWithDefault = 0;
        int skippedCount = 0;

        try {
            CassandraConfigDAO dao = CassandraConfigDAO.getInstance();

            // Track which ConfigKeys were populated from Redis
            java.util.Set<String> populatedKeys = new java.util.HashSet<>();

            // Step 1: Sync flags that exist in both FeatureFlag (Redis) and ConfigKey (Cassandra)
            for (String flagKey : FeatureFlag.getAllKeys()) {
                if (!ConfigKey.isValidKey(flagKey)) {
                    LOG.debug("Skipping Redis flag '{}' - not defined in ConfigKey", flagKey);
                    skippedCount++;
                    continue;
                }

                try {
                    String redisValue = FeatureFlagStore.getFlag(flagKey);

                    if (StringUtils.isNotEmpty(redisValue)) {
                        dao.putConfig(flagKey, redisValue, "redis-sync");
                        populatedKeys.add(flagKey);
                        syncedFromRedis++;
                        LOG.debug("Synced flag '{}' from Redis to Cassandra: {}", flagKey, redisValue);
                    } else {
                        LOG.debug("Skipping flag '{}' - no value in Redis", flagKey);
                        skippedCount++;
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to sync flag '{}' from Redis - will seed default", flagKey, e);
                    skippedCount++;
                }
            }

            // Step 2: Seed defaults for ConfigKeys not populated from Redis
            // This ensures Cassandra has a row for every ConfigKey, preventing empty store issues
            for (ConfigKey configKey : ConfigKey.values()) {
                if (populatedKeys.contains(configKey.getKey())) {
                    continue; // Already synced from Redis
                }

                String defaultValue = configKey.getDefaultValue();
                if (defaultValue != null) {
                    try {
                        // Only seed if no row exists yet (don't overwrite existing Cassandra values)
                        ConfigEntry existing = dao.getConfig(configKey.getKey());
                        if (existing == null) {
                            dao.putConfig(configKey.getKey(), defaultValue, "default-seed");
                            seededWithDefault++;
                            LOG.debug("Seeded default for '{}' in Cassandra: {}", configKey.getKey(), defaultValue);
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to seed default for '{}'", configKey.getKey(), e);
                    }
                }
            }

            LOG.info("Feature flag sync completed - syncedFromRedis: {}, seededWithDefault: {}, skipped: {}",
                    syncedFromRedis, seededWithDefault, skippedCount);

        } catch (Exception e) {
            LOG.error("Failed to sync feature flags from Redis to Cassandra", e);
            // Don't fail initialization - Cassandra may have existing data or we'll use defaults
        }
    }

    /**
     * Check if Cassandra backend is healthy.
     * @return true if healthy, false otherwise
     */
    public boolean isCassandraHealthy() {
        if (!config.isEnabled() || !cassandraAvailable) {
            return false;
        }

        try {
            return CassandraConfigDAO.getInstance().isHealthy();
        } catch (Exception e) {
            LOG.warn("Cassandra health check failed", e);
            return false;
        }
    }

    // ================== Static API Methods ==================

    /**
     * Get a config value.
     * Returns from cache if available, otherwise returns default value.
     *
     * @param key the config key
     * @return the config value or default if not found
     */
    public static String getConfig(String key) {
        DynamicConfigStore store = getInstance();
        if (store == null || !store.config.isEnabled()) {
            return getDefaultValue(key);
        }

        return store.getConfigInternal(key);
    }

    /**
     * Get a config value as boolean.
     *
     * @param key the config key
     * @return true if value is "true" (case-insensitive), false otherwise
     */
    public static boolean getConfigAsBoolean(String key) {
        String value = getConfig(key);
        return "true".equalsIgnoreCase(value);
    }

    /**
     * Set a config value.
     *
     * @param key the config key
     * @param value the config value
     * @param updatedBy who is making the update
     */
    public static void setConfig(String key, String value, String updatedBy) {
        DynamicConfigStore store = getInstance();
        if (store == null || !store.config.isEnabled()) {
            LOG.warn("Cannot set config - DynamicConfigStore not available");
            return;
        }

        store.setConfigInternal(key, value, updatedBy);
    }

    /**
     * Delete a config (reset to default).
     *
     * @param key the config key
     */
    public static void deleteConfig(String key) {
        DynamicConfigStore store = getInstance();
        if (store == null || !store.config.isEnabled()) {
            LOG.warn("Cannot delete config - DynamicConfigStore not available");
            return;
        }

        store.deleteConfigInternal(key);
    }

    /**
     * Get all cached configs.
     *
     * @return map of all config entries
     */
    public static Map<String, ConfigEntry> getAllConfigs() {
        DynamicConfigStore store = getInstance();
        if (store == null || !store.config.isEnabled()) {
            return Map.of();
        }

        return store.cacheStore.getAll();
    }

    /**
     * Check if the store is enabled.
     *
     * @return true if enabled, false otherwise
     */
    public static boolean isEnabled() {
        DynamicConfigStore store = getInstance();
        return store != null && store.config.isEnabled();
    }

    /**
     * Check if the store is initialized.
     *
     * @return true if initialized, false otherwise
     */
    public static boolean isReady() {
        DynamicConfigStore store = getInstance();
        return store != null && store.initialized;
    }

    /**
     * Check if the store is activated for reads.
     * When activated, feature flag reads come from Cassandra instead of Redis.
     *
     * @return true if enabled AND activated, false otherwise
     */
    public static boolean isActivated() {
        DynamicConfigStore store = getInstance();
        return store != null && store.config.isEnabled() && store.config.isActivated();
    }

    // ================== Feature Flag Helper Methods ==================
    //
    // These methods check if DynamicConfigStore is activated.
    // If activated, they read from Cassandra cache.
    // If not activated, they fall back to FeatureFlagStore (Redis).
    //

    /**
     * Check if Tag V2 (Janus optimization) is enabled.
     * Note: The flag ENABLE_JANUS_OPTIMISATION being "true" means Tag V2 is ENABLED.
     * Falls back to FeatureFlagStore (Redis) if DynamicConfigStore is not activated.
     *
     * @return true if Tag V2 is enabled, false otherwise
     */
    public static boolean isTagV2Enabled() {
        if (isActivated()) {
            return getConfigAsBoolean(ConfigKey.ENABLE_JANUS_OPTIMISATION.getKey());
        }
        // Fall back to FeatureFlagStore (Redis)
        return FeatureFlagStore.isTagV2Enabled();
    }

    /**
     * Check if maintenance mode is enabled.
     * Falls back to FeatureFlagStore (Redis) if DynamicConfigStore is not activated.
     *
     * @return true if maintenance mode is enabled, false otherwise
     */
    public static boolean isMaintenanceModeEnabled() {
        if (isActivated()) {
            return getConfigAsBoolean(ConfigKey.MAINTENANCE_MODE.getKey());
        }
        // Fall back configmap for MM
         return AtlasConfiguration.ATLAS_MAINTENANCE_MODE.getBoolean();
    }

    /**
     * Check if persona hierarchy filter is enabled.
     * Falls back to FeatureFlagStore (Redis) if DynamicConfigStore is not activated.
     *
     * @return true if enabled, false otherwise
     */
    public static boolean isPersonaHierarchyFilterEnabled() {
        if (isActivated()) {
            return getConfigAsBoolean(ConfigKey.ENABLE_PERSONA_HIERARCHY_FILTER.getKey());
        }
        // Fall back to FeatureFlagStore (Redis)
        return FeatureFlagStore.evaluate(ConfigKey.ENABLE_PERSONA_HIERARCHY_FILTER.getKey(), "true");
    }

    /**
     * Check if temp ES index should be used.
     * Falls back to FeatureFlagStore (Redis) if DynamicConfigStore is not activated.
     *
     * @return true if temp ES index should be used, false otherwise
     */
    public static boolean useTempEsIndex() {
        if (isActivated()) {
            return getConfigAsBoolean(ConfigKey.USE_TEMP_ES_INDEX.getKey());
        }
        // Fall back to FeatureFlagStore (Redis)
        return FeatureFlagStore.evaluate(ConfigKey.USE_TEMP_ES_INDEX.getKey(), "true");
    }

    /**
     * Check if delete batch operations are enabled.
     * Only enabled when DynamicConfigStore is activated and the flag is set to true.
     *
     * @return true if batch delete operations are enabled, false otherwise
     */
    public static boolean isDeleteBatchEnabled() {
        if (isActivated()) {
            return getConfigAsBoolean(ConfigKey.DELETE_BATCH_ENABLED.getKey());
        }
        return false;
    }

    // ================== Internal Methods ==================

    String getConfigInternal(String key) {
        if (!initialized) {
            LOG.warn("DynamicConfigStore not initialized, returning default for key: {}", key);
            return getDefaultValue(key);
        }

        // Always read from cache
        ConfigEntry entry = cacheStore.get(key);
        if (entry != null) {
            return entry.getValue();
        }

        // Return default value if not in cache
        return getDefaultValue(key);
    }

    void setConfigInternal(String key, String value, String updatedBy) {
        if (!initialized || !cassandraAvailable) {
            LOG.warn("Cannot set config - store not ready");
            return;
        }

        // Validate key
        if (!ConfigKey.isValidKey(key)) {
            LOG.warn("Invalid config key: {}. Only predefined keys are allowed.", key);
            return;
        }

        try {
            // Write to Cassandra first
            CassandraConfigDAO.getInstance().putConfig(key, value, updatedBy);

            // Then update local cache
            cacheStore.put(key, value, updatedBy);

            // Refresh cache on all other pods (blocking call)
            if (cacheRefresher != null) {
                ConfigCacheRefresher.RefreshSummary summary = cacheRefresher.refreshAllPodsCache(key);
                if (!summary.isFullySuccessful() && summary.getTotalCount() > 0) {
                    LOG.warn("Config set - key: {}, value: {}, by: {} - but only {}/{} pods refreshed successfully",
                        key, value, updatedBy, summary.getSuccessCount(), summary.getTotalCount());
                } else {
                    LOG.info("Config set - key: {}, value: {}, by: {} - all {} pods refreshed",
                        key, value, updatedBy, summary.getTotalCount());
                }
            } else {
                LOG.info("Config set - key: {}, value: {}, by: {} (no cache refresher)", key, value, updatedBy);
            }

            // When maintenance mode is disabled, clear the activation flags
            if (ConfigKey.MAINTENANCE_MODE.getKey().equals(key) && "false".equalsIgnoreCase(value)) {
                clearMaintenanceModeActivation(updatedBy);
            }

        } catch (Exception e) {
            LOG.error("Failed to set config - key: {}, value: {}", key, value, e);
        }
    }

    /**
     * Clear maintenance mode activation flags.
     * Called when MAINTENANCE_MODE is set to false.
     */
    private void clearMaintenanceModeActivation(String clearedBy) {
        try {
            String activatedAt = cacheStore.get(ConfigKey.MAINTENANCE_MODE_ACTIVATED_AT.getKey()) != null
                    ? cacheStore.get(ConfigKey.MAINTENANCE_MODE_ACTIVATED_AT.getKey()).getValue() : null;
            String activatedBy = cacheStore.get(ConfigKey.MAINTENANCE_MODE_ACTIVATED_BY.getKey()) != null
                    ? cacheStore.get(ConfigKey.MAINTENANCE_MODE_ACTIVATED_BY.getKey()).getValue() : null;

            if (activatedAt != null || activatedBy != null) {
                // Delete from Cassandra
                CassandraConfigDAO dao = CassandraConfigDAO.getInstance();
                dao.deleteConfig(ConfigKey.MAINTENANCE_MODE_ACTIVATED_AT.getKey());
                dao.deleteConfig(ConfigKey.MAINTENANCE_MODE_ACTIVATED_BY.getKey());

                // Remove from local cache
                cacheStore.remove(ConfigKey.MAINTENANCE_MODE_ACTIVATED_AT.getKey());
                cacheStore.remove(ConfigKey.MAINTENANCE_MODE_ACTIVATED_BY.getKey());

                // Refresh activation keys on all pods
                if (cacheRefresher != null) {
                    cacheRefresher.refreshAllPodsCache(ConfigKey.MAINTENANCE_MODE_ACTIVATED_AT.getKey());
                    cacheRefresher.refreshAllPodsCache(ConfigKey.MAINTENANCE_MODE_ACTIVATED_BY.getKey());
                }

                LOG.info("Maintenance mode activation cleared by {} (was activated at {} by {})",
                        clearedBy, activatedAt, activatedBy);
            }
        } catch (Exception e) {
            LOG.error("Failed to clear maintenance mode activation flags", e);
        }
    }

    void deleteConfigInternal(String key) {
        if (!initialized || !cassandraAvailable) {
            LOG.warn("Cannot delete config - store not ready");
            return;
        }

        try {
            // Delete from Cassandra first
            CassandraConfigDAO.getInstance().deleteConfig(key);

            // Then remove from local cache
            cacheStore.remove(key);

            // Refresh cache on all other pods (blocking call)
            if (cacheRefresher != null) {
                ConfigCacheRefresher.RefreshSummary summary = cacheRefresher.refreshAllPodsCache(key);
                if (!summary.isFullySuccessful() && summary.getTotalCount() > 0) {
                    LOG.warn("Config deleted - key: {} - but only {}/{} pods refreshed successfully",
                        key, summary.getSuccessCount(), summary.getTotalCount());
                } else {
                    LOG.info("Config deleted - key: {} - all {} pods refreshed", key, summary.getTotalCount());
                }
            } else {
                LOG.info("Config deleted - key: {} (no cache refresher)", key);
            }

        } catch (Exception e) {
            LOG.error("Failed to delete config - key: {}", key, e);
        }
    }

    private static String getDefaultValue(String key) {
        ConfigKey configKey = ConfigKey.fromKey(key);
        if (configKey != null) {
            return configKey.getDefaultValue();
        }
        return null;
    }

    // ================== Metrics & Observability ==================

    /**
     * Register all Prometheus metrics for config store observability.
     * Includes: per-flag gauges, store state gauges, version tracking, and counters.
     */
    private void registerMetrics() {
        MeterRegistry meterRegistry = org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry();

        // Version tracking metric (existing)
        Gauge.builder(METRIC_COMPONENT + "_atlas_version_enabled",
                        this,
                        ref -> isTagV2Enabled() ? 2.0 : 1.0)
                .description("Indicates which Tag propagation version is enabled (2.0 = v2, 1.0 = v1)")
                .tag("component", "version")
                .register(meterRegistry);

        // Config store state gauges
        Gauge.builder(METRIC_PREFIX + "_enabled",
                        this,
                        ref -> ref.config.isEnabled() ? 1.0 : 0.0)
                .description("Whether the dynamic config store is enabled (1.0 = yes, 0.0 = no)")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_activated",
                        this,
                        ref -> ref.config.isActivated() ? 1.0 : 0.0)
                .description("Whether the dynamic config store is activated for reads (1.0 = yes, 0.0 = no)")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_loaded_keys_count",
                        this,
                        ref -> ref.cacheStore.size())
                .description("Number of config keys currently loaded in the cache")
                .register(meterRegistry);

        Gauge.builder(METRIC_PREFIX + "_expected_keys_count",
                        this,
                        ref -> ConfigKey.values().length)
                .description("Number of expected config keys defined in ConfigKey enum")
                .register(meterRegistry);

        Gauge.builder("atlas_delete_batch_enabled",
                        this,
                        ref -> isDeleteBatchEnabled() ? 1.0 : 0.0)
                .description("Whether delete batch optimization is enabled (1=enabled, 0=disabled)")
                .tag("component", "delete")
                .register(meterRegistry);

        // Per-flag gauges — allows Grafana dashboards per tenant per flag
        for (ConfigKey configKey : ConfigKey.values()) {
            final String flagKey = configKey.getKey();
            Gauge.builder(METRIC_FLAG_VALUE,
                            this,
                            ref -> {
                                String val = ref.getConfigInternal(flagKey);
                                if ("true".equalsIgnoreCase(val)) return 1.0;
                                if ("false".equalsIgnoreCase(val)) return 0.0;
                                return val != null ? -1.0 : -2.0; // -1 = non-boolean value, -2 = null
                            })
                    .description("Current value of config flag (1.0=true, 0.0=false, -1.0=non-boolean, -2.0=null)")
                    .tag("flag", flagKey)
                    .register(meterRegistry);
        }

        LOG.info("Registered {} config store Prometheus metrics ({} per-flag gauges + store state gauges)",
                ConfigKey.values().length + 4, ConfigKey.values().length);
    }

    /**
     * Log all flag values with their source on startup for debugging.
     * Makes it immediately visible in pod logs what config state the tenant has.
     */
    private void logAllFlagValues() {
        StringBuilder sb = new StringBuilder();
        sb.append("Config store flag values on startup (activated=").append(config.isActivated()).append("):\n");

        for (ConfigKey configKey : ConfigKey.values()) {
            String key = configKey.getKey();
            ConfigEntry entry = cacheStore.get(key);
            String value;
            String source;

            if (entry != null) {
                value = entry.getValue();
                source = "cassandra";
            } else {
                value = configKey.getDefaultValue();
                source = "default";
            }

            sb.append("  ").append(key).append(" = ").append(value)
                    .append(" [source=").append(source).append("]");

            // For flags that also exist in FeatureFlag (Redis), show the Redis value for comparison
            if (FeatureFlag.isValidFlag(key)) {
                try {
                    String redisValue = FeatureFlagStore.getFlag(key);
                    sb.append(" [redis=").append(redisValue).append("]");
                    if (!StringUtils.equals(value, redisValue)) {
                        sb.append(" [MISMATCH]");
                    }
                } catch (Exception e) {
                    sb.append(" [redis=ERROR]");
                }
            }
            sb.append("\n");
        }

        LOG.info(sb.toString());
    }

    /**
     * Record a metric when we fall back to defaults on an activated store.
     * Non-zero values of this counter indicate Phase 1 sync was missed.
     */
    private void recordDefaultFallbackMetric(String key) {
        try {
            Counter.builder(METRIC_DEFAULT_FALLBACK)
                    .description("Count of times a config read fell back to default on an activated store")
                    .tag("flag", key)
                    .register(org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry())
                    .increment();
        } catch (Exception e) {
            LOG.debug("Failed to record default fallback metric for key: {}", key, e);
        }
    }

    /**
     * Record a metric when Redis recovery sync is triggered on an activated store.
     */
    private void recordRecoveryMetric() {
        try {
            Counter.builder(METRIC_REDIS_RECOVERY)
                    .description("Count of times Redis recovery sync was triggered on an activated store with missing rows")
                    .register(org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry())
                    .increment();
        } catch (Exception e) {
            LOG.debug("Failed to record Redis recovery metric", e);
        }
    }

    private static DynamicConfigStore getInstance() {
        if (context == null) {
            LOG.debug("ApplicationContext not available");
            return null;
        }

        try {
            return context.getBean(DynamicConfigStore.class);
        } catch (Exception e) {
            LOG.debug("DynamicConfigStore bean not available: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    // ================== Getters for testing ==================

    public DynamicConfigStoreConfig getConfig() {
        return config;
    }

    public DynamicConfigCacheStore getCacheStore() {
        return cacheStore;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public boolean isCassandraAvailable() {
        return cassandraAvailable;
    }
}
