package org.apache.atlas.service.config;

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
import java.time.Instant;
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

            initialized = true;
            long duration = System.currentTimeMillis() - startTime;
            LOG.info("DynamicConfigStore initialization completed in {}ms - {} configs loaded",
                    duration, cacheStore.size());

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
     * Sync feature flags from Redis to Cassandra.
     * This is Phase 1 of the migration: populate Cassandra with current Redis values.
     * Only syncs keys that exist in both FeatureFlag (Redis) and ConfigKey (Cassandra).
     */
    private void syncFeatureFlagsFromRedis() {
        LOG.info("Starting feature flag sync from Redis to Cassandra...");
        int syncedCount = 0;
        int skippedCount = 0;

        try {
            CassandraConfigDAO dao = CassandraConfigDAO.getInstance();

            // Iterate through all Redis feature flags
            for (String flagKey : FeatureFlag.getAllKeys()) {
                // Only sync if the key also exists in ConfigKey (Cassandra schema)
                if (!ConfigKey.isValidKey(flagKey)) {
                    LOG.debug("Skipping Redis flag '{}' - not defined in ConfigKey", flagKey);
                    skippedCount++;
                    continue;
                }

                try {
                    // Get current value from Redis
                    String redisValue = FeatureFlagStore.getFlag(flagKey);

                    if (StringUtils.isNotEmpty(redisValue)) {
                        // Write to Cassandra
                        dao.putConfig(flagKey, redisValue, "redis-sync");
                        syncedCount++;
                        LOG.debug("Synced flag '{}' from Redis to Cassandra: {}", flagKey, redisValue);
                    } else {
                        LOG.debug("Skipping flag '{}' - no value in Redis", flagKey);
                        skippedCount++;
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to sync flag '{}' from Redis - will use default value", flagKey, e);
                    skippedCount++;
                }
            }

            LOG.info("Feature flag sync from Redis completed - synced: {}, skipped: {}", syncedCount, skippedCount);

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
     * Note: The flag ENABLE_JANUS_OPTIMISATION being "false" means Tag V2 is ENABLED.
     * Falls back to FeatureFlagStore (Redis) if DynamicConfigStore is not activated.
     *
     * @return true if Tag V2 is enabled, false otherwise
     */
    public static boolean isTagV2Enabled() {
        if (isActivated()) {
            return !getConfigAsBoolean(ConfigKey.ENABLE_JANUS_OPTIMISATION.getKey());
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
