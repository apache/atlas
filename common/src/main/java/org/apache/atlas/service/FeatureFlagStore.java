package org.apache.atlas.service;

import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.List;


@Component
public class FeatureFlagStore {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagStore.class);

    private static final String FF_NAMESPACE = "ff:";
    private static final List<String> KNOWN_FLAGS = List.of(FeatureFlag.getAllKeys());

    private final RedisService redisService;
    private final FeatureFlagConfig config;
    private final FeatureFlagCacheStore cacheStore;
    
    private volatile boolean initialized = false;

    @Inject
    public FeatureFlagStore(RedisService redisService, FeatureFlagConfig config, 
                           FeatureFlagCacheStore cacheStore) {
        this.redisService = redisService;
        this.config = config;
        this.cacheStore = cacheStore;
    }

    @PostConstruct
    public void initialize() {
        LOG.info("Starting FeatureFlagStore initialization...");
        long startTime = System.currentTimeMillis();
        
        try {
            preloadAllFlags();
            initialized = true;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("FeatureFlagStore initialization FAILED after {}ms", duration, e);
            throw new RuntimeException("Failed to initialize FeatureFlagStore - cannot start application", e);
        }
    }

    private void preloadAllFlags() {
        LOG.info("Preloading all known feature flags from Redis...");
        
        for (String flagKey : KNOWN_FLAGS) {
            
            FeatureFlag flag = FeatureFlag.fromKey(flagKey);
            String namespacedKey = addFeatureFlagNamespace(flagKey);
            String value = loadFlagFromRedisWithRetry(namespacedKey, flagKey);
            
            if (value != null && !value.isEmpty()) {
                cacheStore.putInFallbackCache(namespacedKey, value);
                LOG.info("Preloaded flag '{}' with Redis value: {}", flagKey, value);
            } else {
                String defaultValue = String.valueOf(flag.getDefaultValue());
                cacheStore.putInFallbackCache(namespacedKey, defaultValue);
                LOG.info("Preloaded flag '{}' with default value: {} (not found in Redis)", flagKey, defaultValue);
            }
        }
    }

    private String loadFlagFromRedisWithRetry(String namespacedKey, String flagKey) {
        for (int attempt = 1; attempt <= config.getRedisRetryAttempts(); attempt++) {
            try {
                return redisService.getValue(namespacedKey);
                
            } catch (Exception e) {
                boolean isLastAttempt = (attempt == config.getRedisRetryAttempts());
                
                if (isLastAttempt) {
                    LOG.error("Redis operation failed for flag '{}' after {} attempts", flagKey, attempt, e);
                    throw new RuntimeException("Failed to load flag " + flagKey + " after " + attempt + " attempts", e);
                }
                
                LOG.warn("Redis operation failed for flag '{}' (attempt {}/{}), retrying...", flagKey, attempt, config.getRedisRetryAttempts(), e);
                
                try {
                    Thread.sleep(config.getRedisRetryDelayMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying flag " + flagKey, ie);
                }
            }
        }
        
        return null; // This line should never be reached
    }

    public static boolean isTagV2Enabled() throws Exception {
        return evaluate(FeatureFlag.ENABLE_JANUS_OPTIMISATION.getKey(), "true") || 
               StringUtils.isNotEmpty(getFlag(FeatureFlag.ENABLE_JANUS_OPTIMISATION.getKey()));
    }

    public static boolean evaluate(String key, String expectedValue) {
        return StringUtils.equals(getFlag(key), expectedValue);
    }

    public static String getFlag(String key){
        if (!isValidFlag(key)) {
            LOG.warn("Invalid feature flag requested: '{}'. Only predefined flags are allowed", key);
            return null;
        }
        
        FeatureFlagStore instance = getInstance();
        if (instance == null) {
            LOG.warn("FeatureFlagStore not initialized, cannot get flag: {}", key);
            return null;
        }
        
        return instance.getFlagInternal(key);
    }

    private static boolean isValidFlag(String key) {
        return FeatureFlag.isValidFlag(key);
    }


    private String getFlagInternal(String key) {
        if (!initialized) {
            LOG.warn("FeatureFlagStore not fully initialized yet, attempting to get flag: {}", key);
            throw new IllegalStateException("FeatureFlagStore not initialized");
        }
        
        if (StringUtils.isEmpty(key)) {
            return "";
        }
        
        String namespacedKey = addFeatureFlagNamespace(key);
        
        String value = cacheStore.getFromPrimaryCache(namespacedKey);
        if (value != null) {
            return value;
        }
        
        value = fetchFromRedisAndCache(namespacedKey, key);
        if (value != null) {
            return value;
        }
        
        value = cacheStore.getFromFallbackCache(namespacedKey);
        if (value != null) {
            LOG.debug("Using fallback cache value for key: {}", key);
            return value;
        }
        
        LOG.warn("No value found for flag '{}' in any cache or Redis", key);
        return null;
    }

    private String fetchFromRedisAndCache(String namespacedKey, String key) {
        try {
            String value = redisService.getValue(namespacedKey);
            updateBothCaches(namespacedKey, value != null ? value : "");
            return value;
            
        } catch (Exception e) {
            LOG.debug("Failed to fetch flag '{}' from Redis", key, e);
            return null;
        }
    }

    private synchronized void updateBothCaches(String namespacedKey, String value) {
        cacheStore.putInBothCaches(namespacedKey, value);
    }

    public static void setFlag(String key, String value) {
        if (!isValidFlag(key)) {
            LOG.error("Cannot set invalid feature flag: '{}'. Only predefined flags are allowed: {}", 
                     key, String.join(", ", FeatureFlag.getAllKeys()));
            return;
        }
        
        FeatureFlagStore instance = getInstance();
        if (instance == null) {
            LOG.warn("FeatureFlagStore not initialized, cannot set flag: {}", key);
            return;
        }
        instance.setFlagInternal(key, value);
    }

    private synchronized void setFlagInternal(String key, String value) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return;
        }
        
        String namespacedKey = addFeatureFlagNamespace(key);
        try {
            redisService.putValue(namespacedKey, value);
            cacheStore.putInBothCaches(namespacedKey, value);
            LOG.info("Set feature flag '{}' to value: {}", key, value);
            
        } catch (Exception e) {
            LOG.error("Failed to set feature flag '{}'", key, e);
        }
    }

    public static void deleteFlag(String key) {
        if (!isValidFlag(key)) {
            LOG.error("Cannot delete invalid feature flag: '{}'. Only predefined flags are allowed: {}", 
                     key, String.join(", ", FeatureFlag.getAllKeys()));
            return;
        }
        
        FeatureFlagStore instance = getInstance();
        if (instance == null) {
            LOG.warn("FeatureFlagStore not initialized, cannot delete flag: {}", key);
            return;
        }
        instance.deleteFlagInternal(key);
    }

    private synchronized void deleteFlagInternal(String key) {
        if (StringUtils.isEmpty(key)) {
            return;
        }
        
        String namespacedKey = addFeatureFlagNamespace(key);
        try {
            redisService.removeValue(namespacedKey);
            cacheStore.removeFromBothCaches(namespacedKey);
            LOG.info("Deleted feature flag: {}", key);
            
        } catch (Exception e) {
            LOG.error("Failed to delete feature flag '{}'", key, e);
        }
    }

    private static FeatureFlagStore getInstance() {
        return ApplicationContextProvider.getBean(FeatureFlagStore.class);
    }

    private static String addFeatureFlagNamespace(String key) {
        return FF_NAMESPACE + key;
    }

    public boolean isInitialized() {
        return initialized;
    }

}
