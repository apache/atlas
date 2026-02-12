package org.apache.atlas.service;

import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


@Component
@DependsOn("redisServiceImpl")
public class FeatureFlagStore implements ApplicationContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagStore.class);

    private static final String FF_NAMESPACE = "ff:";
    private static final List<String> KNOWN_FLAGS = List.of(FeatureFlag.getAllKeys());
    
    // Thundering herd prevention: result sharing pattern
    private static final int LOCK_TIMEOUT_SECONDS = 10;
    private static final long INIT_RETRY_DELAY_MS = 20000L; // 2 seconds

    private final ConcurrentHashMap<String, CompletableFuture<String>> inFlightRedisFetches = new ConcurrentHashMap<>();

    private final RedisService redisService;
    private final FeatureFlagConfig config;
    private final FeatureFlagCacheStore cacheStore;
    private static ApplicationContext context;

    private volatile boolean initialized = false;

    @Inject
    public FeatureFlagStore(RedisService redisService, FeatureFlagConfig config, 
                           FeatureFlagCacheStore cacheStore) {
        this.redisService = Objects.requireNonNull(redisService, "RedisService cannot be null - critical dependency missing!");
        this.config = Objects.requireNonNull(config, "FeatureFlagConfig cannot be null");
        this.cacheStore = Objects.requireNonNull(cacheStore, "FeatureFlagCacheStore cannot be null");
        
        LOG.info("FeatureFlagStore dependencies injected successfully - RedisService: {}", 
                redisService.getClass().getSimpleName());
    }

    private static FeatureFlagStore getStore() {
        try {
            if (context == null) {
                LOG.error("ApplicationContext not initialized yet");
                return null;
            }
            return context.getBean(FeatureFlagStore.class);
        } catch (Exception e) {
            LOG.error("Failed to get FeatureFlagStore from Spring context", e);
            return null;
        }
    }

    @PostConstruct
    public void initialize() throws InterruptedException {
        LOG.info("Starting FeatureFlagStore initialization...");
        long startTime = System.currentTimeMillis();

        // A single, consolidated retry loop for the entire initialization process. Doesn't let Atlas start until Redis FF store is set up correctly.
        while (true) {
            try {
                validateDependencies();
                preloadAllFlags();
                initialized = true;

                long duration = System.currentTimeMillis() - startTime;
                LOG.info("FeatureFlagStore initialization completed successfully in {}ms", duration);
                break; // Success! Exit the loop.

            } catch (Exception e) {
                // Catches any failure from validation or preloading and retries the whole process.
                long duration = System.currentTimeMillis() - startTime;
                LOG.warn("FeatureFlagStore initialization failed after {}ms, retrying in {} seconds... Error: {}",
                        duration, INIT_RETRY_DELAY_MS / 1000, e.getMessage());
                Thread.sleep(INIT_RETRY_DELAY_MS);
            }
        }
    }

    private void validateDependencies() {
        LOG.info("Validating FeatureFlagStore dependencies...");
        try {
            // Test Redis connectivity with a simple operation
            String testKey = "ff:_health_check";
            redisService.putValue(testKey, "test");
            String testValue = redisService.getValue(testKey);
            redisService.removeValue(testKey);

            if (!"test".equals(testValue)) {
                throw new RuntimeException("Redis connectivity test failed - value mismatch");
            }

            LOG.info("Redis connectivity validated successfully");
        } catch (Exception e) {
            // Re-throw the exception to be caught by the central loop in initialize()
            throw new RuntimeException("Redis dependency validation failed", e);
        }
    }

    private void preloadAllFlags() {
        LOG.info("Preloading all known feature flags from Redis...");
        for (String flagKey : KNOWN_FLAGS) {
            FeatureFlag flag = FeatureFlag.fromKey(flagKey);
            String namespacedKey = addFeatureFlagNamespace(flagKey);
            // loadFlagFromRedisWithRetry will throw an exception on failure, which is caught by initialize()
            String value = loadFlagFromRedisWithRetry(namespacedKey, flagKey);

            if (!StringUtils.isEmpty(value)) {
                cacheStore.putInBothCaches(namespacedKey, value);
                LOG.debug("Preloaded flag '{}' with Redis value: {}", flagKey, value);
            } else {
                String defaultValue = String.valueOf(flag.getDefaultValue());
                cacheStore.putInBothCaches(namespacedKey, defaultValue);
                LOG.debug("Preloaded flag '{}' with default value: {} (not found in Redis)", flagKey, defaultValue);
            }
        }
        LOG.info("All feature flags preloaded.");
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
                
                // Calculate exponential backoff delay
                long backoffDelay = calculateBackoffDelay(attempt);
                LOG.warn("Redis operation failed for flag '{}' (attempt {}/{}), retrying in {}ms...", 
                        flagKey, attempt, config.getRedisRetryAttempts(), backoffDelay, e);
                
                try {
                    Thread.sleep(backoffDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying flag " + flagKey, ie);
                }
            }
        }
        
        return null; // This line should never be reached
    }
    
    private long calculateBackoffDelay(int attempt) {
        double exponentialFactor = Math.pow(config.getRedisRetryBackoffMultiplier(), attempt - 1);
        long backoffDelay = Math.round(config.getRedisRetryDelayMs() * exponentialFactor);
        
        // Cap the maximum delay to prevent extremely long waits (e.g., 30 seconds max)
        long maxDelayMs = 30000L;
        return Math.min(backoffDelay, maxDelayMs);
    }

    public static boolean isTagV2Enabled() {
        return !evaluate(FeatureFlag.ENABLE_JANUS_OPTIMISATION.getKey(), "false"); // Default value is false, if the flag is present or has value true it's treated as enabled
    }

    public static boolean evaluate(String key, String expectedValue) {
        return StringUtils.equals(getFlag(key), expectedValue);
    }

    public static String getFlag(String key){
        try {
            if (!isValidFlag(key)) {
                LOG.warn("Invalid feature flag requested: '{}'. Only predefined flags are allowed", key);
                throw new IllegalStateException("Invalid feature flag requested: " + key);
            }

            FeatureFlagStore instance = getInstance();
            if (instance == null) {
                LOG.warn("FeatureFlagStore not initialized, cannot get flag: {}", key);
                return getDefaultValue(key);
            }

            return instance.getFlagInternal(key);
        } catch (Exception e) {
            if (FeatureFlag.ENABLE_JANUS_OPTIMISATION.getKey().equals(key))
                throw e;
            LOG.error("Error getting feature flag '{}'", key, e);
            return getDefaultValue(key); // Always return something
        }
    }

    private static String getDefaultValue(String key) {
        if (FeatureFlag.ENABLE_JANUS_OPTIMISATION.getKey().equals(key))
            throw new RuntimeException("Cannot return default value for critical Tags FF: " + key);
        FeatureFlag flag = FeatureFlag.fromKey(key);
        return flag != null ? String.valueOf(flag.getDefaultValue()) : "false";
    }

    private static boolean isValidFlag(String key) {
        return FeatureFlag.isValidFlag(key);
    }

    String getFlagInternal(String key) {
        if (!initialized) {
            LOG.warn("FeatureFlagStore not fully initialized yet, attempting to get flag: {}", key);
            throw new IllegalStateException("FeatureFlagStore not initialized");
        }

        if (redisService == null) {
            LOG.error("RedisService is null - this should never happen after proper initialization");
            throw new IllegalStateException("RedisService is not available");
        }

        if (StringUtils.isEmpty(key)) {
            LOG.error("Invalid key: cannot be null or empty");
            throw new IllegalStateException("Null or empty redis key");
        }

        String namespacedKey = addFeatureFlagNamespace(key);

        // 1. First check: primary cache
        String value = cacheStore.getFromPrimaryCache(namespacedKey);
        if (value != null) {
            return value;
        }

        // 2. Second check: see if another thread is already fetching this key
        CompletableFuture<String> future = inFlightRedisFetches.get(key);

        if (future == null) {
            // This thread is the first; it will do the fetching
            CompletableFuture<String> newFuture = new CompletableFuture<>();

            // Atomically put the new future into the map.
            // If another thread beats us, `putIfAbsent` will return its future.
            future = inFlightRedisFetches.putIfAbsent(key, newFuture);

            if (future == null) { // We successfully put our new future in the map; we are the leader.
                future = newFuture; // Use the future we created
                try {
                    LOG.debug("Fetching from Redis for key: {}", key);
                    String redisValue = fetchFromRedisAndCache(namespacedKey, key);

                    // Complete the future successfully with the result
                    future.complete(redisValue);

                } catch (Exception e) {
                    // Complete the future exceptionally to notify other waiting threads of the failure
                    LOG.error("Exception while fetching flag '{}' for the first time.", key, e);
                    future.completeExceptionally(e);
                } finally {
                    // CRUCIAL: Clean up the map so subsequent requests re-trigger a fetch
                    inFlightRedisFetches.remove(key, future);
                }
            }
        }

        // 3. Wait for the result from the leader thread (or this thread if it was the leader)
        try {
            value = future.get(LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("Failed to get feature flag '{}' from in-flight future (timeout or exception)", key, e);
        }

        if (value != null) {
            return value;
        }

        // 4. Final check: fallback cache
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
            String value = loadFlagFromRedisWithRetry(namespacedKey, key);
            if (value != null)
                updateBothCaches(namespacedKey, value);
            return value;
        } catch (Exception e) {
            LOG.error("Failed to fetch flag '{}' from Redis", key, e);
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

    synchronized void setFlagInternal(String key, String value) {
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

    synchronized void deleteFlagInternal(String key) {
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
        return getStore();
    }

    private static String addFeatureFlagNamespace(String key) {
        return FF_NAMESPACE + key;
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

}
