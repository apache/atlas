package org.apache.atlas.service;

import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

@Component
public class FeatureFlagStore {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagStore.class);

    private static final String FF_ENABLE_JANUS_OPTIMISATION_KEY = "ENABLE_JANUS_OPTIMISATION";
    private static final String ENABLED_VALUE = "true"; // Assuming "true" means enabled

    private static RedisService redisService = null;

    // Cache variable for isTagV2Enabled
    private static volatile Boolean cachedTagV2Enabled = null;
    private static final Object tagV2Lock = new Object(); // For thread-safe initial cache loading

    @Inject
    public FeatureFlagStore(RedisService redisService) {
        FeatureFlagStore.redisService = redisService;
        // Initial cache load when the bean is created
        loadTagV2Cache();
    }

    /**
     * Returns the cached value of the JANUS_OPTIMISATION feature flag.
     * The cache is loaded once on application startup and refreshed only on explicit set/delete operations.
     *
     * @return true if Janus Optimisation is enabled; false otherwise.
     */
    public static boolean isTagV2Enabled() {
        // Double-checked locking for thread-safe initial loading
        if (cachedTagV2Enabled == null) {
            synchronized (tagV2Lock) {
                if (cachedTagV2Enabled == null) {
                    loadTagV2Cache();
                }
            }
        }
        return cachedTagV2Enabled;
    }

    private static void loadTagV2Cache() {
        if (redisService == null) {
            LOG.warn("RedisService is not initialized. Cannot load tag V2 feature flag cache. Defaulting to false.");
            cachedTagV2Enabled = false;
            return;
        }

        try {
            String value = redisService.getValue(addFeatureFlagNamespace(FF_ENABLE_JANUS_OPTIMISATION_KEY));
            cachedTagV2Enabled = StringUtils.equals(value, ENABLED_VALUE);
            LOG.info("Loaded feature flag '{}'. Value: {}", FF_ENABLE_JANUS_OPTIMISATION_KEY, cachedTagV2Enabled);
        } catch (Exception e) {
            LOG.error("Error loading feature flag cache for '{}'. Defaulting to false.", FF_ENABLE_JANUS_OPTIMISATION_KEY, e);
            cachedTagV2Enabled = false; // Default to false on error
        }
    }

    public static boolean evaluate(String key, String expectedValue) {
        boolean ret = false;
        try{
            if (redisService == null) {
                return false;
            }
            if (StringUtils.isEmpty(key) || StringUtils.isEmpty(expectedValue))
                return ret;
            String value = redisService.getValue(addFeatureFlagNamespace(key));
            ret = StringUtils.equals(value, expectedValue);
        } catch (Exception e) {
            LOG.error("Error evaluating feature flag '{}' with expected value '{}'", key, expectedValue, e);
            return ret;
        }
        return ret;
    }

    public static String getFlag(String key) {
        String ret = "";
        try{
            if (redisService == null) {
                return "";
            }
            if (StringUtils.isEmpty(key))
            {
                return ret;
            }
            return redisService.getValue(addFeatureFlagNamespace(key));
        } catch (Exception e) {
            LOG.error("Error getting feature flag '{}'", key, e);
            return ret;
        }
    }

    public static void setFlag(String key, String value) {
        if (redisService == null || StringUtils.isEmpty(key) || StringUtils.isEmpty(value))
            return;

        try {
            redisService.putValue(addFeatureFlagNamespace(key), value);
            // If the updated flag is the one we're caching, refresh it immediately
            if (FF_ENABLE_JANUS_OPTIMISATION_KEY.equals(key)) {
                loadTagV2Cache(); // Reload the cache for this specific flag
            }
        } catch (Exception e) {
            LOG.error("Error setting feature flag '{}' to value '{}'", key, value, e);
        }
    }

    public static void deleteFlag(String key) {
        if (redisService == null || StringUtils.isEmpty(key))
            return;

        try {
            redisService.removeValue(addFeatureFlagNamespace(key));
            // If the deleted flag is the one we're caching, refresh it immediately
            if (FF_ENABLE_JANUS_OPTIMISATION_KEY.equals(key)) {
                loadTagV2Cache(); // Reload the cache for this specific flag
            }
        } catch (Exception e) {
            LOG.error("Error deleting feature flag '{}'", key, e);
        }
    }

    private static String addFeatureFlagNamespace(String key) {
        return "ff:"+key;
    }
}
