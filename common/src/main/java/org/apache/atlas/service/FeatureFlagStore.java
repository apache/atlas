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

    // Cache variables for isTagV2Enabled
    private static volatile Boolean cachedTagV2Enabled = null;
    private static volatile long lastTagV2RefreshTime = 0L;
    private static final long CACHE_REFRESH_INTERVAL_MS = 30 * 60 * 1000; // 30 minutes in milliseconds
    private static final Object tagV2Lock = new Object(); // For thread-safe cache refresh

    @Inject
    public FeatureFlagStore(RedisService redisService) {
        FeatureFlagStore.redisService = redisService;
        // Initial cache load when the bean is created
        refreshTagV2Cache();
    }

    /**
     * Returns the cached value of the JANUS_OPTIMISATION feature flag, refreshing it if stale.
     *
     * @return true if Janus Optimisation is enabled; false otherwise.
     */
    public static boolean isTagV2Enabled() {
        // Check if cache is stale or not initialized
        if (cachedTagV2Enabled == null || (System.currentTimeMillis() - lastTagV2RefreshTime > CACHE_REFRESH_INTERVAL_MS)) {
            synchronized (tagV2Lock) {
                // Double-checked locking to prevent multiple threads from refreshing
                if (cachedTagV2Enabled == null || (System.currentTimeMillis() - lastTagV2RefreshTime > CACHE_REFRESH_INTERVAL_MS)) {
                    refreshTagV2Cache();
                }
            }
        }
        return cachedTagV2Enabled;
    }

    private static void refreshTagV2Cache() {
        if (redisService == null) {
            LOG.warn("RedisService is not initialized. Cannot refresh tag V2 feature flag cache. Defaulting to false.");
            cachedTagV2Enabled = false;
            lastTagV2RefreshTime = System.currentTimeMillis();
            return;
        }

        try {
            String value = redisService.getValue(addFeatureFlagNamespace(FF_ENABLE_JANUS_OPTIMISATION_KEY));
            cachedTagV2Enabled = StringUtils.equals(value, ENABLED_VALUE);
            lastTagV2RefreshTime = System.currentTimeMillis();
            LOG.debug("Refreshed feature flag '{}'. New value: {}", FF_ENABLE_JANUS_OPTIMISATION_KEY, cachedTagV2Enabled);
        } catch (Exception e) {
            LOG.error("Error refreshing feature flag cache for '{}'. Defaulting to false.", FF_ENABLE_JANUS_OPTIMISATION_KEY, e);
            cachedTagV2Enabled = false; // Default to false on error
            lastTagV2RefreshTime = System.currentTimeMillis();
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
                refreshTagV2Cache();
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
                refreshTagV2Cache();
            }
        } catch (Exception e) {
            LOG.error("Error deleting feature flag '{}'", key, e);
        }
    }

    private static String addFeatureFlagNamespace(String key) {
        return "ff:"+key;
    }
}
