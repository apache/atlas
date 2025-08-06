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
        if (cachedTagV2Enabled) {
            LOG.debug("Using v2 tag flow (Cassandra)");
        } else {
            LOG.debug("Using v1 tag flow (JanusGraph)");
        }
        return cachedTagV2Enabled;
    }

    private static void loadTagV2Cache() {
        if (redisService == null) {
            LOG.warn("RedisService is not initialized. Cannot load tag V2 feature flag cache.");
            return;
        }

        int retries = 5;
        while (retries > 0) {
            try {
                String value = redisService.getValue(addFeatureFlagNamespace(FF_ENABLE_JANUS_OPTIMISATION_KEY));
                cachedTagV2Enabled = StringUtils.isNotEmpty(value);
                LOG.info("Loaded feature flag '{}'. Value: {}", FF_ENABLE_JANUS_OPTIMISATION_KEY, cachedTagV2Enabled);
                return;
            } catch (Exception e) {
                retries--;
                LOG.error("Error loading feature flag cache for '{}'. Retries left: {}.", FF_ENABLE_JANUS_OPTIMISATION_KEY, retries, e);
                if (retries == 0) {
                    throw new RuntimeException("Failed to load feature flag for " + FF_ENABLE_JANUS_OPTIMISATION_KEY + " after multiple retries.", e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
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
        redisService.putValue(addFeatureFlagNamespace(key), value);
    }

    public static void deleteFlag(String key) {
        if (redisService == null || StringUtils.isEmpty(key))
            return;
        redisService.removeValue(addFeatureFlagNamespace(key));
    }

    private static String addFeatureFlagNamespace(String key) {
        return "ff:"+key;
    }
}
