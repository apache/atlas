package org.apache.atlas.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.time.Duration;

@Component
@DependsOn({"redisServiceImpl"})
public class FeatureFlagCacheStore {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagCacheStore.class);
    
    private final SimpleCache<String, String> primaryCache;
    private final SimpleCache<String, String> fallbackCache;
    
    @Inject
    public FeatureFlagCacheStore(FeatureFlagConfig config) {
        this.primaryCache = new SimpleCache<>(Duration.ofMinutes(config.getPrimaryCacheTtlMinutes()).toMillis());
        this.fallbackCache = new SimpleCache<>(null); // No TTL for fallback cache
        
        LOG.info("Initialized FeatureFlagCacheStore - Primary cache TTL: {}min, Fallback cache: no expiration", 
                config.getPrimaryCacheTtlMinutes());
    }
    
    public String getFromPrimaryCache(String key) {
        String value = primaryCache.getIfPresent(key);
        LOG.debug("Primary cache {} for key: {}", value != null ? "hit" : "miss", key);
        return value;
    }
    
    public String getFromFallbackCache(String key) {
        String value = fallbackCache.getIfPresent(key);
        if (value == null) {
            LOG.warn("Fallback cache miss for key: {}", key);
        }
        return value;
    }
    
    public void putInBothCaches(String key, String value) {
        primaryCache.put(key, value);
        fallbackCache.put(key, value);
        LOG.debug("Cached value for key: {} in both primary and fallback caches", key);
    }
    
    public void putInFallbackCache(String key, String value) {
        fallbackCache.put(key, value);
        LOG.debug("Cached value for key: {} in fallback cache only", key);
    }
    
    public void removeFromBothCaches(String key) {
        primaryCache.invalidate(key);
        fallbackCache.invalidate(key);
        LOG.debug("Removed key: {} from both caches", key);
    }

}
