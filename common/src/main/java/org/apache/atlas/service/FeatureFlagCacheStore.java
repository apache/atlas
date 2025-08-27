package org.apache.atlas.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@DependsOn({"redisServiceImpl"})
public class FeatureFlagCacheStore {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagCacheStore.class);
    
    private final FeatureFlagConfig config;
    
    private final Cache<String, String> primaryCache;
    private final Cache<String, String> fallbackCache;
    private final Set<String> knownFlags = ConcurrentHashMap.newKeySet();
    
    @Inject
    public FeatureFlagCacheStore(FeatureFlagConfig config) {
        this.config = config;
        
        this.primaryCache = Caffeine.newBuilder()
                .maximumSize(config.getPrimaryCacheMaxSize())
                .expireAfterWrite(Duration.ofMinutes(config.getPrimaryCacheTtlMinutes()))
                .build();
        
        this.fallbackCache = Caffeine.newBuilder()
                .maximumSize(config.getFallbackCacheMaxSize())
                .build();
        
        LOG.info("Initialized FeatureFlagCacheStore - Primary cache TTL: {}min, Max size: {}, Fallback max size: {}", 
                config.getPrimaryCacheTtlMinutes(), config.getPrimaryCacheMaxSize(), config.getFallbackCacheMaxSize());
    }
    
    public String getFromPrimaryCache(String key) {
        String value = primaryCache.getIfPresent(key);
        LOG.debug("Primary cache {} for key: {}", value != null ? "hit" : "miss", key);
        return value;
    }
    
    public String getFromFallbackCache(String key) {
        String value = fallbackCache.getIfPresent(key);
        if (value == null) {
            LOG.warn("Fallback cache miss for key: {} - this indicates a serious issue", key);
        }
        return value;
    }
    
    public void putInBothCaches(String key, String value) {
        primaryCache.put(key, value);
        fallbackCache.put(key, value);
        knownFlags.add(key);
        LOG.debug("Cached value for key: {} in both primary and fallback caches", key);
    }
    
    public void putInFallbackCache(String key, String value) {
        fallbackCache.put(key, value);
        knownFlags.add(key);
        LOG.debug("Cached value for key: {} in fallback cache only", key);
    }
    
    public void removeFromBothCaches(String key) {
        primaryCache.invalidate(key);
        fallbackCache.invalidate(key);
        knownFlags.remove(key);
        LOG.debug("Removed key: {} from both caches", key);
    }
    
    public void clearPrimaryCache() {
        primaryCache.invalidateAll();
        LOG.info("Cleared primary cache");
    }
    
    public Set<String> getKnownFlags() {
        return Set.copyOf(knownFlags);
    }
    
    public long getPrimaryCacheSize() {
        return primaryCache.estimatedSize();
    }
    
    public long getFallbackCacheSize() {
        return fallbackCache.estimatedSize();
    }
}
