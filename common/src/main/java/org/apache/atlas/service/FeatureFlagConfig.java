package org.apache.atlas.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.springframework.stereotype.Component;

@Component
public class FeatureFlagConfig {
    
    private final int primaryCacheTtlMinutes;
    private final long primaryCacheMaxSize;
    private final long fallbackCacheMaxSize;
    private final int redisRetryAttempts;
    private final long redisRetryDelayMs;
    
    public FeatureFlagConfig() throws AtlasException {
        ApplicationProperties props = (ApplicationProperties) ApplicationProperties.get();
        
        this.primaryCacheTtlMinutes = props.getInt("atlas.feature.flag.cache.primary.ttl.minutes", 1);
        this.primaryCacheMaxSize = props.getLong("atlas.feature.flag.cache.primary.max.size", 1000L);
        this.fallbackCacheMaxSize = props.getLong("atlas.feature.flag.cache.fallback.max.size", 1000L);
        this.redisRetryAttempts = props.getInt("atlas.feature.flag.redis.retry.attempts", 3);
        this.redisRetryDelayMs = props.getLong("atlas.feature.flag.redis.retry.delay.ms", 1000L);
    }
    
    public int getPrimaryCacheTtlMinutes() {
        return primaryCacheTtlMinutes;
    }
    
    public long getPrimaryCacheMaxSize() {
        return primaryCacheMaxSize;
    }
    
    public long getFallbackCacheMaxSize() {
        return fallbackCacheMaxSize;
    }

    public int getRedisRetryAttempts() {
        return redisRetryAttempts;
    }
    
    public long getRedisRetryDelayMs() {
        return redisRetryDelayMs;
    }
}
