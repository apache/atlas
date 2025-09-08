package org.apache.atlas.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.springframework.stereotype.Component;

@Component
public class FeatureFlagConfig {
    
    private final int primaryCacheTtlMinutes;
    private final int redisRetryAttempts;
    private final long redisRetryDelayMs;
    private final double redisRetryBackoffMultiplier;
    
    public FeatureFlagConfig() throws AtlasException {
        Configuration props = ApplicationProperties.get();
        
        this.primaryCacheTtlMinutes = props.getInt("atlas.feature.flag.cache.primary.ttl.minutes", 30);
        this.redisRetryAttempts = props.getInt("atlas.feature.flag.redis.retry.attempts", 5);
        this.redisRetryDelayMs = props.getLong("atlas.feature.flag.redis.retry.delay.ms", 1000L);
        this.redisRetryBackoffMultiplier = props.getDouble("atlas.feature.flag.redis.retry.backoff.multiplier", 2.0);
    }
    
    public int getPrimaryCacheTtlMinutes() {
        return primaryCacheTtlMinutes;
    }

    public int getRedisRetryAttempts() {
        return redisRetryAttempts;
    }
    
    public long getRedisRetryDelayMs() {
        return redisRetryDelayMs;
    }
    
    public double getRedisRetryBackoffMultiplier() {
        return redisRetryBackoffMultiplier;
    }

}
