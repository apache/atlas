package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.service.metrics.MetricUtils;
import org.redisson.Redisson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RedisServiceImpl extends AbstractRedisService{

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceImpl.class);

    @PostConstruct
    public void init() throws AtlasException {
        try {
            LOG.info("==> RedisServiceImpl.init() - Starting Redis service initialization");
            
            redisClient = Redisson.create(getProdConfig());
            redisCacheClient = Redisson.create(getCacheImplConfig());
            
            if (redisClient == null || redisCacheClient == null) {
                MetricUtils.recordRedisConnectionFailure();
                throw new AtlasException("Failed to create Sentinel redis client.");
            }
            
            // Test basic connectivity
            testRedisConnectivity();
            
            LOG.info("RedisServiceImpl initialization completed successfully");
            
        } catch (Exception e) {
            LOG.error("CRITICAL: RedisServiceImpl initialization FAILED", e);
            MetricUtils.recordRedisConnectionFailure();
            throw new AtlasException("Error creating Sentinel redis client.", e);
        }
    }
    
    /**
     * Test Redis connectivity with basic operations
     */
    private void testRedisConnectivity() throws Exception {
        String testKey = "atlas:startup:connectivity:test:" + System.currentTimeMillis();
        String testValue = "connectivity-test";
        
        try {
            // Test cache client
            LOG.debug("Testing Redis cache client connectivity");
            redisCacheClient.getBucket(testKey).set(testValue);
            String retrievedValue = (String) redisCacheClient.getBucket(testKey).get();
            
            if (!testValue.equals(retrievedValue)) {
                throw new RuntimeException("Redis cache client connectivity test failed - value mismatch");
            }
            
            redisCacheClient.getBucket(testKey).delete();
            LOG.debug("Redis connectivity test completed successfully");
            
        } catch (Exception e) {
            MetricUtils.recordRedisConnectionFailure();
            throw new Exception("Redis connectivity test failed", e);
        }
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

}
