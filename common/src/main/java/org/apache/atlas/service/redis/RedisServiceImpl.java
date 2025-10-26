package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.service.metrics.MetricUtils;
import org.redisson.Redisson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl")
@Order(Ordered.HIGHEST_PRECEDENCE)
@Profile("!local")  // Don't use in tests (local profile) - use RedisServiceLocalImpl instead
public class RedisServiceImpl extends AbstractRedisService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceImpl.class);
    private static final long RETRY_DELAY_MS = 1000L;

    @PostConstruct
    public void init() throws InterruptedException {
        LOG.info("==> RedisServiceImpl.init() - Starting Redis service initialization.");

        // This loop will block the main application thread until a connection is successful.
        while (true) {
            try {
                LOG.info("Attempting to connect to Redis...");

                redisClient = Redisson.create(getProdConfig());
                redisCacheClient = Redisson.create(getCacheImplConfig());

                if (redisClient == null || redisCacheClient == null) {
                    throw new AtlasException("Failed to create Sentinel redis client.");
                }

                // Test basic connectivity to ensure clients are working.
                testRedisConnectivity();

                LOG.info("RedisServiceImpl initialization completed successfully!");
                break;
            } catch (Exception e) {
                LOG.warn("Redis connection failed: {}. Application startup is BLOCKED. Retrying in {} seconds...", e.getMessage(), RETRY_DELAY_MS / 1000);
                MetricUtils.recordRedisConnectionFailure();
                // Clean up any partially created clients before retrying.
                shutdownClients();
                Thread.sleep(RETRY_DELAY_MS);
            }
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

    private void shutdownClients() {
        if (redisClient != null && !redisClient.isShutdown()) {
            redisClient.shutdown();
        }
        if (redisCacheClient != null && !redisCacheClient.isShutdown()) {
            redisCacheClient.shutdown();
        }
        redisClient = null;
        redisCacheClient = null;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

}
