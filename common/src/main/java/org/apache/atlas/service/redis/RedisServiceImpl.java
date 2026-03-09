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
    private static final int MAX_INIT_RETRIES = 3;
    private static final long BACKGROUND_INIT_DELAY_MS = 5000L;

    private volatile boolean initialized = false;
    private volatile boolean available = false;
    private volatile Exception lastError = null;
    private final Object initLock = new Object();

    @PostConstruct
    public void init() {
        LOG.info("==> RedisServiceImpl.init() - RedisServiceImpl registered, will connect lazily on first use");
        // NO BLOCKING HERE - just log and return
        // Start background initialization attempt
        startBackgroundInitialization();
    }

    /**
     * Attempt Redis connection in background thread.
     * Does not block startup.
     */
    private void startBackgroundInitialization() {
        Thread initThread = new Thread(() -> {
            try {
                Thread.sleep(BACKGROUND_INIT_DELAY_MS);  // Wait for other services to start
                ensureInitialized();
                LOG.info("Background Redis initialization successful");
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.info("Background Redis initialization interrupted during startup delay");
            } catch (Exception e) {
                LOG.warn("Background Redis initialization failed - will retry on first use: {}", e.getMessage());
            }
        }, "redis-init-background");
        initThread.setDaemon(true);
        initThread.start();
    }

    /**
     * Lazy initialization - called before any Redis operation.
     * Uses bounded retries, not infinite loop.
     */
    private void ensureInitialized() {
        if (initialized && available) {
            return;  // Already connected
        }

        synchronized (initLock) {
            if (initialized && available) {
                return;  // Double-check after acquiring lock
            }

            for (int attempt = 1; attempt <= MAX_INIT_RETRIES; attempt++) {
                try {
                    LOG.info("Attempting Redis connection (attempt {}/{})", attempt, MAX_INIT_RETRIES);

                    if (redisClient != null) {
                        shutdownClients();
                    }

                    redisClient = Redisson.create(getProdConfig());
                    redisCacheClient = Redisson.create(getCacheImplConfig());

                    if (redisClient == null || redisCacheClient == null) {
                        throw new AtlasException("Failed to create Sentinel redis client.");
                    }

                    testRedisConnectivity();

                    initialized = true;
                    available = true;
                    lastError = null;
                    LOG.info("Redis connection established successfully");
                    return;

                } catch (Exception e) {
                    lastError = e;
                    MetricUtils.recordRedisConnectionFailure();
                    LOG.warn("Redis connection attempt {}/{} failed: {}", attempt, MAX_INIT_RETRIES, e.getMessage());

                    if (attempt < MAX_INIT_RETRIES) {
                        try {
                            Thread.sleep(RETRY_DELAY_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }

            // All retries exhausted
            initialized = true;  // Mark as "tried"
            available = false;   // But not available
            LOG.error("Redis initialization failed after {} attempts", MAX_INIT_RETRIES);
        }
    }

    /**
     * Check if Redis is available for use.
     * Triggers lazy initialization if not yet attempted.
     */
    @Override
    public boolean isAvailable() {
        if (!initialized) {
            ensureInitialized();
        }
        return available;
    }

    /**
     * Get the last connection error (for diagnostics).
     */
    public Exception getLastError() {
        return lastError;
    }

    /**
     * Attempt to reconnect if previously failed.
     */
    public void reconnect() {
        synchronized (initLock) {
            initialized = false;
            available = false;
            ensureInitialized();
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

    @Override
    public boolean acquireDistributedLock(String key) throws Exception {
        if (!isAvailable()) {
            LOG.warn("Cannot acquire distributed lock {} - Redis unavailable", key);
            return false;
        }
        return super.acquireDistributedLock(key);
    }

    @Override
    public java.util.concurrent.locks.Lock acquireDistributedLockV2(String key) throws Exception {
        if (!isAvailable()) {
            LOG.warn("Cannot acquire distributed lock {} - Redis unavailable", key);
            return null;
        }
        return super.acquireDistributedLockV2(key);
    }

    @Override
    public void releaseDistributedLock(String key) {
        if (!isAvailable()) {
            LOG.warn("Cannot release lock {} - Redis unavailable", key);
            return;
        }
        super.releaseDistributedLock(key);
    }

    @Override
    public void releaseDistributedLockV2(java.util.concurrent.locks.Lock lock, String key) {
        if (!isAvailable()) {
            LOG.warn("Cannot release lock {} - Redis unavailable", key);
            return;
        }
        super.releaseDistributedLockV2(lock, key);
    }

    @Override
    public String getValue(String key) {
        if (!isAvailable()) {
            LOG.warn("Cannot get value for {} - Redis unavailable", key);
            return null;
        }
        return super.getValue(key);
    }

    @Override
    public String getValue(String key, String defaultValue) {
        if (!isAvailable()) {
            LOG.warn("Cannot get value for {} - Redis unavailable, returning default", key);
            return defaultValue;
        }
        return super.getValue(key, defaultValue);
    }

    @Override
    public String putValue(String key, String value) {
        if (!isAvailable()) {
            LOG.warn("Cannot put value for {} - Redis unavailable", key);
            return null;
        }
        return super.putValue(key, value);
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        if (!isAvailable()) {
            LOG.warn("Cannot put value for {} - Redis unavailable", key);
            return null;
        }
        return super.putValue(key, value, timeout);
    }

    @Override
    public void removeValue(String key) {
        if (!isAvailable()) {
            LOG.warn("Cannot remove value for {} - Redis unavailable", key);
            return;
        }
        super.removeValue(key);
    }

}
