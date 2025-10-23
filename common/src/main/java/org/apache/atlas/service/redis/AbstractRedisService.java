package org.apache.atlas.service.redis;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.service.metrics.MetricUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public abstract class AbstractRedisService implements RedisService {

    private static final String REDIS_URL_PREFIX = "redis://";
    private static final String ATLAS_REDIS_URL = "atlas.redis.url";
    private static final String ATLAS_REDIS_SENTINEL_URLS = "atlas.redis.sentinel.urls";
    private static final String ATLAS_REDIS_USERNAME = "atlas.redis.username";
    private static final String ATLAS_REDIS_PASSWORD = "atlas.redis.password";
    private static final String ATLAS_REDIS_MASTER_NAME = "atlas.redis.master_name";
    private static final String ATLAS_REDIS_LOCK_WAIT_TIME_MS = "atlas.redis.lock.wait_time.ms";
    private static final String ATLAS_REDIS_LOCK_WATCHDOG_TIMEOUT_MS = "atlas.redis.lock.watchdog_timeout.ms";
    private static final String ATLAS_REDIS_LEASE_TIME_MS = "atlas.redis.lease_time.ms";
    private static final String CHECK_SENTINELS_LIST = "atlas.redis.sentinel.check_list.enabled";
    private static final int DEFAULT_REDIS_WAIT_TIME_MS = 15_000;
    private static final int DEFAULT_REDIS_LOCK_WATCHDOG_TIMEOUT_MS = 600_000;
    private static final int DEFAULT_REDIS_LEASE_TIME_MS = 60_000;
    private static final String ATLAS_METASTORE_SERVICE = "atlas-metastore-service";
    // Heartbeat monitoring for lock health checking (not for renewal)
    private final ScheduledExecutorService heartbeatExecutor = Executors.newScheduledThreadPool(1);
    private final long HEARTBEAT_INTERVAL_MS = 30000; // 30 seconds - only for health monitoring
    private final Map<String, LockInfo> activeLocks = new ConcurrentHashMap<>();

    RedissonClient redisClient;
    RedissonClient redisCacheClient;
    Map<String, RLock> keyLockMap;
    Configuration atlasConfig;
    long waitTimeInMS;
    long leaseTimeInMS;
    long watchdogTimeoutInMS;
    boolean checkSentinelsList;

    // Inner class to track lock information
    private static class LockInfo {
        final RLock lock;
        final long lockAcquiredTime;
        final String ownerThreadName;
        volatile ScheduledFuture<?> healthCheckTask;

        LockInfo(RLock lock, String ownerThreadName) {
            this.lock = lock;
            this.lockAcquiredTime = System.currentTimeMillis();
            this.ownerThreadName = ownerThreadName;
        }
    }

    @Override
    public boolean acquireDistributedLock(String key) throws Exception {
        getLogger().info("Attempting to acquire distributed lock for {}, host:{}", key, getHostAddress());
        RLock lock = redisClient.getFairLock(key);
        String currentThreadName = Thread.currentThread().getName();
        try {
            // Redisson automatically handles lock renewal via its watchdog mechanism
            // when leaseTime is -1 (which is the default for tryLock without leaseTime)
            boolean isLockAcquired = lock.tryLock(waitTimeInMS, TimeUnit.MILLISECONDS);
            if (isLockAcquired) {
                getLogger().info("Lock with key {} is acquired, host: {}, thread: {}", key, getHostAddress(), currentThreadName);

                // Store lock information
                keyLockMap.put(key, lock);
                LockInfo lockInfo = new LockInfo(lock, currentThreadName);
                activeLocks.put(key, lockInfo);

                // Start health monitoring (not renewal - Redisson handles that automatically)
                ScheduledFuture<?> healthCheckTask = heartbeatExecutor.scheduleAtFixedRate(() -> {
                    monitorLockHealth(key, lockInfo);
                }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
                lockInfo.healthCheckTask = healthCheckTask;
            } else {
                getLogger().info("Attempt failed as fair lock {} is already acquired, host: {}", key, getHostAddress());
            }
            return isLockAcquired;
        } catch (InterruptedException e) {
            getLogger().error("Failed to acquire distributed lock for {}, host: {}", key, getHostAddress(), e);
            throw new AtlasException(e);
        }
    }
    // Health monitoring method - does not attempt to renew locks
    private void monitorLockHealth(String key, LockInfo lockInfo) {
        try {
            RLock lock = lockInfo.lock;
            if (lock != null && lock.isLocked()) {
                long lockDuration = System.currentTimeMillis() - lockInfo.lockAcquiredTime;
                getLogger().debug("Lock health check for {}: locked for {} ms, owner: {}",
                        key, lockDuration, lockInfo.ownerThreadName);
            } else {
                // Lock is no longer held, cleanup monitoring
                getLogger().debug("Lock {} is no longer held, stopping health monitoring", key);
                cleanupLockMonitoring(key);
            }
        } catch (Exception e) {
            getLogger().warn("Health check failed for lock {}", key, e);
        }
    }

    @Override
    public Lock acquireDistributedLockV2(String key) throws Exception {
        getLogger().info("Attempting to acquire distributed lock for {}, host:{}", key, getHostAddress());
        RLock lock = null;
        try {
            lock = redisClient.getFairLock(key);
            // Use Redisson's automatic renewal by not specifying leaseTime
            boolean isLockAcquired = lock.tryLock(waitTimeInMS, TimeUnit.MILLISECONDS);
            if (isLockAcquired) {
                getLogger().info("Lock with key {} is acquired, host: {}.", key, getHostAddress());
                return lock;
            } else {
                getLogger().info("Attempt failed as fair lock {} is already acquired, host: {}.", key, getHostAddress());
                return null;
            }

        } catch (InterruptedException e) {
            getLogger().error("Failed to acquire distributed lock for {}, host: {}", key, getHostAddress(), e);
            if (lock != null && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
            throw new AtlasException(e);
        }
    }


    @Override
    public void releaseDistributedLock(String key) {
        if (!keyLockMap.containsKey(key)) {
            return;
        }
        try {
            RLock lock = keyLockMap.get(key);
            if (lock != null && lock.isHeldByCurrentThread()) {
                lock.unlock();
                getLogger().info("Released distributed lock for {}", key);
            }
            // Cleanup monitoring and tracking
            cleanupLockMonitoring(key);
            keyLockMap.remove(key);

        } catch (Exception e) {
            getLogger().error("Failed to release distributed lock for {}", key, e);
        }
    }

    // Thread-safe cleanup of lock monitoring
    private void cleanupLockMonitoring(String key) {
        LockInfo lockInfo = activeLocks.remove(key);
        if (lockInfo != null && lockInfo.healthCheckTask != null) {
            lockInfo.healthCheckTask.cancel(false);
        }
    }

    @Override
    public void releaseDistributedLockV2(Lock lock, String key) {
        try {
            if (lock != null) {
                lock.unlock();
            }
        } catch (Exception e) {
            getLogger().error("Failed to release distributed lock for {}", key, e);
        }
    }

    @Override
    public String getValue(String key) {
        try {
            return (String) redisCacheClient.getBucket(convertToNamespace(key)).get();
        } catch (Exception e) {
            MetricUtils.recordRedisConnectionFailure();
            getLogger().error("Redis getValue operation failed for key: {}", key, e);
            throw e;
        }
    }

    @Override
    public String getValue(String key, String defaultValue) {
        try {
            String value = (String) redisCacheClient.getBucket(convertToNamespace(key)).get();
            if (StringUtils.isEmpty(value)) {
                return defaultValue;
            } else {
                return value;
            }
        } catch (Exception e) {
            MetricUtils.recordRedisConnectionFailure();
            getLogger().error("Redis getValue operation failed for key: {}", key, e);
            throw e;
        }
    }

    @Override
    public String putValue(String key, String value) {
        try {
            // Put the value in the redis cache with TTL
            redisCacheClient.getBucket(convertToNamespace(key)).set(value);
            return value;
        } catch (Exception e) {
            MetricUtils.recordRedisConnectionFailure();
            getLogger().warn("Redis putValue operation failed for key: {}", key, e);
            throw e;
        }
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        try {
            // Put the value in the redis cache with TTL
            redisCacheClient.getBucket(convertToNamespace(key)).set(value, timeout, TimeUnit.SECONDS);
            return value;
        } catch (Exception e) {
            MetricUtils.recordRedisConnectionFailure();
            getLogger().warn("Redis putValue with TTL operation failed for key: {}", key, e);
            throw e;
        }
    }

    @Override
    public void removeValue(String key)  {
        try {
            // Remove the value from the redis cache
            redisCacheClient.getBucket(convertToNamespace(key)).delete();
        } catch (Exception e) {
            MetricUtils.recordRedisConnectionFailure();
            getLogger().warn("Redis removeValue operation failed for key: {}", key, e);
            throw e;
        }
    }

    private String getHostAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    private Config initAtlasConfig() throws AtlasException {
        keyLockMap = new ConcurrentHashMap<>();
        atlasConfig = ApplicationProperties.get();
        waitTimeInMS = atlasConfig.getLong(ATLAS_REDIS_LOCK_WAIT_TIME_MS, DEFAULT_REDIS_WAIT_TIME_MS);
        leaseTimeInMS = atlasConfig.getLong(ATLAS_REDIS_LEASE_TIME_MS, DEFAULT_REDIS_LEASE_TIME_MS);
        watchdogTimeoutInMS = atlasConfig.getLong(ATLAS_REDIS_LOCK_WATCHDOG_TIMEOUT_MS, DEFAULT_REDIS_LOCK_WATCHDOG_TIMEOUT_MS);
        checkSentinelsList = atlasConfig.getBoolean(CHECK_SENTINELS_LIST, true);
        Config redisConfig = new Config();
        redisConfig.setLockWatchdogTimeout(watchdogTimeoutInMS);
        return redisConfig;
    }

    private String convertToNamespace(String key){
        // Append key with namespace :atlas
        return "atlas:"+key;
    }

    Config getLocalConfig() throws AtlasException {
        Config config = initAtlasConfig();
        config.useSingleServer()
                .setAddress(formatUrls(atlasConfig.getStringArray(ATLAS_REDIS_URL))[0])
                .setUsername(atlasConfig.getString(ATLAS_REDIS_USERNAME))
                .setPassword(atlasConfig.getString(ATLAS_REDIS_PASSWORD));
        return config;
    }

    Config getProdConfig() throws AtlasException {
        Config config = initAtlasConfig();
        config.useSentinelServers()
                .setClientName(ATLAS_METASTORE_SERVICE)
                .setReadMode(ReadMode.MASTER_SLAVE)
                .setCheckSentinelsList(false)
                .setKeepAlive(true)
                .setMasterConnectionMinimumIdleSize(10)
                .setMasterConnectionPoolSize(20)
                .setSlaveConnectionMinimumIdleSize(10)
                .setSlaveConnectionPoolSize(20)
                .setMasterName(atlasConfig.getString(ATLAS_REDIS_MASTER_NAME))
                .addSentinelAddress(formatUrls(atlasConfig.getStringArray(ATLAS_REDIS_SENTINEL_URLS)))
                .setUsername(atlasConfig.getString(ATLAS_REDIS_USERNAME))
                .setPassword(atlasConfig.getString(ATLAS_REDIS_PASSWORD));
        return config;
    }

    Config getCacheImplConfig() {
        Config config = new Config();
        config.useSentinelServers()
                .setClientName(ATLAS_METASTORE_SERVICE+"-redisCache")
                .setReadMode(ReadMode.MASTER_SLAVE)
                .setCheckSentinelsList(checkSentinelsList)
                .setKeepAlive(true)
                .setMasterConnectionMinimumIdleSize(5)
                .setMasterConnectionPoolSize(5)
                .setSlaveConnectionMinimumIdleSize(5)
                .setSlaveConnectionPoolSize(5)
                .setMasterName(atlasConfig.getString(ATLAS_REDIS_MASTER_NAME))
                .addSentinelAddress(formatUrls(atlasConfig.getStringArray(ATLAS_REDIS_SENTINEL_URLS)))
                .setUsername(atlasConfig.getString(ATLAS_REDIS_USERNAME))
                .setPassword(atlasConfig.getString(ATLAS_REDIS_PASSWORD))
                .setTimeout(50) //Setting UP timeout to 50ms
                .setRetryAttempts(10); //Retry 10 times;
        return config;
    }

    private String[] formatUrls(String[] urls) throws IllegalArgumentException {
        if (ArrayUtils.isEmpty(urls)) {
            getLogger().error("Invalid redis cluster urls");
            throw new IllegalArgumentException("Invalid redis cluster urls");
        }
        return Arrays.stream(urls).map(url -> {
            if (url.startsWith(REDIS_URL_PREFIX)) {
                return url;
            }
            return REDIS_URL_PREFIX + url;
        }).toArray(String[]::new);
    }

    @PreDestroy
    public void flushLocks(){
        // Cancel all health check tasks
        activeLocks.values().forEach(lockInfo -> {
            if (lockInfo.healthCheckTask != null) {
                lockInfo.healthCheckTask.cancel(false);
            }
        });
        activeLocks.clear();

        // Release all locks held by current thread
        keyLockMap.entrySet().removeIf(entry -> {
            String key = entry.getKey();
            RLock lock = entry.getValue();
            try {
                if (lock != null && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                    getLogger().info("Released lock {} during shutdown", key);
                    return true;
                }
            } catch (Exception e) {
                getLogger().warn("Failed to release lock {} during shutdown", key, e);
            }
            return false;
        });

        // Shutdown the health monitoring executor
        heartbeatExecutor.shutdown();
        try {
            if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                getLogger().warn("Health monitoring executor did not terminate gracefully, forcing shutdown");
                heartbeatExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            getLogger().warn("Interrupted while waiting for health monitoring executor shutdown");
            heartbeatExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}