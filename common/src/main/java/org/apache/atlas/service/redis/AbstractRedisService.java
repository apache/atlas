package org.apache.atlas.service.redis;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
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

public abstract class AbstractRedisService implements RedisService {

    private static final String REDIS_URL_PREFIX = "redis://";
    private static final String ATLAS_REDIS_URL = "atlas.redis.url";
    private static final String ATLAS_REDIS_SENTINEL_URLS = "atlas.redis.sentinel.urls";
    private static final String ATLAS_REDIS_USERNAME = "atlas.redis.username";
    private static final String ATLAS_REDIS_PASSWORD = "atlas.redis.password";
    private static final String ATLAS_REDIS_MASTER_NAME = "atlas.redis.master_name";
    private static final String ATLAS_REDIS_LOCK_WAIT_TIME_MS = "atlas.redis.lock.wait_time.ms";
    private static final String ATLAS_REDIS_LOCK_WATCHDOG_TIMEOUT_MS = "atlas.redis.lock.watchdog_timeout.ms";
    private static final int DEFAULT_REDIS_WAIT_TIME_MS = 15_000;
    private static final int DEFAULT_REDIS_LOCK_WATCHDOG_TIMEOUT_MS = 600_000;
    private static final String ATLAS_METASTORE_SERVICE = "atlas-metastore-service";

    RedissonClient redisClient;
    RedissonClient redisCacheClient;
    Map<String, RLock> keyLockMap;
    Configuration atlasConfig;
    long waitTimeInMS;
    long watchdogTimeoutInMS;

    @Override
    public boolean acquireDistributedLock(String key) throws Exception {
        getLogger().info("Attempting to acquire distributed lock for {}, host:{}", key, getHostAddress());
        boolean isLockAcquired;
        try {
            RLock lock = redisClient.getFairLock(key);
            isLockAcquired = lock.tryLock(waitTimeInMS, TimeUnit.MILLISECONDS);
            if (isLockAcquired) {
                keyLockMap.put(key, lock);
            } else {
                getLogger().info("Attempt failed as lock {} is already acquired, host: {}.", key, getHostAddress());
            }
        } catch (InterruptedException e) {
            getLogger().error("Failed to acquire distributed lock for {}, host: {}", key, getHostAddress(), e);
            throw new AtlasException(e);
        }
        return isLockAcquired;
    }

    @Override
    public void releaseDistributedLock(String key) {
        if (!keyLockMap.containsKey(key)) {
            return;
        }
        try {
            RLock lock = keyLockMap.get(key);
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        } catch (Exception e) {
            getLogger().error("Failed to release distributed lock for {}", key, e);
        }
    }

    @Override
    public String getValue(String key) {
        // If value doesn't exist, return null else return the value
        return (String) redisCacheClient.getBucket(convertToNamespace(key)).get();
    }

    @Override
    public String putValue(String key, String value) {
        // Put the value in the redis cache with TTL
        redisCacheClient.getBucket(convertToNamespace(key)).set(value);
        return value;
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        // Put the value in the redis cache with TTL
        redisCacheClient.getBucket(convertToNamespace(key)).set(value, timeout, TimeUnit.SECONDS);
        return value;
    }

    @Override
    public void removeValue(String key)  {
        // Remove the value from the redis cache
        redisCacheClient.getBucket(convertToNamespace(key)).delete();
    }

    private String getHostAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    private Config initAtlasConfig() throws AtlasException {
        keyLockMap = new ConcurrentHashMap<>();
        atlasConfig = ApplicationProperties.get();
        waitTimeInMS = atlasConfig.getLong(ATLAS_REDIS_LOCK_WAIT_TIME_MS, DEFAULT_REDIS_WAIT_TIME_MS);
        watchdogTimeoutInMS = atlasConfig.getLong(ATLAS_REDIS_LOCK_WATCHDOG_TIMEOUT_MS, DEFAULT_REDIS_LOCK_WATCHDOG_TIMEOUT_MS);
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
                .setCheckSentinelsList(false)
                .setKeepAlive(true)
                .setMasterConnectionMinimumIdleSize(10)
                .setMasterConnectionPoolSize(20)
                .setSlaveConnectionMinimumIdleSize(10)
                .setSlaveConnectionPoolSize(20)
                .setMasterName(atlasConfig.getString(ATLAS_REDIS_MASTER_NAME))
                .addSentinelAddress(formatUrls(atlasConfig.getStringArray(ATLAS_REDIS_SENTINEL_URLS)))
                .setUsername(atlasConfig.getString(ATLAS_REDIS_USERNAME))
                .setPassword(atlasConfig.getString(ATLAS_REDIS_PASSWORD))
                .setTimeout(500) //Setting UP timeout to 50ms
                .setRetryAttempts(0);
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
        keyLockMap.keySet().stream().forEach(k->keyLockMap.get(k).unlock());
    }
}
