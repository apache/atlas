package org.apache.atlas.service.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache for dynamic configuration values.
 *
 * Design principles:
 * - All reads are served from memory (never hit Cassandra on read path)
 * - Background sync refreshes the entire cache atomically
 * - No TTL needed since background sync keeps values fresh
 * - Thread-safe using ConcurrentHashMap
 */
@Component
public class DynamicConfigCacheStore {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigCacheStore.class);

    private final ConcurrentHashMap<String, ConfigEntry> cache = new ConcurrentHashMap<>();

    /**
     * Get a config entry from cache.
     * @param key the config key
     * @return ConfigEntry or null if not found
     */
    public ConfigEntry get(String key) {
        ConfigEntry entry = cache.get(key);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cache {} for key: {}", entry != null ? "hit" : "miss", key);
        }
        return entry;
    }

    /**
     * Get only the value for a config key.
     * @param key the config key
     * @return the value or null if not found
     */
    public String getValue(String key) {
        ConfigEntry entry = cache.get(key);
        return entry != null ? entry.getValue() : null;
    }

    /**
     * Put a config entry in the cache.
     * @param key the config key
     * @param entry the config entry
     */
    public void put(String key, ConfigEntry entry) {
        cache.put(key, entry);
        LOG.debug("Cached entry for key: {}", key);
    }

    /**
     * Put a config value with metadata.
     * @param key the config key
     * @param value the config value
     * @param updatedBy who updated the config
     */
    public void put(String key, String value, String updatedBy) {
        ConfigEntry entry = new ConfigEntry(value, updatedBy, Instant.now());
        cache.put(key, entry);
        LOG.debug("Cached value for key: {} by {}", key, updatedBy);
    }

    /**
     * Remove a config entry from cache.
     * @param key the config key
     */
    public void remove(String key) {
        cache.remove(key);
        LOG.debug("Removed key: {} from cache", key);
    }

    /**
     * Get all cached entries.
     * @return unmodifiable view of all entries
     */
    public Map<String, ConfigEntry> getAll() {
        return Collections.unmodifiableMap(cache);
    }

    /**
     * Replace the entire cache atomically.
     * Used during background sync to update all values at once.
     * @param entries new entries to replace the cache with
     */
    public void replaceAll(Map<String, ConfigEntry> entries) {
        cache.clear();
        cache.putAll(entries);
        LOG.info("Cache replaced with {} entries", entries.size());
    }

    /**
     * Get the current cache size.
     * @return number of entries in cache
     */
    public int size() {
        return cache.size();
    }

    /**
     * Check if cache is empty.
     * @return true if cache has no entries
     */
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    /**
     * Represents a config entry with value and metadata.
     */
    public static class ConfigEntry {
        private final String value;
        private final String updatedBy;
        private final Instant lastUpdated;

        public ConfigEntry(String value, String updatedBy, Instant lastUpdated) {
            this.value = value;
            this.updatedBy = updatedBy;
            this.lastUpdated = lastUpdated;
        }

        public String getValue() {
            return value;
        }

        public String getUpdatedBy() {
            return updatedBy;
        }

        public Instant getLastUpdated() {
            return lastUpdated;
        }

        @Override
        public String toString() {
            return "ConfigEntry{" +
                    "value='" + value + '\'' +
                    ", updatedBy='" + updatedBy + '\'' +
                    ", lastUpdated=" + lastUpdated +
                    '}';
        }
    }
}