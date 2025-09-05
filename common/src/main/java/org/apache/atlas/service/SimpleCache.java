package org.apache.atlas.service;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple, thread-safe cache implementation with optional TTL-based expiration.
 * Uses ConcurrentHashMap for thread safety without explicit locking.
 * Designed for small caches like feature flags where size limits are not needed.
 */
public class SimpleCache<K, V> {
    private final ConcurrentHashMap<K, CacheEntry<V>> cache = new ConcurrentHashMap<>();
    private final long ttlMs;
    private final boolean hasTtl;
    
    /**
     * Creates a new SimpleCache instance.
     * 
     * @param ttlMs TTL in milliseconds. If null or <= 0, entries never expire.
     */
    public SimpleCache(Long ttlMs) {
        this.ttlMs = ttlMs != null ? ttlMs : 0;
        this.hasTtl = ttlMs != null && ttlMs > 0;
    }
    
    /**
     * Gets a value from the cache if present and not expired.
     * 
     * @param key the cache key
     * @return the cached value, or null if not present or expired
     */
    public V getIfPresent(K key) {
        CacheEntry<V> entry = cache.get(key);
        if (entry == null) {
            return null;
        }
        
        if (hasTtl && entry.isExpired(ttlMs)) {
            // Lazy expiration - remove expired entry
            cache.remove(key);
            return null;
        }
        
        return entry.getValue();
    }
    
    /**
     * Puts a value into the cache.
     * 
     * @param key the cache key
     * @param value the value to cache
     */
    public void put(K key, V value) {
        cache.put(key, new CacheEntry<>(value));
    }
    
    /**
     * Removes a value from the cache.
     * 
     * @param key the cache key to remove
     */
    public void invalidate(K key) {
        cache.remove(key);
    }
    
    /**
     * Removes all values from the cache.
     */
    public void invalidateAll() {
        cache.clear();
    }
    
    /**
     * Gets the estimated size of the cache.
     * Note: This is the actual size, not an estimate, since we're using ConcurrentHashMap.
     * 
     * @return the number of entries in the cache
     */
    public long estimatedSize() {
        return cache.size();
    }
    
    /**
     * Checks if the cache has TTL-based expiration enabled.
     * 
     * @return true if TTL is enabled, false otherwise
     */
    public boolean hasTtl() {
        return hasTtl;
    }
    
    /**
     * Gets the TTL value in milliseconds.
     * 
     * @return the TTL in milliseconds, or 0 if TTL is disabled
     */
    public long getTtlMs() {
        return ttlMs;
    }
}
