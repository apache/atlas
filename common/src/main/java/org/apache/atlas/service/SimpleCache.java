package org.apache.atlas.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A simple, thread-safe cache implementation with TTL-based expiration and jitter.
 * Jitter is added to the TTL to prevent the "thundering herd" problem by spreading out expirations.
 * Uses ConcurrentHashMap for thread safety without explicit locking.
 */
public class SimpleCache<K, V> {
    private final ConcurrentHashMap<K, CacheEntry<V>> cache = new ConcurrentHashMap<>();
    private final long ttlMs;
    private final boolean hasTtl;

    // NEW: Jitter factor (e.g., 0.2 means +/- 20% of the TTL)
    private static final double JITTER_FACTOR = 0.2;

    /**
     * Creates a new SimpleCache instance.
     * * @param ttlMs TTL in milliseconds. If null or <= 0, entries never expire.
     */
    public SimpleCache(Long ttlMs) {
        this.ttlMs = ttlMs != null ? ttlMs : 0;
        this.hasTtl = ttlMs != null && ttlMs > 0;
    }

    /**
     * Gets a value from the cache if present and not expired.
     * * @param key the cache key
     * @return the cached value, or null if not present or expired
     */
    public V getIfPresent(K key) {
        CacheEntry<V> entry = cache.get(key);
        if (entry == null) {
            return null;
        }

        if (entry.isExpired()) {
            // Lazy expiration - remove expired entry
            cache.remove(key, entry);
            return null;
        }

        return entry.getValue();
    }

    /**
     * Puts a value into the cache, calculating its expiration time with jitter.
     * * @param key the cache key
     * @param value the value to cache
     */
    public void put(K key, V value) {
        long expirationTime = calculateExpirationTime();
        cache.put(key, new CacheEntry<>(value, expirationTime));
    }

    /**
     * NEW: Helper method to calculate the expiration timestamp with jitter.
     * @return The calculated expiration timestamp in milliseconds.
     */
    private long calculateExpirationTime() {
        if (!hasTtl) {
            return Long.MAX_VALUE; // Never expires
        }

        // Calculate the jitter range (e.g., for 1000ms TTL and 0.2 factor, range is +/- 200ms)
        long jitterRange = (long) (this.ttlMs * JITTER_FACTOR);

        // Generate a random jitter value within [-jitterRange, +jitterRange]
        long jitter = ThreadLocalRandom.current().nextLong(-jitterRange, jitterRange + 1);

        return System.currentTimeMillis() + this.ttlMs + jitter;
    }

    /**
     * Removes a value from the cache.
     * * @param key the cache key to remove
     */
    public void invalidate(K key) {
        cache.remove(key);
    }

}