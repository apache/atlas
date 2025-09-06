package org.apache.atlas.service;

/**
 * A simple wrapper for cache entries that includes the value and its expiration timestamp.
 * Used for TTL-based expiration with jitter in the custom cache implementation.
 */
class CacheEntry<V> {
    private final V value;
    private final long expirationTime; // CHANGED: Store expiration time directly

    /**
     * Creates a new cache entry.
     *
     * @param value          The value to be stored.
     * @param expirationTime The pre-calculated timestamp (in milliseconds) when this entry should expire.
     * A value of Long.MAX_VALUE means it never expires.
     */
    public CacheEntry(V value, long expirationTime) {
        this.value = value;
        this.expirationTime = expirationTime;
    }

    public V getValue() {
        return value;
    }

    /**
     * Check if this cache entry has expired.
     * * @return true if the entry has expired, false otherwise
     */
    public boolean isExpired() {
        // No expiration for entries with max value timestamp
        if (expirationTime == Long.MAX_VALUE) {
            return false;
        }
        return System.currentTimeMillis() > expirationTime;
    }
}