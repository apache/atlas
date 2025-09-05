package org.apache.atlas.service;

/**
 * A simple wrapper for cache entries that includes the value and creation timestamp.
 * Used for TTL-based expiration in the custom cache implementation.
 */
class CacheEntry<V> {
    private final V value;
    private final long creationTime;
    
    public CacheEntry(V value) {
        this.value = value;
        this.creationTime = System.currentTimeMillis();
    }
    
    public V getValue() {
        return value;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    /**
     * Check if this cache entry has expired based on the given TTL.
     * 
     * @param ttlMs TTL in milliseconds. If <= 0, entry never expires.
     * @return true if the entry has expired, false otherwise
     */
    public boolean isExpired(long ttlMs) {
        if (ttlMs <= 0) {
            return false; // No expiration
        }
        return (System.currentTimeMillis() - creationTime) > ttlMs;
    }
}
