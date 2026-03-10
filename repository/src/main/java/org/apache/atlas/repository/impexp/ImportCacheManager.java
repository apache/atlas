/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.impexp;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Lightweight in-memory cache for import operations.
 *
 * <p>Keeps at most 10 entries alive for up to 30 minutes.
 * Ideal for caching import-related objects such as entity DTOs,
 * vertex lookups, or ImportID→Entity mappings during a single import cycle.</p>
 */
public class ImportCacheManager<K, V> {

    private static final int MAX_SIZE = 10;             // Max 10 entries
    private static final long TTL_MINUTES = 30;         // Expire after 30 minutes
    private static final long TTL_MILLIS = TimeUnit.MINUTES.toMillis(TTL_MINUTES);

    private final ConcurrentHashMap<K, CacheEntry<V>> cache = new ConcurrentHashMap<>();

    public ImportCacheManager() {
        startCleanupThread();
    }

    private static class CacheEntry<V> {
        final V value;
        final long timestamp;

        CacheEntry(V value) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired(long ttlMillis) {
            return System.currentTimeMillis() - timestamp > ttlMillis;
        }
    }

    /** Store or update a value in the cache */
    public void put(K key, V value) {
        if (key == null || value == null) {
            return;
        }

        // Evict oldest if max size exceeded
        if (cache.size() >= MAX_SIZE) {
            evictOldest();
        }

        cache.put(key, new CacheEntry<>(value));
    }

    /** Retrieve a value if still valid */
    public V get(K key) {
        CacheEntry<V> entry = cache.get(key);
        if (entry == null) {
            return null;
        }

        if (entry.isExpired(TTL_MILLIS)) {
            cache.remove(key);
            return null;
        }

        return entry.value;
    }

    /** Manually remove one entry */
    public void invalidate(K key) {
        cache.remove(key);
    }

    /** Clear entire cache */
    public void clear() {
        cache.clear();
    }

    /** Returns current cache size */
    public int size() {
        return cache.size();
    }

    /** Evicts the oldest entry based on timestamp */
    private void evictOldest() {
        K oldestKey = null;
        long oldestTime = Long.MAX_VALUE;

        for (Map.Entry<K, CacheEntry<V>> e : cache.entrySet()) {
            if (e.getValue().timestamp < oldestTime) {
                oldestKey = e.getKey();
                oldestTime = e.getValue().timestamp;
            }
        }

        if (oldestKey != null) {
            cache.remove(oldestKey);
        }
    }

    /** Periodic cleanup for expired entries */
    private void startCleanupThread() {
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ImportCache-Cleanup");
            t.setDaemon(true);
            return t;
        }).scheduleAtFixedRate(this::cleanup, 1, 1, TimeUnit.MINUTES);
    }

    private void cleanup() {
        long now = System.currentTimeMillis();
        cache.entrySet().removeIf(e -> (now - e.getValue().timestamp) > TTL_MILLIS);
    }
}
