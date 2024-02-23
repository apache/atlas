package org.apache.atlas.repository.graphdb.janus;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SearchContextCache {
    private static final Cache<Object, Object> searchContextCache = CacheBuilder.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    private static final Cache<Object, Object> searchContextSequenceCache = CacheBuilder.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    public static void put(String key, String value) {
        searchContextCache.put(key, value);
    }

    public static void putSequence(String key, Integer value) {
        searchContextSequenceCache.put(key, value);
    }

    public static Integer getSequence(String key) {
        return (Integer) searchContextSequenceCache.getIfPresent(key);
    }

    public static String get(String key){
        return (String) searchContextCache.getIfPresent(key);
    }

    public static void remove(String key) {
        searchContextCache.invalidate(key);
    }

    public static void clear() {
        searchContextCache.cleanUp();
    }
}
