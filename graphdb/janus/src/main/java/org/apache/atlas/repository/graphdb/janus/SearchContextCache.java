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
    private static Cache<Object, Object> searchContextCache = CacheBuilder.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    public static void put(String key, String value) {
        searchContextCache.put(key, value);
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
