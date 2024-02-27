package org.apache.atlas.repository.graphdb.janus;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SearchContextCache {
    private static final Cache<String, HashMap<Integer, String>> searchContextCache = CacheBuilder.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    public static void put(String key, Integer sequence, String esAsyncId) {
        HashMap<Integer, String> entry = new HashMap<>();
        entry.put(sequence, esAsyncId);
        searchContextCache.put(key, entry);
    }
    public static HashMap<Integer, String> get(String key){
        return searchContextCache.getIfPresent(key);
    }

    public static String getESAsyncSearchIdFromContextCache(String key, Integer sequence){
        //Get the context cache for the given key
        HashMap<Integer, String> contextCache = get(key);
        if(contextCache == null || sequence == null){
            return null;
        }
        //Find the highest sequence number
        int maxStoredSequence = 0;
        for (Integer seq : contextCache.keySet()) {
            if (seq > maxStoredSequence) {
                maxStoredSequence = seq;
            }
        }
        //If the given sequence is greater than the max stored sequence, return the ESAsyncId else return null
        return sequence > maxStoredSequence ? contextCache.getOrDefault(maxStoredSequence, null) : null;
    }

    public static void remove(String key) {
        searchContextCache.invalidate(key);
    }

    public static void clear() {
        searchContextCache.cleanUp();
    }
}
