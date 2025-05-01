package org.apache.atlas.repository.graphdb.janus;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class APIKeySessionCache {
    private static final Logger LOG = LoggerFactory.getLogger(APIKeySessionCache.class);
    
    private static final long TTL_MINUTES = 1;
    private final Cache<String, Boolean> apiKeyCache;
    
    private static APIKeySessionCache instance;
    
    private APIKeySessionCache() {
        apiKeyCache = Caffeine.newBuilder()
                .expireAfterWrite(TTL_MINUTES, TimeUnit.MINUTES)
                .build();
    }
    
    public static synchronized APIKeySessionCache getInstance() {
        if (instance == null) {
            instance = new APIKeySessionCache();
        }
        return instance;
    }
    
    public void setCache(String apiKeyUsername) {
        apiKeyCache.put(apiKeyUsername, Boolean.TRUE);
        LOG.debug("Cached API key for user: {}", apiKeyUsername);
    }
    
    public boolean isValid(String apiKeyUsername) {
        Boolean isPresent = apiKeyCache.getIfPresent(apiKeyUsername);
        return isPresent != null && isPresent;
    }
}
