package org.apache.atlas.repository.graphdb.janus;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.atlas.AtlasConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class APIKeySessionCache {
    private static final Logger LOG = LoggerFactory.getLogger(APIKeySessionCache.class);
    
    private static final long TTL_SECONDS = AtlasConfiguration.KEYCLOAK_TOKEN_INTROSPECT_CACHE_TTL_SECOND.getLong();
    private final Cache<String, Boolean> apiKeyCache;
    private final Set<String> deniedApiKeys;
    
    private static APIKeySessionCache instance;
    
    private APIKeySessionCache() {
        apiKeyCache = Caffeine.newBuilder()
                .expireAfterWrite(TTL_SECONDS, TimeUnit.SECONDS)
                .build();
        deniedApiKeys = ConcurrentHashMap.newKeySet();
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
    
    public void addToDeniedCache(String apiKeyUsername) {
        deniedApiKeys.add(apiKeyUsername);
        LOG.debug("Added API key to denied cache: {}", apiKeyUsername);
    }
    
    public boolean isValid(String apiKeyUsername) {
        if (deniedApiKeys.contains(apiKeyUsername)) {
            LOG.debug("API key found in denied cache: {}", apiKeyUsername);
            return false;
        }
        
        Boolean isPresent = apiKeyCache.getIfPresent(apiKeyUsername);
        return isPresent != null && isPresent;
    }
}
