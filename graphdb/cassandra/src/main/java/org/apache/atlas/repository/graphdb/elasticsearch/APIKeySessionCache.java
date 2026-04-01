package org.apache.atlas.repository.graphdb.elasticsearch;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.atlas.AtlasConfiguration;

import java.util.concurrent.TimeUnit;

public class APIKeySessionCache {

    private static final long TTL_SECONDS = AtlasConfiguration.KEYCLOAK_TOKEN_INTROSPECT_CACHE_TTL_SECOND.getLong();
    private final Cache<String, Boolean> apiKeyCache;

    private static APIKeySessionCache instance;

    private APIKeySessionCache() {
        apiKeyCache = Caffeine.newBuilder()
                .expireAfterWrite(TTL_SECONDS, TimeUnit.SECONDS)
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
    }

    public boolean isValid(String apiKeyUsername) {
        Boolean isPresent = apiKeyCache.getIfPresent(apiKeyUsername);
        return isPresent != null && isPresent;
    }

}
