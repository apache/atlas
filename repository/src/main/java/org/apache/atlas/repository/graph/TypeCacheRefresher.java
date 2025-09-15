package org.apache.atlas.repository.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
public class TypeCacheRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefresher.class);
    private final AtlasTypeDefStore typeDefStore;

    @Inject
    public TypeCacheRefresher(final AtlasTypeDefStore typeDefStore) {
        this.typeDefStore = typeDefStore;
    }

    public boolean isCacheRefreshNeeded(RedisService redisService) {
        long currentRedisVersion = Long.parseLong(redisService.getValue(Constants.TYPEDEF_CACHE_LATEST_VERSION, "1"));
        LOG.debug("Current Redis typedef version: {}, Latest typedef version: {}", currentRedisVersion, AtlasTypeDefStoreInitializer.getCurrentTypedefInternalVersion());
        return AtlasTypeDefStoreInitializer.getCurrentTypedefInternalVersion() != currentRedisVersion;
    }

    public void refreshCacheIfNeeded(RedisService redisService) throws AtlasBaseException {
        if (isCacheRefreshNeeded(redisService)) {
            long currentRedisVersion = Long.parseLong(redisService.getValue(Constants.TYPEDEF_CACHE_LATEST_VERSION, "1"));
            LOG.info("Refreshing type-def cache as the version is different from latest");
            typeDefStore.reloadCustomTypeDefs();
            AtlasTypeDefStoreInitializer.setCurrentTypedefInternalVersion(currentRedisVersion);
        }
    }

}