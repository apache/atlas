package org.apache.atlas.repository.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.*;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import static org.apache.atlas.repository.Constants.*;

@Component
public class TypeCacheRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefresher.class);
    private final AtlasTypeDefStore typeDefStore;

    @Inject
    public TypeCacheRefresher(final AtlasTypeDefStore typeDefStore) {
        this.typeDefStore = typeDefStore;
    }

    public void refreshCacheIfNeeded(RedisService redisService) throws AtlasBaseException {
        if (isEnumTypeDefCacheRefreshNeeded(redisService)) {
            LOG.info("Refreshing enum type-def cache as the version is different from latest");
            typeDefStore.reloadEnumTypeDefs();
            long currentRedisVersion = Long.parseLong(redisService.getValue(Constants.TYPEDEF_ENUM_CACHE_LATEST_VERSION, "1"));
            AtlasTypeDefStoreInitializer.setCurrentEnumTypedefInternalVersion(currentRedisVersion);
        }

        if (isBMTypeDefCacheRefreshNeeded(redisService)) {
            LOG.info("Refreshing BM type-def cache as the version is different from latest");
            typeDefStore.reloadBusinessMetadataTypeDefs();
            long currentRedisVersion = Long.parseLong(redisService.getValue(TYPEDEF_BUSINESS_METADATA_CACHE_LATEST_VERSION, "1"));
            AtlasTypeDefStoreInitializer.setCurrentBMTypedefInternalVersion(currentRedisVersion);
        }

        if (isClassificationTypeDefCacheRefreshNeeded(redisService)) {
            LOG.info("Refreshing Classification type-def cache as the version is different from latest");
            typeDefStore.reloadClassificationMetadataTypeDefs();
            long currentRedisVersion = Long.parseLong(redisService.getValue(Constants.TYPEDEF_CLASSIFICATION_METADATA_CACHE_LATEST_VERSION, "1"));
            AtlasTypeDefStoreInitializer.setCurrentClassificationTypedefInternalVersion(currentRedisVersion);
        }
    }

    public void updateVersion(RedisService redisService, AtlasTypesDef atlasTypesDef) {
        if (atlasTypesDef == null) {
            return;
        }
        if (CollectionUtils.isNotEmpty(atlasTypesDef.getBusinessMetadataDefs())) {
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_BUSINESS_METADATA_CACHE_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_BUSINESS_METADATA_CACHE_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentBMTypedefInternalVersion(latestVersion);
        }
        if (CollectionUtils.isNotEmpty(atlasTypesDef.getClassificationDefs())) {
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_CLASSIFICATION_METADATA_CACHE_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_CLASSIFICATION_METADATA_CACHE_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentClassificationTypedefInternalVersion(latestVersion);
        }
        if (CollectionUtils.isNotEmpty(atlasTypesDef.getEnumDefs())) {
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_ENUM_CACHE_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_ENUM_CACHE_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentEnumTypedefInternalVersion(latestVersion);
        }
    }

    public void updateVersion(RedisService redisService, AtlasBaseTypeDef atlasBaseTypeDef) {
        if (atlasBaseTypeDef == null) {
            return;
        }
        if (atlasBaseTypeDef instanceof AtlasBusinessMetadataDef) {
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_BUSINESS_METADATA_CACHE_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_BUSINESS_METADATA_CACHE_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentBMTypedefInternalVersion(latestVersion);
        }
        if (atlasBaseTypeDef instanceof AtlasClassificationDef) {
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_CLASSIFICATION_METADATA_CACHE_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_CLASSIFICATION_METADATA_CACHE_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentClassificationTypedefInternalVersion(latestVersion);
        }
        if (atlasBaseTypeDef instanceof AtlasEnumDef) {
            long latestVersion = Long.parseLong(redisService.getValue(TYPEDEF_ENUM_CACHE_LATEST_VERSION, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(TYPEDEF_ENUM_CACHE_LATEST_VERSION, latestVersionStr);
            AtlasTypeDefStoreInitializer.setCurrentEnumTypedefInternalVersion(latestVersion);
        }
    }

    private boolean isEnumTypeDefCacheRefreshNeeded(RedisService redisService) {
        long currentRedisVersion = Long.parseLong(redisService.getValue(Constants.TYPEDEF_ENUM_CACHE_LATEST_VERSION, "1"));
        LOG.info("Current Redis Enum typedef version: {}, Latest Enum typedef version: {}", currentRedisVersion, AtlasTypeDefStoreInitializer.getCurrentEnumTypedefInternalVersion());
        return AtlasTypeDefStoreInitializer.getCurrentEnumTypedefInternalVersion() < currentRedisVersion;
    }

    private boolean isBMTypeDefCacheRefreshNeeded(RedisService redisService) {
        long currentRedisVersion = Long.parseLong(redisService.getValue(TYPEDEF_BUSINESS_METADATA_CACHE_LATEST_VERSION, "1"));
        LOG.info("Current Redis BM typedef version: {}, Latest BM typedef version: {}", currentRedisVersion, AtlasTypeDefStoreInitializer.getCurrentBMTypedefInternalVersion());
        return AtlasTypeDefStoreInitializer.getCurrentBMTypedefInternalVersion() < currentRedisVersion;
    }

    private boolean isClassificationTypeDefCacheRefreshNeeded(RedisService redisService) {
        long currentRedisVersion = Long.parseLong(redisService.getValue(Constants.TYPEDEF_CLASSIFICATION_METADATA_CACHE_LATEST_VERSION, "1"));
        LOG.info("Current Redis Classification typedef version: {}, Latest Classification typedef version: {}", currentRedisVersion, AtlasTypeDefStoreInitializer.getCurrentClassificationTypedefInternalVersion());
        return AtlasTypeDefStoreInitializer.getCurrentClassificationTypedefInternalVersion() < currentRedisVersion;
    }
}