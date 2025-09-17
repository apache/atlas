package org.apache.atlas.repository.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.*;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.atlas.repository.Constants.*;

@Component
public class TypeCacheRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefresher.class);
    private final AtlasTypeDefStore typeDefStore;

    // Define type metadata to make code generic
    private static class TypeDefMetadata {
        final String redisKey;
        final String typeName;
        final Supplier<Long> getCurrentVersion;
        final Consumer<Long> setCurrentVersion;
        final Runnable reloadTypeDefs;

        TypeDefMetadata(String redisKey, String typeName, 
                       Supplier<Long> getCurrentVersion, 
                       Consumer<Long> setCurrentVersion,
                       Runnable reloadTypeDefs) {
            this.redisKey = redisKey;
            this.typeName = typeName;
            this.getCurrentVersion = getCurrentVersion;
            this.setCurrentVersion = setCurrentVersion;
            this.reloadTypeDefs = reloadTypeDefs;
        }
    }

    // Map of all type definitions and their metadata
    private final Map<Class<?>, TypeDefMetadata> typeDefMetadataMap;

    @Inject
    public TypeCacheRefresher(final AtlasTypeDefStore typeDefStore) {
        this.typeDefStore = typeDefStore;
        this.typeDefMetadataMap = new HashMap<>();

        // Initialize metadata for each type
        typeDefMetadataMap.put(AtlasBusinessMetadataDef.class, new TypeDefMetadata(
            TYPEDEF_BUSINESS_METADATA_CACHE_LATEST_VERSION,
            "BM",
            AtlasTypeDefStoreInitializer::getCurrentBMTypedefInternalVersion,
            AtlasTypeDefStoreInitializer::setCurrentBMTypedefInternalVersion,
            () -> {
                try {
                    typeDefStore.reloadBusinessMetadataTypeDefs();
                } catch (AtlasBaseException e) {
                    LOG.error("Error reloading BM typedefs", e);
                }
            }
        ));

        typeDefMetadataMap.put(AtlasClassificationDef.class, new TypeDefMetadata(
            TYPEDEF_CLASSIFICATION_METADATA_CACHE_LATEST_VERSION,
            "Classification",
            AtlasTypeDefStoreInitializer::getCurrentClassificationTypedefInternalVersion,
            AtlasTypeDefStoreInitializer::setCurrentClassificationTypedefInternalVersion,
            () -> {
                try {
                    typeDefStore.reloadClassificationMetadataTypeDefs();
                } catch (AtlasBaseException e) {
                    LOG.error("Error reloading Classification typedefs", e);
                }
            }
        ));

        typeDefMetadataMap.put(AtlasEnumDef.class, new TypeDefMetadata(
            TYPEDEF_ENUM_CACHE_LATEST_VERSION,
            "Enum",
            AtlasTypeDefStoreInitializer::getCurrentEnumTypedefInternalVersion,
            AtlasTypeDefStoreInitializer::setCurrentEnumTypedefInternalVersion,
            () -> {
                try {
                    typeDefStore.reloadEnumTypeDefs();
                } catch (AtlasBaseException e) {
                    LOG.error("Error reloading Enum typedefs", e);
                }
            }
        ));

        typeDefMetadataMap.put(AtlasStructDef.class, new TypeDefMetadata(
            TYPEDEF_STRUCT_CACHE_LATEST_VERSION,
            "Struct",
            AtlasTypeDefStoreInitializer::getCurrentStructTypedefInternalVersion,
            AtlasTypeDefStoreInitializer::setCurrentStructTypedefInternalVersion,
            () -> {
                try {
                    typeDefStore.reloadStructTypeDefs();
                } catch (AtlasBaseException e) {
                    LOG.error("Error reloading Struct typedefs", e);
                }
            }
        ));

        typeDefMetadataMap.put(AtlasEntityDef.class, new TypeDefMetadata(
            TYPEDEF_ENTITY_CACHE_LATEST_VERSION,
            "Entity",
            AtlasTypeDefStoreInitializer::getCurrentEntityTypedefInternalVersion,
            AtlasTypeDefStoreInitializer::setCurrentEntityTypedefInternalVersion,
            () -> {
                try {
                    typeDefStore.reloadEntityTypeDefs();
                } catch (AtlasBaseException e) {
                    LOG.error("Error reloading Entity typedefs", e);
                }
            }
        ));

        typeDefMetadataMap.put(AtlasRelationshipDef.class, new TypeDefMetadata(
            TYPEDEF_RELATIONSHIP_CACHE_LATEST_VERSION,
            "Relationship",
            AtlasTypeDefStoreInitializer::getCurrentRelationshipTypedefInternalVersion,
            AtlasTypeDefStoreInitializer::setCurrentRelationshipTypedefInternalVersion,
            () -> {
                try {
                    typeDefStore.reloadRelationshipTypeDefs();
                } catch (AtlasBaseException e) {
                    LOG.error("Error reloading Relationship typedefs", e);
                }
            }
        ));
    }

    public void refreshCacheIfNeeded(RedisService redisService) throws AtlasBaseException {
        for (TypeDefMetadata metadata : typeDefMetadataMap.values()) {
            if (isTypeDefCacheRefreshNeeded(redisService, metadata)) {
                LOG.info("Refreshing {} type-def cache as the version is different from latest", metadata.typeName);
                metadata.reloadTypeDefs.run();
                long currentRedisVersion = Long.parseLong(redisService.getValue(metadata.redisKey, "1"));
                metadata.setCurrentVersion.accept(currentRedisVersion);
            }
        }
    }

    private boolean isTypeDefCacheRefreshNeeded(RedisService redisService, TypeDefMetadata metadata) {
        long currentRedisVersion = Long.parseLong(redisService.getValue(metadata.redisKey, "1"));
        long currentInternalVersion = metadata.getCurrentVersion.get();
        LOG.info("Current Redis {} typedef version: {}, Latest {} typedef version: {}", 
                metadata.typeName, currentRedisVersion, metadata.typeName, currentInternalVersion);
        return currentInternalVersion < currentRedisVersion;
    }

    public void updateVersion(RedisService redisService, AtlasTypesDef atlasTypesDef) {
        if (atlasTypesDef == null) {
            return;
        }

        updateTypeDefVersions(redisService, atlasTypesDef.getBusinessMetadataDefs(), AtlasBusinessMetadataDef.class);
        updateTypeDefVersions(redisService, atlasTypesDef.getClassificationDefs(), AtlasClassificationDef.class);
        updateTypeDefVersions(redisService, atlasTypesDef.getEnumDefs(), AtlasEnumDef.class);
        updateTypeDefVersions(redisService, atlasTypesDef.getStructDefs(), AtlasStructDef.class);
        updateTypeDefVersions(redisService, atlasTypesDef.getEntityDefs(), AtlasEntityDef.class);
        updateTypeDefVersions(redisService, atlasTypesDef.getRelationshipDefs(), AtlasRelationshipDef.class);
    }

    private <T extends AtlasBaseTypeDef> void updateTypeDefVersions(RedisService redisService, 
                                                                   List<T> typeDefs, 
                                                                   Class<T> typeClass) {
        if (CollectionUtils.isNotEmpty(typeDefs)) {
            TypeDefMetadata metadata = typeDefMetadataMap.get(typeClass);
            if (metadata != null) {
                long latestVersion = Long.parseLong(redisService.getValue(metadata.redisKey, "1")) + 1;
                String latestVersionStr = String.valueOf(latestVersion);
                redisService.putValue(metadata.redisKey, latestVersionStr);
                metadata.setCurrentVersion.accept(latestVersion);
            }
        }
    }

    public void updateVersion(RedisService redisService, AtlasBaseTypeDef atlasBaseTypeDef) {
        if (atlasBaseTypeDef == null) {
            return;
        }

        TypeDefMetadata metadata = typeDefMetadataMap.get(atlasBaseTypeDef.getClass());
        if (metadata != null) {
            long latestVersion = Long.parseLong(redisService.getValue(metadata.redisKey, "1")) + 1;
            String latestVersionStr = String.valueOf(latestVersion);
            redisService.putValue(metadata.redisKey, latestVersionStr);
            metadata.setCurrentVersion.accept(latestVersion);
        }
    }

}
