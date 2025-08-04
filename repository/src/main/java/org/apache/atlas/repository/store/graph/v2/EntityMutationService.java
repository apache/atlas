package org.apache.atlas.repository.store.graph.v2;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.CassandraTagOperation;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeaders;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

@Service
public class EntityMutationService {

    private static final Logger LOG = LoggerFactory.getLogger(EntityMutationService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("store.EntityMutationService");

    private final AtlasEntityStoreV2 entityStore;
    private final EntityMutationPostProcessor entityMutationPostProcessor;
    private final AtlasTypeRegistry typeRegistry;
    private final AtlasEntityStore entitiesStore;
    private final EntityGraphMapper entityGraphMapper;
    private final IAtlasEntityChangeNotifier entityChangeNotifier;
    private final AtlasInstanceConverter instanceConverter;
    private final EntityGraphRetriever entityGraphRetriever;
    private final AtlasRelationshipStore relationshipStore;

    @Inject
    public EntityMutationService(AtlasEntityStoreV2 entityStore, EntityMutationPostProcessor entityMutationPostProcessor, AtlasTypeRegistry typeRegistry, AtlasEntityStore entitiesStore, EntityGraphMapper entityGraphMapper, IAtlasEntityChangeNotifier entityChangeNotifier, AtlasInstanceConverter instanceConverter, EntityGraphRetriever entityGraphRetriever, AtlasRelationshipStore relationshipStore) {
        this.entityStore = entityStore;
        this.entityMutationPostProcessor = entityMutationPostProcessor;
        this.typeRegistry = typeRegistry;
        this.entitiesStore = entitiesStore;
        this.entityGraphMapper = entityGraphMapper;
        this.entityChangeNotifier = entityChangeNotifier;
        this.instanceConverter = instanceConverter;
        this.entityGraphRetriever = entityGraphRetriever;
        this.relationshipStore = relationshipStore;
    }

    public EntityMutationResponse createOrUpdate(EntityStream entityStream,
                                                 BulkRequestContext context) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityMutationService.createOrUpdate");
        }

        boolean isGraphTransactionFailed = false;
        try {
            return entityStore.createOrUpdate(entityStream, context);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
            AtlasPerfTracer.log(perf);
        }
    }

    public void setClassifications(AtlasEntityHeaders entityHeaders, boolean overrideClassifications) throws AtlasBaseException {

        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "EntityMutationService.setClassifications");
        }

        boolean isGraphTransactionFailed = false;
        try {
            ClassificationAssociator.Updater associator = new ClassificationAssociator.Updater(typeRegistry, entitiesStore, entityGraphMapper, entityChangeNotifier, instanceConverter, entityGraphRetriever);
            associator.setClassifications(entityHeaders.getGuidHeaderMap(), overrideClassifications);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
            AtlasPerfTracer.log(perf);
        }
    }

    public EntityMutationResponse updateByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes,
                                                           AtlasEntity.AtlasEntityWithExtInfo updatedEntityInfo) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            return entityStore.updateByUniqueAttributes(entityType, uniqAttributes, updatedEntityInfo);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public EntityMutationResponse deleteByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            return entityStore.deleteByUniqueAttributes(entityType, uniqAttributes);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public EntityMutationResponse deleteById(final String guid) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            return entityStore.deleteById(guid);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public void addClassifications(final String guid, final List<AtlasClassification> classifications) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            entityStore.addClassifications(guid, classifications);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public void updateClassifications(String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            entityStore.updateClassifications(guid, classifications);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public void deleteClassification(final String guid, final String classificationName) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            entityStore.deleteClassification(guid, classificationName);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public void deleteClassification(final String guid, final String classificationName, final String associatedEntityGuid) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            entityStore.deleteClassification(guid, classificationName, associatedEntityGuid);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public EntityMutationResponse deleteByIds(final List<String> guids) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            return entityStore.deleteByIds(guids);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public EntityMutationResponse restoreByIds(final List<String> guids) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            return entityStore.restoreByIds(guids);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public void addClassification(final List<String> guids, final AtlasClassification classification) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            entityStore.addClassification(guids, classification);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public EntityMutationResponse deleteByUniqueAttributes(List<AtlasObjectId> objectIds) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            return entityStore.deleteByUniqueAttributes(objectIds);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public Map<String, String> repairClassificationMappings(List<String> guids) throws AtlasBaseException {
        Map<String, String> errorMap = new HashMap<>(0);

        boolean isGraphTransactionFailed = false;
        boolean isTagV2Enabled = FeatureFlagStore.isTagV2Enabled();

        int batchSize = isTagV2Enabled ? 300 : 20;
        List<List<String>> chunks = Lists.partition(guids, batchSize);

        LOG.info("Chunked {} guids into {} batches of {} guids each", guids.size(), chunks.size(), batchSize);

        int processedGuids = 0;

        for (int i = 0; i < chunks.size(); i++) {
            List<String> chunk = chunks.get(i);

            LOG.info("Processing batch {} of size {}", i+1, chunk.size());
            if (isTagV2Enabled) {
                try {
                    errorMap.putAll(entityStore.repairClassificationMappingsV2(chunk));
                } catch (Throwable e) {
                    isGraphTransactionFailed = true;
                    rollbackNativeCassandraOperations();
                    throw e;
                } finally {
                    executeESPostProcessing(isGraphTransactionFailed);
                    RequestContext.get().getESDeferredOperations().clear();
                }

            } else {
                entityStore.repairClassificationMappings(chunk);
            }
            processedGuids+=chunk.size();
            LOG.info("Processed batch {}, total guids processed {}", i+1, processedGuids);
        }

        return errorMap;
    }

    public void deleteRelationshipById(String guid) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            relationshipStore.deleteById(guid);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    public void deleteRelationshipsByIds(List<String> guids) throws AtlasBaseException {
        boolean isGraphTransactionFailed = false;
        try {
            relationshipStore.deleteByIds(guids);
        } catch (Throwable e) {
            isGraphTransactionFailed = true;
            rollbackNativeCassandraOperations();
            throw e;
        } finally {
            executeESPostProcessing(isGraphTransactionFailed);  // Only execute ES operations if no errors occurred
        }
    }

    private void executeESPostProcessing(boolean isGraphTransactionFailed) {
        if (!isGraphTransactionFailed && !RequestContext.get().getESDeferredOperations().isEmpty()) {
            // This will be skipped for v1 as RequestContext.get().getESDeferredOperations() will be empty
            try {
                entityMutationPostProcessor.executeESOperations(RequestContext.get().getESDeferredOperations());
            } catch (Exception e) {
                LOG.error("Failed to execute ES operations after graph transaction failure", e);
            }
        }
    }

    private void rollbackNativeCassandraOperations() throws AtlasBaseException {
        Map<String, Stack<CassandraTagOperation>> cassandraTagOps = RequestContext.get().getCassandraTagOperations();

        // This will be skipped for v1 as cassandraTagOps will be empty
        entityMutationPostProcessor.rollbackCassandraTagOperations(cassandraTagOps);

        // Can add more rollbacks for id-graph operations if needed
    }

}